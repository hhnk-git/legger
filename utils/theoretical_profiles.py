import logging
import os
import sqlite3
import typing
from decimal import Decimal, getcontext
from math import ceil, sqrt

try:
    from legger.sql_models.legger import Varianten, get_or_create
    from legger.sql_models.legger_database import load_spatialite
except ImportError:
    from sql_models.legger import Varianten, get_or_create
    from sql_models.legger_database import load_spatialite

log = logging.getLogger('legger.' + __name__)

getcontext().prec = 8

"""
Boundary Conditions
"""
Km = 25  # Manning coefficient in m**(1/3/s)
Kb = 23  # Bos and Bijkerk coefficient in 1/s

DEFAULT_MINIMAL_WATER_DEPTH = Decimal(0.3)
DEFAULT_MINIMAL_BOTTOM_WIDTH = Decimal(0.2)  # (m) Ditch bottom width can not be smaller dan 0,2m.


def sorter_none_last(a):
    if a is None:
        return float("inf")
    return a


"""
Slope functions
"""


def get_gradient_norm(grondsoort):
    if grondsoort.lower() == 'veen':
        return HydroObject.gradient_norm_table['veen']
    return HydroObject.gradient_norm_table['default']


def calc_pitlo_griffioen(flow, ditch_bottom_width, water_depth, slope, friction_manning, friction_begroeiing,
                         begroeiingsdeel):
    """
    A calculation of the formula for gradient in the water level according to Pitlo and Griffioen.
    Based on physical parameters like normative flow, ditch width, water depth and plant growth within the profile.
    """
    water_depth = float(water_depth)
    ditch_bottom_width = float(ditch_bottom_width)
    slope = float(slope)

    width_at_waterlevel = ditch_bottom_width + 2 * water_depth * slope

    ditch_circumference = width_at_waterlevel + 2 * (1 - begroeiingsdeel) * water_depth

    total_cross_section_area = 0.5 * (
            width_at_waterlevel - ditch_bottom_width) * water_depth + ditch_bottom_width * water_depth

    A_1 = (1 - begroeiingsdeel) * total_cross_section_area
    A_2 = begroeiingsdeel * total_cross_section_area
    R = A_1 / ditch_circumference
    B = A_2 * friction_begroeiing
    C = friction_manning * A_1 * (R ** 0.66666666666667)

    if B == 0:
        gradient = 99999999
    else:

        try:
            gradient = 100000 * (2 * B * flow + C ** 2 - C * sqrt(4 * B * flow + C ** 2)) / (2 * B ** 2)
        except TypeError:
            gradient = 99999

    return gradient


def calc_hydraulic_radius(ditch_bottom_width, water_depth, slope):
    ditch_circumference = (ditch_bottom_width
                           + (sqrt(water_depth ** 2 + (slope * water_depth) ** 2))
                           + (sqrt(water_depth ** 2 + (slope * water_depth) ** 2)))

    ditch_cross_section_area = (ditch_bottom_width * water_depth
                                + (0.5 * (water_depth * slope) * water_depth)
                                + (0.5 * (water_depth * slope) * water_depth))

    # Formule: Hydraulische Straal = Nat Oppervlak/ Natte Omtrek
    hydraulic_radius = ditch_cross_section_area / ditch_circumference
    return ditch_circumference, ditch_cross_section_area, hydraulic_radius


def calc_bos_bijkerk(normative_flow, ditch_bottom_width, water_depth, slope, friction_bos_bijkerk=Kb):
    """
    function not in use...
    A calculation of the formula for gradient in the water level according to De Bos and Bijkerk.
    Based on physical parameters like normative flow, ditch width, water depth and slope.
    """
    ditch_circumference, ditch_cross_section_area, hydraulic_radius = \
        calc_hydraulic_radius(ditch_bottom_width, water_depth, slope)

    # Formule: Gradient = Q / (((A*Kb*(waterdiepte^1/3))*(hydraulische straal^1/2)^2)*100000)
    gradient_bos_bijkerk = ((normative_flow / (
            ditch_cross_section_area * friction_bos_bijkerk * (water_depth ** 0.333333) *
            (hydraulic_radius ** 0.5))) ** 2) * 100000

    return gradient_bos_bijkerk


def calc_manning(normative_flow, ditch_bottom_width, water_depth, slope, friction_manning=Km):
    """ function not in use..."""
    ditch_circumference, ditch_cross_section_area, hydraulic_radius = \
        calc_hydraulic_radius(ditch_bottom_width, water_depth, slope)

    # Formule: Verhang = ((Q / (A*Km*(hydraulische straal^(2/3)))^2)*100000
    gradient_manning = ((normative_flow /
                         (ditch_cross_section_area * friction_manning * (hydraulic_radius ** 0.666667))) ** 2) * 100000

    return gradient_manning


def get_depth_list(from_depth, to_depth):
    out: typing.list[float] = []
    depth = from_depth

    while depth <= to_depth:
        out.append(Decimal(depth))

        if depth <= 1.0:
            depth += 0.05
        elif depth <= 2.0:
            depth += 0.10
        else:
            depth += 0.20

    return out


class HydroObject(object):
    gradient_norm_table = {
        'veen': 2.0,
        'default': 3.0
    }

    """
     Compute the slope and width according to:
     Veenhydraulic_ditch_width
         - Waterbreedte tot 4 m, talud 1:2
         - Waterbreedte 4-10 m, talud 1:3
         - Waterbreedte >10 m, talud 1:4
     Klei/ Zand
         - Waterbreedte tot 6 m, talud 1:1,5
         - Waterbreedte >6 m talud 1:2
     """

    slopes = {
        'veen': [
            {'max_width_m': Decimal(4.0), 'slope_1_on': Decimal(2.0)},
            {'max_width_m': Decimal(10.0), 'slope_1_on': Decimal(3.0)},
            {'max_width_m': None, 'slope_1_on': Decimal(4.0)},
        ],
        'default': [
            {'max_width_m': Decimal(6.0), 'slope_1_on': Decimal(1.5)},
            {'max_width_m': None, 'slope_1_on': Decimal(2.0)},
        ]
    }
    over_depth = {
        'veen': [
            {'max_width_m': Decimal(2.0), 'over_depth': Decimal(0.25)},
            {'max_width_m': None, 'over_depth': Decimal(0.35)},
        ],
        'default': [
            {'max_width_m': Decimal(2.0), 'over_depth': Decimal(0.15)},
            {'max_width_m': None, 'over_depth': Decimal(0.20)},
        ]
    }

    def __init__(self,
                 object_id,
                 ditch_depth,
                 zpeil_diff,
                 ditch_width,
                 category,
                 slope_preference,
                 grondsoort,
                 length,
                 normative_flow,
                 debiet_inlaat):

        self.object_id = object_id
        self.ditch_depth = Decimal(ditch_depth) if ditch_depth is not None else None
        self.zpeil_diff = Decimal(zpeil_diff) if zpeil_diff is not None else Decimal(0)
        self.ditch_width = Decimal(ditch_width) if ditch_width is not None else Decimal(999)
        self.category = category
        self.slope_preference = slope_preference
        self.grondsoort = grondsoort.lower() if grondsoort is not None else ''
        self.length = length
        self.normative_flow = normative_flow
        self.debiet_inlaat = debiet_inlaat
        self.check_water_widths = sorted(
            set(
                [s['max_width_m'] for s in self.slope_table] + [s['max_width_m'] for s in self.over_depth_table]),
            key=sorter_none_last)

        if self.grondsoort in self.gradient_norm_table:
            self.gradient_norm = self.gradient_norm_table[self.grondsoort]
        else:
            self.gradient_norm = self.gradient_norm_table['default']

        self.gradient_norm_inlaat = self.gradient_norm

    @property
    def slope_table(self):
        return self.slopes[self.grondsoort] if self.grondsoort in self.slopes else self.slopes['default']

    def get_slope(self, water_width):
        table = self.slope_table
        if water_width is None:
            water_width = Decimal(10000)
        for row in table:
            if row.get('max_width_m') is None or row.get('max_width_m') >= water_width:
                return row.get('slope_1_on')

    @property
    def over_depth_table(self):
        return self.over_depth[self.grondsoort] if self.grondsoort in self.over_depth else self.over_depth['default']

    def get_over_depth(self, water_width):
        table = self.over_depth_table
        for row in table:
            if row.get('max_width_m') is None or row.get('max_width_m') >= water_width:
                return row.get('over_depth')

    @staticmethod
    def bottom_width_from_water_width(water_width, slope, depth):
        return Decimal(water_width - Decimal(2) * slope * depth)

    @staticmethod
    def water_width_from_bottom_width(bottom_width, slope, depth):
        return Decimal(bottom_width + Decimal(2) * slope * depth)

    def get_profile_size_from_bottom_width_and_hydraulic_depth(self, hydraulic_depth, bottom_width):
        slope = None
        over_depth = None
        water_width = None

        for width in self.check_water_widths:
            slope = self.get_slope(width or Decimal(999))
            over_depth = self.get_over_depth(width or Decimal(999))
            water_width = self.water_width_from_bottom_width(bottom_width, slope, (hydraulic_depth + over_depth))
            if width is None or water_width <= width:
                # selected 'regime' is valid for this bottom_width, so select this
                break

        return {
            'water_width': Decimal(water_width),
            'slope': slope,
            'over_depth': Decimal(over_depth),
            'bottom_width': Decimal(bottom_width),
            'hydraulic_bottom_width': self.bottom_width_from_water_width(water_width, slope, hydraulic_depth),
        }

    def get_minimum_profile(self, hydraulic_depth):
        water_width = None
        slope = None
        over_depth = None

        # first try from max_width
        bottom_width = DEFAULT_MINIMAL_BOTTOM_WIDTH

        # turn around and calculate from bottom up
        # first get points in depth on which the parameters change:
        for width in self.check_water_widths:
            slope = self.get_slope(width or Decimal(999))
            over_depth = self.get_over_depth(width or Decimal(999))
            water_width = self.water_width_from_bottom_width(bottom_width, slope, (hydraulic_depth + over_depth))
            if width is None or water_width <= width or width is None:
                # selected 'regime' is valid for this bottom_width, so select this
                break

        return {
            'water_width': Decimal(water_width),
            'slope': Decimal(slope),
            'over_depth': Decimal(over_depth),
            'bottom_width': Decimal(bottom_width),
            'hydraulic_bottom_width': self.bottom_width_from_water_width(water_width, slope, hydraulic_depth),
        }

    def get_minimum_and_maximum(self, hydraulic_depth):
        # first try from max_width (ditch_width)
        minimum_profile = self.get_minimum_profile(hydraulic_depth)

        if minimum_profile['water_width'] >= self.ditch_width:
            # minimum profile is wider than max_width,
            # so minimum profile is equal to maximum profile
            maximum_profile = minimum_profile
            min_and_max_equal = True
        else:
            water_width = self.ditch_width
            slope = self.get_slope(water_width)
            over_depth = self.get_over_depth(water_width)
            bottom_width = self.bottom_width_from_water_width(water_width, slope, (hydraulic_depth + over_depth))
            if bottom_width < DEFAULT_MINIMAL_BOTTOM_WIDTH:
                # ditch bottom width is smaller than minimum bottom width
                # ditch bottom width is set to minimum bottom width
                # assuming slope stays the same
                bottom_width = DEFAULT_MINIMAL_BOTTOM_WIDTH
                water_width = self.water_width_from_bottom_width(bottom_width, slope, (hydraulic_depth + over_depth))
            maximum_profile = {
                'water_width': Decimal(water_width),
                'slope': Decimal(slope),
                'bottom_width': Decimal(bottom_width),
                'hydraulic_bottom_width': self.bottom_width_from_water_width(water_width, slope, hydraulic_depth),
                'over_depth': Decimal(over_depth),
            }
            min_and_max_equal = False

        return minimum_profile, maximum_profile, min_and_max_equal,


"""
General Definitions
"""


def create_variants(legger_db_filepath):
    """
        Main function for reading, calculating and writing the variants
    """
    conn = load_spatialite(legger_db_filepath)

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Step 1: Read the database
    cursor.execute("""
        Select ho.id, km.diepte, (ho.zomerpeil - ho.streefpeil) as zpeil_diff, km.breedte, 
        ho.categorieoppwaterlichaam, km.taludvoorkeur, km.grondsoort, 
        ST_LENGTH(ST_TRANSFORM(ho.geometry, 28992)) as length, ho.debiet, ho.debiet_inlaat 
        from hydroobject ho 
        left outer join kenmerken km on ho.id = km.hydro_id 
        where km.soort_vak is NULL
        """)

    hydro_objects = []

    for row in cursor.fetchall():
        hydro_objects.append(HydroObject(*row))

    log.debug("Finished step 1: SpatiaLite Database read successfully %i objects\n", len(hydro_objects))

    # Part 2a: get begroeiingsvariants
    cursor.execute(
        "Select  id, naam, friction_manning, friction_begroeiing, begroeiingsdeel "
        "from begroeiingsvariant ORDER BY begroeiingsdeel")
    begroeiingsvariants = cursor.fetchall()

    # Part 2b: get settings for depth rages from database

    cursor.execute("Select * from categorie")
    all_categories = cursor.fetchall()

    min_depth_settings = {cat['categorie']: cat['variant_diepte_min'] for cat in all_categories
                          if cat['variant_diepte_min'] is not None}

    max_depth_settings = {cat['categorie']: cat['variant_diepte_max'] for cat in all_categories
                          if cat['variant_diepte_max'] is not None}

    # additional: set max on 1.2 times th maximal depth within the specific category
    cursor.execute(
        "SELECT categorieoppwaterlichaam, max(diepte) as max_diepte FROM hydroobjects_kenmerken "
        "WHERE diepte > 0 AND diepte < 10 GROUP BY categorieoppwaterlichaam ORDER BY categorieoppwaterlichaam ")
    categories_max_depth = cursor.fetchall()

    last_category = 15
    for cat in categories_max_depth:
        try:
            cat_max_depth = Decimal(cat['max_diepte']) * Decimal(1.2)
        except TypeError:
            log.warning('category max depth caclulation fault. Max depth is {} of type {}'.format(
                cat['max_diepte'], type(cat['max_diepte'])))
            cat_max_depth = Decimal(999)

        if cat['categorieoppwaterlichaam'] in max_depth_settings:

            max_depth_settings[cat['categorieoppwaterlichaam']] = min(
                cat_max_depth, last_category, max_depth_settings[cat['categorieoppwaterlichaam']])
        else:
            max_depth_settings[cat['categorieoppwaterlichaam']] = min(cat['max_diepte'], cat_max_depth)

    # Part 3: calculate variants
    variants_table = []

    for hydro_object in hydro_objects:

        # get range of depths to calculate
        from_depth = min_depth_settings.get(hydro_object.category, DEFAULT_MINIMAL_WATER_DEPTH)
        to_depth = max_depth_settings.get(hydro_object.category, 8)
        to_depth = hydro_object.ditch_depth * Decimal(1.2) \
            if hydro_object.ditch_depth and hydro_object.ditch_depth * Decimal(1.2) > to_depth \
            else to_depth

        # calculate variants
        try:
            if hydro_object.normative_flow is None or hydro_object.normative_flow == 0:
                raise ValueError("hydro object value 'normative_flow' must be a value (not None or 0).")

            # get depth list for all variants to calculate
            # reverse the list so the width of the deeper variant can be used as
            # starting point for the next variant (results in less iterations)
            hydraulic_depth_list = get_depth_list(from_depth, to_depth)
            # hydraulic_depth_list.reverse()

            # step 1: calc on max waterwidth
            # if bottom width not ok --> get profile and calc
            # if gradient not ok -> this is the profile
            # else start from hydraulic bottom width and find best fit
            # if water width > m

            # loop over depths
            for hydraulic_depth in hydraulic_depth_list:

                minimum_profile, maximum_profile, min_and_max_equal = hydro_object.get_minimum_and_maximum(
                    hydraulic_depth)

                bottom_width = minimum_profile.get('bottom_width')

                for bv in begroeiingsvariants:
                    # initial values for finding profile which fits
                    i = 0
                    # the next begroeiingsvariant will start with the bottom width of the previous to save iterations
                    # a variant with higher friction must be wider than the previous bottom width
                    bottom_width = bottom_width
                    last_slope = None

                    # calculate width for selected depth, limit iteration to prevent endless loop
                    while i < 1000:
                        i += 1

                        # Bereken slope en waterbreedte
                        profile_size = hydro_object.get_profile_size_from_bottom_width_and_hydraulic_depth(
                            hydraulic_depth, bottom_width)

                        if (maximum_profile.get('water_width') <= profile_size.get('water_width')):
                            bottom_width = maximum_profile.get('bottom_width')
                            profile_size = maximum_profile

                        if last_slope != profile_size.get('slope'):
                            # todo: slope has changed, so change profile_width setting
                            last_slope = profile_size.get('slope')

                        # Begin berekenen verhang
                        gradient_pitlo_griffioen = calc_pitlo_griffioen(
                            abs(hydro_object.normative_flow),
                            profile_size.get('hydraulic_bottom_width'),
                            hydraulic_depth,
                            profile_size.get('slope'),
                            bv['friction_manning'],
                            bv['friction_begroeiing'],
                            bv['begroeiingsdeel']
                        )

                        gradient_pitlo_griffioen_inlaat = None
                        if (gradient_pitlo_griffioen <= hydro_object.gradient_norm and
                                hydro_object.debiet_inlaat is not None and hydro_object.debiet_inlaat != 0.0):
                            # check inlaat
                            if hydraulic_depth + hydro_object.zpeil_diff >= Decimal(0.05):
                                gradient_pitlo_griffioen_inlaat = calc_pitlo_griffioen(
                                    abs(hydro_object.debiet_inlaat),
                                    profile_size.get('hydraulic_bottom_width'),
                                    hydraulic_depth + hydro_object.zpeil_diff,
                                    profile_size.get('slope'),
                                    bv['friction_manning'],
                                    bv['friction_begroeiing'],
                                    bv['begroeiingsdeel']
                                )
                            else:
                                gradient_pitlo_griffioen_inlaat = 999

                        # stop when gradient is ok or maximum width is reached (except for the first variant with
                        # the lowest friction)
                        if ((gradient_pitlo_griffioen <= hydro_object.gradient_norm and
                             (
                                     gradient_pitlo_griffioen_inlaat is None or gradient_pitlo_griffioen_inlaat <= hydro_object.gradient_norm_inlaat))
                                or (profile_size['water_width'] >= maximum_profile.get('water_width'))):
                            # calc some final values

                            if hydro_object.debiet_inlaat is None:
                                gradient_pitlo_griffioen_inlaat = None
                            elif hydro_object.debiet_inlaat == 0.0:
                                gradient_pitlo_griffioen_inlaat = 0.0
                            else:
                                # check inlaat
                                if hydraulic_depth + hydro_object.zpeil_diff >= Decimal(0.05):
                                    gradient_pitlo_griffioen_inlaat = calc_pitlo_griffioen(
                                        abs(hydro_object.debiet_inlaat),
                                        profile_size.get('hydraulic_bottom_width'),
                                        hydraulic_depth + hydro_object.zpeil_diff,
                                        profile_size.get('slope'),
                                        bv['friction_manning'],
                                        bv['friction_begroeiing'],
                                        bv['begroeiingsdeel']
                                    )
                                else:
                                    gradient_pitlo_griffioen_inlaat = 999
                            if gradient_pitlo_griffioen_inlaat and gradient_pitlo_griffioen_inlaat > gradient_pitlo_griffioen:
                                afvoer_leidend = 0
                            else:
                                afvoer_leidend = 1

                            object_waterdepth_id = "{0}_{1:.2f}-{2:.0f}".format(
                                hydro_object.object_id,
                                hydraulic_depth,
                                bv['id'])

                            try:
                                if gradient_pitlo_griffioen > hydro_object.gradient_norm:
                                    opmerkingen = "voldoet niet aan de norm."
                                elif gradient_pitlo_griffioen_inlaat is not None and gradient_pitlo_griffioen_inlaat > hydro_object.gradient_norm:
                                    opmerkingen = "inlaat voldoet niet aan de norm."
                                else:
                                    opmerkingen = ""
                            except Exception as e:
                                log.error('error bij check gradient %s - %s hydraulic_depth: %s',
                                          hydro_object.object_id, hydraulic_depth, e)
                                opmerkingen = str(e)

                            variants_table.append([
                                object_waterdepth_id,
                                hydro_object.object_id,  # hydro_id
                                bv['id'],  # begroeiingsvariant_id,
                                profile_size.get('over_depth') + hydraulic_depth,  # diepte,
                                profile_size.get('water_width'),  # waterbreedte,
                                bottom_width,  # bodembreedte,
                                profile_size.get('slope'),  # talud,
                                hydraulic_depth,  # hydraulische_diepte,
                                profile_size.get('water_width'),  # hydraulische_waterbreedte,
                                profile_size.get('hydraulic_bottom_width'),  # hydraulische_bodembreedte,
                                profile_size.get('slope'),  # hydraulische_talud,
                                gradient_pitlo_griffioen,  # verhang,
                                gradient_pitlo_griffioen_inlaat,  # verhang_inlaat,
                                afvoer_leidend,  # afvoer_leidend,
                                opmerkingen,  # opmerkingen
                            ])
                            # break out while loop
                            break
                        else:
                            # increase bottom width
                            bottom_width += Decimal(0.1)
                            bottom_width = round(bottom_width, 1)

                    is_first_variant = False

        except ValueError as e:
            log.info('can not calculate variant for profile %s', hydro_object.object_id)
            log.critical(e)
            continue

    log.info("All potential profiles are created\n")

    def parse_string(value):
        if value is None:
            return "NULL"
        else:
            return "'{}'".format(value)

    def parse_float(value, decimals=2):
        if value is None:
            return "NULL"
        else:
            return "{0:.{1}f}".format(value, decimals)

    def parse_row(row_data):
        return "( {} )".format(", ".join([
            parse_string(row_data[0]),  # id,
            parse_string(row_data[1]),  # hydro_id
            parse_string(row_data[2]),  # begroeiingsvariant_id,
            parse_float(row_data[3]),  # diepte,
            parse_float(row_data[4]),  # waterbreedte,
            parse_float(row_data[5]),  # bodembreedte,
            parse_float(row_data[6]),  # talud,
            parse_float(row_data[7]),  # hydraulische_diepte,
            parse_float(row_data[8]),  # hydraulische_waterbreedte,
            parse_float(row_data[9]),  # hydraulische_bodembreedte,
            parse_float(row_data[10]),  # hydraulische_talud,
            parse_float(row_data[11]),  # verhang,
            parse_float(row_data[12]),  # verhang_inlaat,
            parse_float(row_data[13]),  # afvoer_leidend,
            parse_string(row_data[14]),  # opmerkingen
        ]))

    data = ",".join([parse_row(row) for row in variants_table])

    cursor.executescript("""
        BEGIN;
        CREATE TEMP TABLE tmp_varianten(
            id TEXT PRIMARY KEY, 
            hydro_id TEXT, 
            begroeiingsvariant_id TEXT, 
            diepte REAL, 
            waterbreedte REAL, 
            bodembreedte REAL, 
            talud REAL,
            hydraulische_diepte REAL,
            hydraulische_waterbreedte REAL,
            hydraulische_bodembreedte REAL, 
            hydraulische_talud REAL,
            verhang REAL,
            verhang_inlaat  REAL,
            afvoer_leidend INT,
            opmerkingen TEXT
        );
        
        INSERT INTO tmp_varianten(id, hydro_id, begroeiingsvariant_id, diepte, waterbreedte, bodembreedte, talud,
            hydraulische_diepte, hydraulische_waterbreedte, hydraulische_bodembreedte, hydraulische_talud,
            verhang, verhang_inlaat, afvoer_leidend, opmerkingen) VALUES {data};
            
        UPDATE varianten 
        SET
            diepte = tmp_varianten.diepte,
            waterbreedte = tmp_varianten.waterbreedte,
            bodembreedte = tmp_varianten.bodembreedte,
            talud = tmp_varianten.talud,
            hydraulische_diepte = tmp_varianten.hydraulische_diepte,
            hydraulische_waterbreedte = tmp_varianten.hydraulische_waterbreedte,
            hydraulische_bodembreedte = tmp_varianten.hydraulische_bodembreedte,
            hydraulische_talud = tmp_varianten.hydraulische_talud,
            verhang = tmp_varianten.verhang,
            verhang_inlaat = tmp_varianten.verhang_inlaat,
            afvoer_leidend = tmp_varianten.afvoer_leidend,
            opmerkingen = tmp_varianten.opmerkingen
        FROM tmp_varianten
        WHERE varianten.id = tmp_varianten.id;
        
        INSERT INTO varianten (id, hydro_id, begroeiingsvariant_id, diepte, waterbreedte, bodembreedte, talud,
            hydraulische_diepte, hydraulische_waterbreedte, hydraulische_bodembreedte, hydraulische_talud,
            verhang, verhang_inlaat, afvoer_leidend, opmerkingen)
        SELECT
            tmp_varianten.id,
            tmp_varianten.hydro_id,
            tmp_varianten.begroeiingsvariant_id,
            tmp_varianten.diepte,
            tmp_varianten.waterbreedte,
            tmp_varianten.bodembreedte,
            tmp_varianten.talud,
            tmp_varianten.hydraulische_diepte,
            tmp_varianten.hydraulische_waterbreedte,
            tmp_varianten.hydraulische_bodembreedte,
            tmp_varianten.hydraulische_talud,
            tmp_varianten.verhang,
            tmp_varianten.verhang_inlaat,
            tmp_varianten.afvoer_leidend,
            tmp_varianten.opmerkingen
        FROM tmp_varianten
        LEFT OUTER JOIN varianten ON varianten.id = tmp_varianten.id
        WHERE varianten.id IS NULL;
        
        DROP TABLE tmp_varianten;
        
        --COMMIT;        
    """.format(data=data))

    conn.commit()


if __name__ == '__main__':
    import sys

    os.environ["PROJ_LIB"] = "/Applications/QGIS-LTR.app/Contents/Resources/proj"
    os.environ["GDAL_DATA"] = "/Applications/QGIS-LTR.app/Contents/Resources/gdal"
    sys.path.append('/Users/bastiaanroos/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins')

    create_variants('//Users/bastiaanroos/Documents/testdata/leggertool/callantsoog4.sqlite')
