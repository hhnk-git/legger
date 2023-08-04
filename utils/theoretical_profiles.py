import logging

import numpy as np
import pandas as pd
from legger.sql_models.legger import Varianten, get_or_create
from pandas import DataFrame
import sqlite3
from legger.sql_models.legger_database import load_spatialite
from math import ceil

log = logging.getLogger('legger.' + __name__)

"""
Boundary Conditions
"""

Km = 25  # Manning coefficient in m**(1/3/s)
Kb = 23  # Bos and Bijkerk coefficient in 1/s

ini_waterdepth = 0.20  # Initial water depth (m).
default_minimal_waterdepth = 0.3
default_minimal_hydraulic_waterdepth = 0.1
min_ditch_bottom_width = 0.2  # (m) Ditch bottom width can not be smaller dan 0,2m.
default_minimal_bottom_width = min_ditch_bottom_width


def get_gradient_norm(grondsoort):
    """
    Get the gradient norm for the given soil type.
    maximal allowable gradient in waterway in cm/km
    """
    if 'veen' in grondsoort.lower():
        return 2.0
    else:
        return 3.0

def get_slope_width(grondsoort, hydraulic_ditch_bottom_width, hydraulic_water_depth):
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
    
    if 'veen' in grondsoort.lower():
        hydraulic_slope = 2
        hydraulic_ditch_width = hydraulic_ditch_bottom_width + hydraulic_water_depth * hydraulic_slope * 2.0
        if hydraulic_ditch_width >= 4.0:
            hydraulic_slope = 3
            hydraulic_ditch_width = hydraulic_ditch_bottom_width + hydraulic_water_depth * hydraulic_slope * 2.0
            if hydraulic_ditch_width >= 10.0:
                hydraulic_slope = 4
                hydraulic_ditch_width = hydraulic_ditch_bottom_width + hydraulic_water_depth * hydraulic_slope * 2.0
    else: # overige grondsoorten
        hydraulic_slope = 1.5
        hydraulic_ditch_width = hydraulic_ditch_bottom_width + hydraulic_water_depth * hydraulic_slope * 2.0
        if hydraulic_ditch_width >= 6.0:
            hydraulic_slope = 2
            hydraulic_ditch_width = hydraulic_ditch_bottom_width + hydraulic_water_depth * hydraulic_slope * 2.0

    hydraulic_ditch_width = round(hydraulic_ditch_width,2
                                  )
    return hydraulic_slope, hydraulic_ditch_width

"""
General Definitions
"""


def read_spatialite(cursor):
    """
    Read the database where all the information on hydro objects for the legger
    calculations are found.
    Return a database to be used in Python.

    Following information is collected:
    - object id
    - normative_flow
    - ditch depth
    - slope (talud), initial and maximum
    - maximum ditch width
    - length of hydro object
    - soil type of area ditch is located
    """

    cursor.execute(
        "Select ho.id, km.diepte, (ho.zomerpeil - ho.streefpeil) as zpeil_diff, km.breedte, ho.categorieoppwaterlichaam, km.taludvoorkeur, km.grondsoort, "
        "ST_LENGTH(ST_TRANSFORM(ho.geometry, 28992)) as length, ho.debiet, ho.debiet_inlaat "
        "from hydroobject ho "
        "left outer join kenmerken km on ho.id = km.hydro_id ")

    all_hits = cursor.fetchall()

    df = DataFrame(
        all_hits,
        columns=[
            'object_id',
            'DIEPTE',
            'zpeil_diff',
            'max_ditch_width',
            'category',
            'slope',
            'grondsoort',
            'length',
            'normative_flow',
            'debiet_inlaat']
    )

    df.slope = pd.to_numeric(df.slope)

    return df


def calc_pitlo_griffioen(flow, ditch_bottom_width, water_depth, slope, friction_manning, friction_begroeiing,
                         begroeiingsdeel):
    """
    A calculation of the formula for gradient in the water level according to Pitlo and Griffioen.
    Based on physical parameters like normative flow, ditch width, water depth and plant growth within the profile.
    """
    width_at_waterlevel = ditch_bottom_width + 2 * water_depth * slope

    ditch_circumference = width_at_waterlevel + 2 * (1 - begroeiingsdeel) * water_depth

    total_cross_section_area = 0.5 * (
                width_at_waterlevel - ditch_bottom_width) * water_depth + ditch_bottom_width * water_depth

    A_1 = (1 - begroeiingsdeel) * total_cross_section_area
    A_2 = begroeiingsdeel * total_cross_section_area
    R = A_1 / ditch_circumference
    B = A_2 * friction_begroeiing
    C = friction_manning * A_1 * (R ** 0.66666666666667)

    gradient = 100000 * (2 * B * flow + C ** 2 - C * np.sqrt(4 * B * flow + C ** 2)) / (2 * B ** 2)
    return gradient


def calc_bos_bijkerk(normative_flow, ditch_bottom_width, water_depth, slope, friction_bos_bijkerk=Kb):
    """
    A calculation of the formula for gradient in the water level according to De Bos and Bijkerk.
    Based on physical parameters like normative flow, ditch width, water depth and slope.
    """
    ditch_circumference = (ditch_bottom_width
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2))
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2)))

    ditch_cross_section_area = (ditch_bottom_width * water_depth
                                + (0.5 * (water_depth * slope) * water_depth)
                                + (0.5 * (water_depth * slope) * water_depth))

    # Formule: Hydraulische Straal = Nat Oppervlak/ Natte Omtrek
    hydraulic_radius = ditch_cross_section_area / ditch_circumference

    # Formule: Gradient = Q / (((A*Kb*(waterdiepte^1/3))*(hydraulische straal^1/2)^2)*100000)
    gradient_bos_bijkerk = ((normative_flow / (
            ditch_cross_section_area * friction_bos_bijkerk * (water_depth ** 0.333333) *
            (hydraulic_radius ** 0.5))) ** 2) * 100000

    return gradient_bos_bijkerk


def calc_manning(normative_flow, ditch_bottom_width, water_depth, slope, friction_manning=Km):
    ditch_circumference = (ditch_bottom_width
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2))
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2)))

    ditch_cross_section_area = (ditch_bottom_width * water_depth
                                + (0.5 * (water_depth * slope) * water_depth)
                                + (0.5 * (water_depth * slope) * water_depth))

    # Formule: Hydraulische Straal = Nat Oppervlak/ Natte Omtrek
    hydraulic_radius = ditch_cross_section_area / ditch_circumference

    # Formule: Verhang = ((Q / (A*Km*(hydraulische straal^(2/3)))^2)*100000
    gradient_manning = ((normative_flow /
                         (ditch_cross_section_area * friction_manning * (hydraulic_radius ** 0.666667))) ** 2) * 100000

    return gradient_manning


def calc_profile_variants_for_hydro_object(
        hydro_object,
        minimal_hydraulic_waterdepth=default_minimal_waterdepth,
        minimal_bottom_width=None,
        store_from_depth=None,
        store_to_depth=None,
        friction_manning=None,
        friction_begroeiing=None,
        begroeiingsdeel=None):
    """
    In this formula the different variants of suitable profiles are generated.
    The output is twofold:
    - a table where every variant is added.
    """
    if friction_manning is None or friction_begroeiing is None:
        raise ValueError('friction manning or begroeiing are both None')

    slope = hydro_object.slope # gebruikt als eerste inschatting voor bepalen profiel, daarna check op welk talud werkelijk
    max_ditch_width = hydro_object.max_ditch_width
    normative_flow = hydro_object.normative_flow
    zpeil_diff = hydro_object.zpeil_diff
    debiet_inlaat = hydro_object.debiet_inlaat
    object_id = hydro_object.object_id
    length = hydro_object.length
    grondsoort = hydro_object.grondsoort

    hydraulic_slope = slope

    gradient_norm = get_gradient_norm(grondsoort)
    gradient_norm_inlaat = gradient_norm

    # if max_ditch_width is None:
    #     raise ValueError("hydro object value 'max_ditch_width' must be a value (not None or 0).")
    if normative_flow is None or pd.isnull(normative_flow):
        raise ValueError("hydro object value 'normative_flow' must be a value (not None or 0).")
    if zpeil_diff is None or pd.isnull(zpeil_diff):
        zpeil_diff = 0

    # a table where variants are saved.
    variants_table = DataFrame(columns=['object_id', 'object_waterdepth_id', 'slope',
                                        'water_depth', 'ditch_width', 'ditch_bottom_width',
                                        'hydraulic_water_depth', 'hydraulic_ditch_width',
                                        'hydraulic_ditch_bottom_width', 'hydraulic_slope',
                                        'normative_flow', 'gradient', 'friction_manning', 'friction_begroeiing',
                                        'begroeiingsdeel', 'surge', 'afvoer_leidend', 'verhang_inlaat',
                                        'gradient_norm',])

    # minus 0.05, because in loop this is added
    hydraulic_water_depth = store_from_depth - 0.05

    go_on = True
    afvoer_leidend = 1

    # loop over depths
    while go_on:
        # water_depth for this while loop
        if hydraulic_water_depth <= 1:
            hydraulic_water_depth = hydraulic_water_depth + 0.05
        elif hydraulic_water_depth <= 2:
            hydraulic_water_depth = hydraulic_water_depth + 0.10
        else:
            hydraulic_water_depth = hydraulic_water_depth + 0.20

        water_depth = 0

        # initial values for finding profile which fits
        gradient_pitlo_griffioen = 1000
        gradient_pitlo_griffioen_inlaat = 0
        # minus 0.10, because in loop 0.10 is added
        hydraulic_ditch_bottom_width = minimal_bottom_width - 0.10
        ditch_bottom_width = None
        ditch_width = None
        hydraulic_ditch_width = None
        afvoer_leidend = 1

        # caculate width for selected depth
        # make sure this loop runs at least one time to calculate values
        while gradient_pitlo_griffioen > gradient_norm or gradient_pitlo_griffioen_inlaat > gradient_norm_inlaat:

            ### Eerst hydraulisch profiel opbouwen, waterdiepte is bekend uit bovenstaande while
            # Bodembreedte in stappen per 10 cm toe laten nemen
            hydraulic_ditch_bottom_width = hydraulic_ditch_bottom_width + 0.10

            # Bereken slope en waterbreedte
            hydraulic_slope, hydraulic_ditch_width = get_slope_width(grondsoort, hydraulic_ditch_bottom_width, hydraulic_water_depth)
            
            ### Leggerprofiel opbouwen
            # Waterdiepte leggerprofiel
            if 'veen' in grondsoort.lower():
                if hydraulic_ditch_width >= 2:
                    water_depth = hydraulic_water_depth + 0.15
                else:
                    water_depth = hydraulic_water_depth + 0.10
            else:
                if hydraulic_ditch_width >= 6:
                    water_depth = hydraulic_water_depth + 0.20
                else:
                    water_depth = hydraulic_water_depth + 0.15

            # Talud leggerprofiel gelijk aan hydraulische
            slope = hydraulic_slope            
            
            # Bodembreedte wordt smaller met grotere diepte en gelijk talud, maar niet smaller dan 20 cm
            ditch_bottom_width = hydraulic_ditch_bottom_width - (2 * slope * (water_depth - hydraulic_water_depth))
            
            # afronden op 5 cm
            ditch_bottom_width =  ceil(ditch_bottom_width / 0.05) * 0.05
            
            # als bodembreedte te klein is sla over, door naar volgende binnen while loop, volgende breedte-stap
            if ditch_bottom_width < 0.2:
                continue

            # Waterbreedte leggerprofiel
            ditch_width = round(ditch_bottom_width + water_depth * slope * 2.0,2)
            
            ### Begin berekenen verhang
            gradient_pitlo_griffioen = calc_pitlo_griffioen(
                abs(normative_flow), hydraulic_ditch_bottom_width, hydraulic_water_depth, hydraulic_slope,
                friction_manning, friction_begroeiing, begroeiingsdeel)

            gradient_pitlo_griffioen_inlaat = 0
            if gradient_pitlo_griffioen <= gradient_norm and debiet_inlaat is not None and debiet_inlaat != 0.0:
                # check inlaat
                gradient_pitlo_griffioen_inlaat = calc_pitlo_griffioen(
                    abs(debiet_inlaat), hydraulic_ditch_bottom_width, hydraulic_water_depth + zpeil_diff, 
                    hydraulic_slope,
                    friction_manning, friction_begroeiing, begroeiingsdeel)
            else:
                # skip calculation in loop, if normal gradient is already to high
                gradient_pitlo_griffioen_inlaat = 0

            # loop until gradient is lower than norm or profile gets wider than max_width
            # if first try is wider, this (to wide) profile is stored
            if ditch_width + 0.0 > max_ditch_width:
                break

        if debiet_inlaat is None:
            gradient_pitlo_griffioen_inlaat = None
        if debiet_inlaat == 0.0:
            gradient_pitlo_griffioen_inlaat = 0.0
        else:
            # check inlaat
            gradient_pitlo_griffioen_inlaat = calc_pitlo_griffioen(
                abs(debiet_inlaat), hydraulic_ditch_bottom_width, hydraulic_water_depth + zpeil_diff,
                hydraulic_slope,
                friction_manning, friction_begroeiing, begroeiingsdeel)
            if gradient_pitlo_griffioen_inlaat > gradient_pitlo_griffioen:
                afvoer_leidend = 0

        if hydraulic_water_depth < minimal_hydraulic_waterdepth:
            continue
        
        # store profielvarianten
        object_waterdepth_id = "{0}_{1:.2f}-{2:.1f}".format(
            object_id, water_depth, begroeiingsdeel)

        obj = [
            object_id,
            object_waterdepth_id,
            slope,
            water_depth,
            ditch_width,
            ditch_bottom_width,
            hydraulic_water_depth,
            hydraulic_ditch_width,
            hydraulic_ditch_bottom_width,
            hydraulic_slope,
            normative_flow,
            gradient_pitlo_griffioen,
            friction_manning,
            friction_begroeiing,
            begroeiingsdeel,
            length * gradient_pitlo_griffioen / 1000,
            afvoer_leidend,
            gradient_pitlo_griffioen_inlaat,
            gradient_norm
        ]

        variants_table = variants_table.append(
            pd.DataFrame([obj], columns=variants_table.columns))

        if water_depth >= store_to_depth and gradient_pitlo_griffioen < gradient_norm:
            go_on = False

    variants_table.reset_index()
    return variants_table


def create_theoretical_profiles(legger_db_filepath, bv):
    """
    main function for calculation of theoretical profiles

    legger_db_filepath (str): path to legger profile
    bv (Begroeiingsvariant model instance): Begroeiingsvariant (with friction value) for calculation

    return: calculated profile variant
    """

    conn = load_spatialite(legger_db_filepath)

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Part 1: read SpatiaLite
    # The original Spatialite database is read into Python for further analysis.
    hydro_objects = read_spatialite(cursor)
    log.debug("Finished 1: SpatiaLite Database read successfully %i objects\n", len(hydro_objects.object_id))

    # Part 2: set minimal slope and get depth ranges
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
            cat_max_depth = cat['max_diepte'] * 1.2
        except TypeError as e:
            log.warning('category max depth caclulation fault. Max depth is {} of type {}'.format(
                cat['max_diepte'], type(cat['max_diepte'])))
            cat_max_depth = 999

        if cat['categorieoppwaterlichaam'] in max_depth_settings:

            max_depth_settings[cat['categorieoppwaterlichaam']] = min(
                cat_max_depth, last_category, max_depth_settings[cat['categorieoppwaterlichaam']])
        else:
            max_depth_settings[cat['categorieoppwaterlichaam']] = min(cat['max_diepte'] * cat_max_depth)

    default_slope = {cat['categorie']: cat['default_talud'] for cat in all_categories
                     if cat['default_talud'] is not None}

    for cat, slope in default_slope.items():
        hydro_objects.loc[(pd.isnull(hydro_objects.slope) & hydro_objects.category == cat), 'slope'] = slope
    hydro_objects.loc[(pd.isnull(hydro_objects.slope)) & ('veen' in hydro_objects.grondsoort.lower()), 'slope'] = 3.0 # hydro_objects.grondsoort == 'Veen'
    hydro_objects.loc[(pd.isnull(hydro_objects.slope)), 'slope'] = 1.5

    hydro_objects.DIEPTE = pd.to_numeric(hydro_objects.DIEPTE, downcast='float', errors='coerce')
    hydro_objects.zpeil_diff = pd.to_numeric(hydro_objects.zpeil_diff, downcast='float', errors='coerce')

    # Part 3: calculate variants

    depth_mapping_field = 'category'

    variants_table = DataFrame(columns=['object_id', 'object_waterdepth_id', 'slope',
                                        'water_depth', 'ditch_width', 'ditch_bottom_width',
                                        'normative_flow', 'gradient', 'friction_manning', 'friction_begroeiing',
                                        'begroeiingsdeel', 'surge', 'afvoer_leidend', 'verhang_inlaat'])

    for row in hydro_objects.itertuples():

        if depth_mapping_field and type(min_depth_settings) == dict:
            from_depth = min_depth_settings.get(getattr(row, depth_mapping_field), default_minimal_waterdepth)
        else:
            from_depth = min_depth_settings
        if depth_mapping_field and type(max_depth_settings) == dict:
            to_depth = max_depth_settings.get(getattr(row, depth_mapping_field), 8)
        else:
            to_depth = max_depth_settings

        to_depth = max(to_depth, row.DIEPTE * 1.2 if pd.notna(row.DIEPTE) and pd.notnull(row.DIEPTE) else to_depth)

        try:
            variants_table = variants_table.append(
                calc_profile_variants_for_hydro_object(
                    hydro_object=row,
                    minimal_hydraulic_waterdepth=default_minimal_waterdepth,
                    minimal_bottom_width=default_minimal_bottom_width,
                    store_from_depth=from_depth,
                    store_to_depth=to_depth,
                    friction_manning=bv.friction_manning,
                    friction_begroeiing=bv.friction_begroeiing,
                    begroeiingsdeel=bv.begroeiingsdeel
                ),
                ignore_index=True
            )
        except ValueError as e:
            log.info('can not calculate variant for profile %s', row.object_id)
            log.critical(e)
            # raise e

    profile_variants = variants_table.reset_index(drop=True)

    log.info("All potential profiles are created\n")
    return profile_variants


def write_theoretical_profile_results_to_db(session, profile_results, bv):
    log.info("Writing output to db...\n")

    for row in profile_results.itertuples():
        # todo: add for manning
        gradient = row.gradient
        try:
            gradient_inlaat = float(row.verhang_inlaat)
        except Exception as e:
            gradient_inlaat = None

        try:
            gradient_norm = float(row.gradient_norm)
        except Exception as e:
            gradient_inlaat = None

        try:
            if gradient > gradient_norm:
                opmerkingen = "voldoet niet aan de norm."
            elif gradient_inlaat > gradient_norm:
                opmerkingen = "inlaat voldoet niet aan de norm."
            else:
                opmerkingen = ""
        except Exception as e:
            log.error('error bij check gradient %s - %s diepte: %s', row.object_id, row.water_depth, e)
            opmerkingen = str(e)
            gradient = None

        variant, new = get_or_create(
            session,
            Varianten,
            id=row.object_waterdepth_id,
            defaults={
                'hydro_id': row.object_id,
                'begroeiingsvariant': bv,
                'talud': row.slope,
                'diepte': row.water_depth,
            }
        )
        # todo: store more results
        variant.begroeiingsvariant = bv
        variant.diepte = row.water_depth
        variant.waterbreedte = row.ditch_width
        variant.bodembreedte = row.ditch_bottom_width
        variant.verhang = gradient
        variant.opmerkingen = opmerkingen
        variant.afvoer_leidend = row.afvoer_leidend
        variant.verhang_inlaat = gradient_inlaat

        variant.hydraulische_diepte = row.hydraulic_water_depth
        variant.hydraulische_waterbreedte = row.hydraulic_ditch_width
        variant.hydraulische_bodembreedte = row.hydraulic_ditch_bottom_width
        variant.hydraulische_talud = row.hydraulic_slope

    session.commit()
