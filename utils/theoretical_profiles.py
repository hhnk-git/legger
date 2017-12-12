from pyspatialite import dbapi2 as sql
import pandas as pd
import numpy as np
# import math
# import matplotlib.pyplot as plt
from pandas import DataFrame  # Series
from legger.sql_models.legger import ProfielVarianten
from legger.sql_models.legger_database import LeggerDatabase


"""
Definitions
"""


def read_spatialite(legger_db_filepath):
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

    conn = sql.connect(legger_db_filepath)
    c = conn.cursor()

    c.execute("Select ho.objectid, hk.diepte, hk.breedte, hk.initieeltalud, hk.steilstetalud, hk.grondsoort, "
              "ST_LENGTH(TRANSFORM(ho.geometry, 28992)) as length, tr.qend "
              "from hydroobject ho "
              "left outer join hydrokenmerken hk on ho.objectid = hk.objectid "
              "left outer join tdi_hydro_object_results tr on tr.hydroobject_id = hk.objectid")

    all_hits = c.fetchall()

    return DataFrame(all_hits, columns=[
        'OBJECTID',
        'DIEPTE',
        'BREEDTE',
        'INITIEELTALUD',
        'STEILSTETALUD',
        'GRONDSOORT',
        'LENGTH',
        'QEND'])


def filter_unused(df_in, column):
    """ The data set includes data that are incorrect or sometimes data is missing.
    In order for the analysis to work well these objects should be removed from the set.
    With this function the rows in the data frame where this occurs can be deleted from the data frame.
    The result is a new data frame that includes all the dropped rows from the original data frame.

    The input here is an input data frame (df_in), and a column name where the check should be on (i.e. slope, or width)
    Checks include: is the value 0? is the value NaN? D
    """
    df_unused = df_in[df_in[column] == 0]
    df_unused = df_unused.append(
        df_in[np.isnan(df_in[column])])

    df_unused = df_unused.drop_duplicates()  # In the case the same row occurs twice.

    if df_unused['OBJECTID'].count() == 0:
        print ("No hydro objects removed.")
    else:
        print (str(df_unused['OBJECTID'].count()) + " Hydro object(s) removed b/o missing data.")

    df_out = df_in.drop(df_unused.index)

    return df_out


def calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope):
    """
    A calculation of the formula for gradient in the water level.
    Based on physical parameters like normative flow, ditch width, water depth and slope.
    """
    ditch_circumference = (ditch_bottom_width
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2))
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2)))

    ditch_cross_section_area = (ditch_bottom_width * water_depth
                                + (0.5 * (water_depth * slope) * water_depth)
                                + (0.5 * (water_depth * slope) * water_depth))

    hydraulic_radius = ditch_cross_section_area / ditch_circumference
    # Or: Hydraulische Straal = Nat Oppervlak/ Natte Omtrek

    gradient_bos_bijkerke = ((normative_flow / (
        ditch_cross_section_area * Kb * (water_depth ** 0.333333) * (hydraulic_radius ** 0.5))) ** 2) * 100000
    # Gradient = Q / (((A*Kb*(waterdiepte^1/3))*(hydraulische straal^1/2)^2)*100000)

    return gradient_bos_bijkerke


def calc_manning(normative_flow, ditch_bottom_width, water_depth, slope):
    ditch_circumference = (ditch_bottom_width
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2))
                           + (np.sqrt(water_depth ** 2 + (slope * water_depth) ** 2)))

    ditch_cross_section_area = (ditch_bottom_width * water_depth
                                + (0.5 * (water_depth * slope) * water_depth)
                                + (0.5 * (water_depth * slope) * water_depth))

    hydraulic_radius = ditch_cross_section_area / ditch_circumference
    # Or: Hydraulische Straal = Nat Oppervlak/ Natte Omtrek

    gradient_manning = ((normative_flow /
                         (ditch_cross_section_area * Km * (hydraulic_radius ** 0.666667))) ** 2) * 100000
    # Verhang = ((Q / (A*Km*(hydraulische straal^(2/3)))^2)*100000

    return gradient_manning


def calc_profile_max_ditch_width(object_id, normative_flow, length, slope, max_ditch_width):
    """
    Calculate a ditch profile that suffices to the gradient norm, which is based on the maximum ditch width.
    Starting with some initial profile requirements (minimum water depth, minimum ditch bottom width), the calculation
    is done.
    A result is the necessary water depth that is necessary to keep a the gradient low enough.

    If it's possible to calculate a profile that complies to all the norms, the output can be saved
    in a new dataframe.
    """

    # initial values
    water_depth = ini_waterdepth
    ditch_bottom_width = max_ditch_width - slope * water_depth - slope * water_depth

    gradient_bos_bijkerke = calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope)
    gradient_manning = calc_manning(normative_flow, ditch_bottom_width, water_depth, slope)

    # iteration
    while gradient_bos_bijkerke > gradient_norm or gradient_manning > gradient_norm:

        water_depth = water_depth + 0.05

        # If the minimum ditch bottom width is NOT reached yet, then:
        if max_ditch_width - slope * water_depth - slope * water_depth >= min_ditch_bottom_width:

            ditch_bottom_width = max_ditch_width - slope * water_depth - slope * water_depth

            gradient_bos_bijkerke = calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope)
            gradient_manning = calc_manning(normative_flow, ditch_bottom_width, water_depth, slope)

        # If the minimum ditch bottom width is reached, then the iteration is done.
        else:
            # print ("Ditch bottom width became too small for " + str(object_id))

            # Water depth was increased for the calculation that now failed, so has to be restored to previous value.
            water_depth = water_depth - 0.05

            # Same goes for the gradient calculations:
            gradient_bos_bijkerke = calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope)
            gradient_manning = calc_manning(normative_flow, ditch_bottom_width, water_depth, slope)
            break

    profile = pd.DataFrame([[object_id,
                             normative_flow,
                             length,
                             slope,
                             max_ditch_width,
                             water_depth,
                             ditch_bottom_width,
                             gradient_bos_bijkerke,
                             gradient_manning]],
                           columns=['object_id', 'normative_flow', 'length', 'slope',
                                    'max_ditch_width', 'water_depth', 'ditch_bottom_width',
                                    'gradient_bos_bijkerke', 'gradient_manning'])
    return profile


def calc_surge(object_id, length, gradient):
    """
    Calculates the absolute water surge that is the result of the gradient in water level and ditch length.
    The gradient is the max of the gradient calculated with Manning and Bos and Bijkerke equations.
    """

    surge = (float(gradient) * (float(length) / 1000.0))  # gradient in cm/km and length in m.

    return object_id, surge


def calc_profile_variants(hydro_objects_satisfy):
    """
    In this formula the different variants of suitable profiles are generated.
    The output is twofold:
    - a table with the hydro object ID, followed by a number of possible outcomes
    - a table where every variant is added.
    """
    # First two empty tables:
    # 1st one with hydro objects that shows the amount of table variants pssoible.
    options_table = DataFrame(data=hydro_objects_satisfy.object_id, columns=['object_id', 'possibilities_count'])

    # 2nd one a table where variants are saved.
    variants_table = DataFrame(columns=['object_id', 'object_waterdepth_id', 'slope',
                                        'water_depth', 'ditch_width', 'ditch_bottom_width',
                                        'normative_flow', 'gradient_bos_bijkerke'])

    for i, rows in hydro_objects_satisfy.iterrows():
        count = 0
        object_id = hydro_objects_satisfy.object_id[i]
        slope = hydro_objects_satisfy.slope[i]
        water_depth = hydro_objects_satisfy.water_depth[i] + 0.05 * count
        normative_flow = hydro_objects_satisfy.normative_flow[i]

        # Only if ditch bottom width is bigger than the minimum an iteration can take place:
        while (round(hydro_objects_satisfy.max_ditch_width[i], 1)
               - (hydro_objects_satisfy.water_depth[i] + 0.05 * count) *
               hydro_objects_satisfy.slope[i] * 2 > min_ditch_bottom_width):

            water_depth = hydro_objects_satisfy.water_depth[i] + 0.05 * count
            ditch_width = round(hydro_objects_satisfy.max_ditch_width[i], 1)
            ditch_bottom_width = ditch_width - (water_depth * slope * 2)

            gradient_bos_bijkerke = calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope)

            # Only iterate if the surge is less than 2,5 cm/km.
            tolerance = 0.5  # will be substracted from the gradient norm
            while gradient_bos_bijkerke < (gradient_norm-tolerance):
                ditch_width = ditch_width - 0.05
                ditch_bottom_width = ditch_width - (water_depth * slope * 2)

                gradient_bos_bijkerke = calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope)

                ditch_bottom_width = round(ditch_bottom_width, 2)

            if ditch_bottom_width < min_ditch_bottom_width:
                # print(str(object_id) + ": iteration ends because of a too small ditch bottom width")
                break

            object_waterdepth_id = (str(hydro_objects_satisfy.object_id[i]) + "_" +
                                    str(round(hydro_objects_satisfy.water_depth[i] * 100 + (count * 5), 0)))
            # print (str(object_id) + " at " + str(water_depth) + " has width " + str(ditch_bottom_width))
            df_temp = pd.DataFrame([[object_id,
                                     object_waterdepth_id,
                                     slope,
                                     water_depth,
                                     ditch_width,
                                     ditch_bottom_width,
                                     normative_flow,
                                     gradient_bos_bijkerke]],
                                   columns=['object_id', 'object_waterdepth_id', 'slope',
                                            'water_depth', 'ditch_width', 'ditch_bottom_width',
                                            'normative_flow', 'gradient_bos_bijkerke'])

            variants_table = variants_table.append(df_temp)

            count = count + 1

        object_waterdepth_id = (str(hydro_objects_satisfy.object_id[i]) + "_" +
                                str(round(hydro_objects_satisfy.water_depth[i] * 100 + (count * 5), 0)))
        if count == 0:
            # When normative flow is small,
            # the necessary profile dimensions are smaller than the minimum requirements.
            print ("minimum profile dimensions suffices for object " + str(object_id))
            ditch_bottom_width = 0.5
            ditch_width = ditch_bottom_width + slope * water_depth

            gradient_bos_bijkerke = calc_bos_bijkerke(normative_flow, ditch_bottom_width, water_depth, slope)

            df_temp = pd.DataFrame([[object_id,
                                     object_waterdepth_id,
                                     slope,
                                     water_depth,
                                     ditch_width,
                                     ditch_bottom_width,
                                     normative_flow,
                                     gradient_bos_bijkerke]],
                                   columns=['object_id', 'object_waterdepth_id', 'slope',
                                            'water_depth', 'ditch_width', 'ditch_bottom_width',
                                            'normative_flow', 'gradient_bos_bijkerke'])

            variants_table = variants_table.append(df_temp)
            count = 1

        options_table.possibilities_count[options_table.object_id == options_table.object_id[i]] = count
    variants_table = variants_table.reset_index(drop=True)

    return variants_table


def print_failed_hydro_objects(input_table):
    if "object_id" in input_table.columns:
        if "gradient_bos_bijkerke" in input_table.columns:
            if "gradient_manning" in input_table.columns:
                for i, rows in input_table.iterrows():
                    if max(float(input_table.gradient_bos_bijkerke[i]),
                           float(input_table.gradient_manning[i])) > gradient_norm:
                        print str(input_table.object_id[i]) + " doesn't comply to the norm of " + \
                              str(gradient_norm) + " cm/km."
            else:
                print ("No 'gradient_manning' data")
        else:
            print ("No 'gradient_bos_bijkerke' data")
    else:
        print ("No 'object_id' data")


def show_summary(tablename, surge_comparison):
    summary_table = pd.DataFrame({'how many hydro objects do not suffice': pd.Series(
        [(len(tablename[tablename['gradient_manning'] > gradient_norm])),
         (len(tablename[tablename['gradient_bos_bijkerke'] > gradient_norm])),
         (len(tablename[tablename['surge'] > surge_comparison])),
         len(tablename['object_id'])],
        index=[("# objects with Manning > " + str(gradient_norm)),
               "# objects with Bos and Bijkerke > " + str(gradient_norm),
               "total surge is at least " + str(surge_comparison) + " cm over hydro object",
               "total amount of hydro objects"]
    )})
    print summary_table


"""
This is were the main code starts:
2 functions:
- create_theoretical_profiles
- write_theoretical profiles to database

But first:
Boundary conditions
"""

Km = 25  # Manning coefficient in m**(1/3/s)
Kb = 23  # Bos and Bijkerke coefficient in 1/s

ini_waterdepth = 0.30  # Initial water depth (m).
gradient_norm = 3.0  # (cm/km) The norm for maximum gradient according to Bos and Bijkerke or Manning formula.
min_ditch_bottom_width = 0.5  # (m) Ditch bottom width can not be smaller dan 0,5m.


def create_theoretical_profiles(legger_db_filepath):
    print ("\n")

    # Part 1: read SpatiaLite
    # The original Spatialite database is read into Python for further analysis.
    hydro_objects = read_spatialite(legger_db_filepath)  # print (Hydro_objects)

    # Part 2: Filter the table for hydro objects that can not be analyzed due to incomplete data.
    column = "BREEDTE"  # or any column name
    hydro_objects = filter_unused(hydro_objects, column)

    # Part 3: Calculate per hydro object the legger profile based on maximum ditch width

    # Create an empty table to store the results:
    profile_max_ditch_width = pd.DataFrame(
        columns=['object_id', 'normative_flow', 'length', 'slope', 'max_ditch_width', 'water_depth',
                 'ditch_bottom_width', 'gradient_bos_bijkerke', 'gradient_manning'])

    # Loop over the hydro objects table so hydro object specific information is temporarily saved to variables
    for i, rows in hydro_objects.iterrows():
        object_id = hydro_objects.OBJECTID[i]
        if hydro_objects.GRONDSOORT[i] == "veenweide":  # initial slope of ditch based on soil type
            slope = 3.0
        else:
            slope = 2.0
        max_ditch_width = hydro_objects.BREEDTE[i]
        normative_flow = 0.01  # (m3 / s) hydro_objects.normative_flow[i]  # todo: is not a constant
        length = 10  # (m) hydro_objects.length[i]  # todo: is not a constant

        # Calculate a profile
        profile = calc_profile_max_ditch_width(object_id, normative_flow, length, slope, max_ditch_width)

        # Add the profile to the existing table where the results are stored
        profile_max_ditch_width = profile_max_ditch_width.append(profile)

    # When all the results are stored in the table, re-index the table.
        profile_max_ditch_width = profile_max_ditch_width.reset_index(drop=True)

    # Part 4: Print the hydro objects where no suitable legger can be calculated.
    print_failed_hydro_objects(profile_max_ditch_width)

    """
    Up to here the hydro object information is translated to a legger profile using maximum ditch width.
    """

    # Part 5: add surge.
    surge_table = pd.DataFrame(columns=['object_id', 'surge'])

    for i, rows in profile_max_ditch_width.iterrows():
        object_id = profile_max_ditch_width.object_id[i]
        length = profile_max_ditch_width.length[i]
        gradient = max(float(profile_max_ditch_width.gradient_bos_bijkerke[i]),
                       float(profile_max_ditch_width.gradient_manning[i]))

        surge = calc_surge(object_id, length, gradient)
        df_temp = pd.DataFrame([surge], columns=['object_id', 'surge'])

        surge_table = surge_table.append(df_temp)

    surge_table = surge_table.reset_index(drop=True)
    profile_max_ditch_width = pd.merge(profile_max_ditch_width, surge_table, on='object_id', how='left')

    # Part 6: show a table with some statistics on the hydro objects
    surge_comparison = 5  # (cm) What total surge is interesting to compare it to?
    show_summary(profile_max_ditch_width, surge_comparison)

    # Part 7: From the suitable profiles based on max ditch width, all the other suitable profiles are calculated.
    # 2 tables are created:
    # - a table with the number of available profiles per hydro object
    # - a table with information on every available profile.

    # Uncomment these lines if started from here
    # profile_max_ditch_width = pd.read_excel('whatever name of table with profiles based on max width is')

    # Separate the hydro objects in two groups: hydro objects that satisfy requirements from the ones that don't.

    hydro_objects_satisfy = profile_max_ditch_width[profile_max_ditch_width['gradient_bos_bijkerke'] <= 3.0]

    print (str(len(hydro_objects_satisfy)) + " of the " + str(len(profile_max_ditch_width))
           + " hydro objects satisfy the norm")

    profile_variants = calc_profile_variants(hydro_objects_satisfy)
    print profile_variants

    return profile_variants


def write_theoretical_profile_results_to_db(profile_results, path_legger_db):
    db = LeggerDatabase(
        {
            'db_path': path_legger_db
        },
        'spatialite'
    )
    db.create_and_check_fields()
    session = db.get_session()

    profiles = []

    for i, rows in profile_results.iterrows():
        profiles.append(ProfielVarianten(
            hydro_object_id=profile_results.object_id[i],
            id=profile_results.object_waterdepth_id[i],
            talud=profile_results.slope[i],
            waterdiepte=profile_results.water_depth[i],
            waterbreedte=profile_results.ditch_width[i],
            bodembreedte=profile_results.ditch_bottom_width[i],
            maatgevend_debiet=profile_results.normative_flow[i],
            verhang_bos_bijkerk=profile_results.gradient_bos_bijkerke[i],
        ))

    session.execute("Delete from {0}".format(ProfielVarianten.__tablename__))

    session.bulk_save_objects(profiles)
    session.commit()
