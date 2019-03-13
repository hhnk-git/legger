# todo:
#  - debiet koppeling op 'duiker' flowlines. werkt nu alleen op channels. Bijvoorbeeld hydroobject OAF-W-708 met flowline 2279
#  - korte zijslootjes worden nu gekoppeld op hoofdwatergangen. in koppelingscriteria iets van de lengte van de watergang meenemen


import json
import logging
import math

from qgis.core import QgsGeometry, QgsLineStringV2, QgsPoint

try:
    from ThreeDiToolbox.datasource.netcdf_groundwater_h5py import NetcdfGroundwaterDataSourceH5py as NetcdfGroundwaterDataSource
except ImportError:
    from ThreeDiToolbox.datasource.netcdf_groundwater import NetcdfGroundwaterDataSource


from legger.utils.geom_collections.lines import LineCollection
from legger.utils.geometries import LineString
from legger.utils.geometries import shape
from legger.sql_models.legger import DuikerSifonHevel, HydroObject
from legger.sql_models.legger_database import LeggerDatabase
from pyspatialite import dbapi2 as dbapi


log = logging.getLogger(__name__)


# suggestions for improvement:
# - limit max_link_distance related to length of line (no somtimes small lines at end and
#   begin point link.

def create_geom_line(coordinates, maincall=True):
    """recursive function to make QgsPoints from coordinates (point, linestring, multilinestring)"""
    if len(coordinates) > 0 and type(coordinates[0]) == list:
        list_of_geoms = [create_geom_line(coord, False) for coord in coordinates]

        if maincall:
            if type(list_of_geoms[0]) == QgsPoint:
                return QgsGeometry.fromPolyline(list_of_geoms)
            else:
                return QgsGeometry.fromMultiPolyline(list_of_geoms)
        else:
            return list_of_geoms
    else:
        return QgsPoint(coordinates[0], coordinates[1])


def read_tdi_results(path_model_db, path_result_db,
                     path_result_nc, path_legger_db,
                     timestep=-1,
                     max_link_distance=5.0,
                     match_criteria=3):
    """ joins hydoobjects to 3di-flowlines (through channels) and add the discharge of the selected timestep

    path_model_db (str): filepath to sqlite database with 3di model
    path_result_db (str): filepath to sqlite database linked to the 3di results (.sqlite1 file)
    path_result_nc (str):  filepath to netcdf with 3di results (same directory must include the idmapping and
                              aggregated netcdf file)
    path_legger_db (str): filepath to sqlite database with legger data
    timestep (int): index of output timestep, -1 is last timestep
    max_link_distance (float): maximum distance in meters between hydroobject
    match_criteria (float):
    returns:
        list of dictionaries with model results of hydroobjects

    notes:
    - function uses rdnew (EPSG:28992), so only usable in the Netherlands
    - functions of the ThreeDiToolbox qgis-plugin are used, so make sure this plugin is available
    """

    con_model = dbapi.connect(path_model_db)
    con_res = dbapi.connect(path_result_db)
    con_legger = dbapi.connect(path_legger_db)

    result_ds = NetcdfGroundwaterDataSource(path_result_nc)

    # make sure we got dictionaries returned
    con_model.row_factory = dbapi.Row
    con_res.row_factory = dbapi.Row
    con_legger.row_factory = dbapi.Row

    # read discharge of timestep. Returns numpy array with index number is idx.
    # to link to model channels, the id mapping is needed.
    if timestep == -1:
        timestep = len(result_ds.timestamps) - 1

    qts = result_ds.get_values_by_timestep_nr('q', timestep)

    # get all 3di channels (for linking to flowlines and hydrovakken)
    channel_cursor = con_model.execute(
        'SELECT '
        'id,'
        'ASWKT(TRANSFORM(the_geom, 28992)) AS wkt, '
        'ASGEOJSON(TRANSFORM(the_geom, 28992)) AS geojson, '
        'ST_LENGTH(TRANSFORM(the_geom, 28992)) AS length '
        'FROM v2_channel '
        # 'WHERE id=7119 ' # for testing and debugging
    )

    channel_col = LineCollection()

    for channel in channel_cursor.fetchall():
        flowlines = []

        # get flowlines on channel (join on spatialite_id),
        # including start distance and end distance of flowline on channel
        flowline_cursor = con_res.execute(
            "SELECT fl.id, fl.spatialite_id, fl.type, "
            "Line_Locate_Point(GEOMFROMTEXT(:wkt), TRANSFORM(sn.the_geom, 28992)) * :length AS start_distance, "
            "Line_Locate_Point(GEOMFROMTEXT(:wkt), TRANSFORM(en.the_geom, 28992)) * :length AS end_distance "
            "FROM flowlines fl "
            "LEFT OUTER JOIN nodes sn ON fl.start_node_idx = sn.id "
            "LEFT OUTER JOIN nodes en ON fl.end_node_idx = en.id "
            "WHERE fl.type = 'v2_channel' AND fl.spatialite_id = :id;",
            {
                'id': channel['id'],
                'wkt': channel['wkt'],
                'length': channel['length']
            }
        )

        # append flowlines to channel, including flow on selected timestep
        for fl in flowline_cursor.fetchall():
            flowline = {
                'id': fl['id'],
                'spatialite_id': fl['spatialite_id'],
                'start_distance': fl['start_distance'],
                'end_distance': fl['end_distance'],
                'q_end': qts[fl['id'] - 1]  # todo: check correct (with +1 etc.)
            }

            flowlines.append(flowline)

        # write channel to collection
        channel_col.write({
            'geometry': json.loads(channel['geojson']),
            'properties': {
                'id': channel['id'],
                'length': channel['length'],
                'flowlines': flowlines
            }
        })

    hydro_cursor = con_legger.execute(
        'SELECT '
        'id, '
        'ASGEOJSON(geometry) AS geojson, '
        'ST_LENGTH(geometry) AS length '
        'FROM hydroobject '
    )
    hydroobjects = []

    # loop over all hydroobjects and find flowline through link with channel
    for hydroobject in hydro_cursor.fetchall():

        if hydroobject['id'] in [198616, 198362, 659550]:
            a = 1

        line = create_geom_line(json.loads(hydroobject['geojson'])['coordinates'])
        # with shapely
        # line = LineString(
        #     json.loads(hydroobject['geojson'])['coordinates'])

        bbox = line.boundingBox()
        bbox = bbox.buffer(max_link_distance)
        bbox = [bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum()]
        # with shapely
        # bbox = line.bounds
        # bbox = (
        #     bbox[0] - max_link_distance,
        #     bbox[1] - max_link_distance,
        #     bbox[2] + max_link_distance,
        #     bbox[3] + max_link_distance,
        # )

        # create 9 point on the line to check matching geometries
        length = line.length()
        # with shapely
        # length = line.length

        points_on_line = [
            line.interpolate((float(nr) / 10.0) * length)  # normalized=True
            for nr in range(1, 10)
            ]

        # print(', '.join([str(point.x) for point in points_on_line]))

        candidates = []

        for channel in channel_col.filter(bbox=bbox):

            geom_channel = create_geom_line(channel['geometry']['coordinates'])
            # with shapely
            # geom_channel = shape(channel['geometry'])

            distances = [
                geom_channel.distance(point)
                for point in points_on_line
            ]

            # check how many points are matching
            nr_matches = sum([1 for distance in distances if distance <= max_link_distance])

            if nr_matches >= match_criteria:
                # lower score is better
                score = sum([
                    min(dist, max_link_distance)
                    for dist in distances
                ]) / max_link_distance
                candidates.append((
                    score,
                    channel,
                    geom_channel
                ))

        if len(candidates) > 0:
            score, selected, geom_channel = sorted(candidates, key=lambda item: item[0])[0]

            # get orientation of hydroobject vs channel
            if line.isMultipart():
                vertexes = line.asMultiPolyline()
                d1, p1, v1 = geom_channel.closestSegmentWithContext(vertexes[0][0])
                d2, p2, v2 = geom_channel.closestSegmentWithContext(vertexes[-1][-1])
            else:
                vertexes = line.asPolyline()
                d1, p1, v1 = geom_channel.closestSegmentWithContext(vertexes[0])
                d2, p2, v2 = geom_channel.closestSegmentWithContext(vertexes[-1])

            if (d1 > 50 or d2> 50):
                a = 2

            # v1 = v1 -1 if v1 > 0 else 0
            # v2 = v2 - 1 if v2 > 0 else 0

            dist1 = geom_channel.distanceToVertex(v1) - math.sqrt(geom_channel.sqrDistToVertexAt(p1, v1))
            dist2 = geom_channel.distanceToVertex(v2) - math.sqrt(geom_channel.sqrDistToVertexAt(p2, v2))
            # dist1 = dist1 * line.length()
            # dist2 = dist2 * line.length()
            # with shapely:
            # dist1 = geom_channel.project(line.startpoint())
            # dist2 = geom_channel.project(line.endpoint())

            dist_startpoint = min(dist1, dist2)
            dist_endpoint = max(dist1, dist2)
            if dist2 < dist1:
                factor = -1
            else:
                factor = 1

            # get best flowline
            flowline_candidates = []

            for fl in selected['properties']['flowlines']:
                # add 1.0 meter because distance calculation differ a small bit between platforms
                # if (dist_startpoint <= fl['end_distance'] + 1.0 and
                #             dist_endpoint >= fl['start_distance'] - 1.0):
                matching_length = (min(dist_endpoint, fl['end_distance'] -
                                       max(dist_startpoint, fl['start_distance'])))

                flowline_candidates.append((
                    matching_length,
                    fl
                ))

            if len(flowline_candidates) == 0:
                log.warning(
                    'Hydroboject %i was linked to channel %i, but it was not possible to '
                    'link both. Maybe hydroobject is small line at begin or end',
                    hydroobject['id'],
                    selected['properties']['id'])

            else:
                matching_length, selected_fl = sorted(
                    flowline_candidates,
                    key=lambda item: item[0],
                    reverse=True
                )[0]

                hydroobjects.append({
                    'id': hydroobject['id'],
                    'qend': selected_fl['q_end'] * factor,
                    'channel_id': selected['properties']['id'],
                    'flowline_id': selected_fl['id'],
                    'nr_candidates': len(candidates),
                    'score': score
                })

    return hydroobjects


def write_tdi_results_to_db(hydroobject_results, path_legger_db):
    db = LeggerDatabase(
        {
            'db_path': path_legger_db
        },
        'spatialite'
    )
    # db.create_and_check_fields()
    session = db.get_session()

    results = {hydro['id']: hydro for hydro in hydroobject_results}

    for hydroobj in session.query(HydroObject):
        if hydroobj.id in results:
            result = results[hydroobj.id]
            hydroobj.debiet = result['qend']
            hydroobj.channel_id = result['channel_id']
            hydroobj.flowline_id = result['flowline_id']
            hydroobj.score = result['score']

    log.info("Save 3di results (update hydro objects) to database ")
    session.commit()


def read_tdi_culvert_results(path_model_db, path_result_db,
                             path_result_nc, path_legger_db,
                             timestep):
    """

    path_model_db (str): path to 3di modelspatialite
    path_result_db (str): path to 3di gridadmin spatialite
    path_result_nc (str): path to 3di result netcdf (format with groundwater of june 2018)
    path_legger_db (str): path to legger spatialite
    timestep (int): index of output timestep, -1 is last timestep
    :return:
    """

    # open databases and netCDF
    con_model = dbapi.connect(path_model_db)
    con_result = dbapi.connect(path_result_db)
    con_legger = dbapi.connect(path_legger_db)

    result_ds = NetcdfGroundwaterDataSource(path_result_nc)

    # make sure we got dictionaries returned
    con_model.row_factory = dbapi.Row
    con_result.row_factory = dbapi.Row
    con_legger.row_factory = dbapi.Row

    # read discharge of  timestep. Returns numpy array with index number is idx.
    # to link to model channels, the id mapping is needed.
    if timestep == -1:
        timestep = len(result_ds.timestamps) - 1

    qts = result_ds.get_values_by_timestep_nr('q', timestep)

    culverts = []

    # process reading for multiple object types
    for tp in ['v2_culvert', 'v2_orifice']:

        # create id-mapping
        rcursor = con_result.execute("""
            SELECT id, spatialite_id
            FROM flowlines
            WHERE type = '{0}'""".format(tp))

        id_mapping = {f['spatialite_id']: f['id'] for f in rcursor.fetchall()}

        # get all objects (including code)
        mcursor = con_model.execute(
            'SELECT '
            'id, code '
            'FROM ' + tp
        )

        for culvert in mcursor.fetchall():
            idx = id_mapping.get(culvert['id'])

            culverts.append({
                'code': culvert['code'],
                'id': culvert['id'],
                'source': tp,
                'idx': idx,
                'qend': qts[idx]
            })

    return culverts


def write_tdi_culvert_results_to_db(culvert_results, path_legger_db):
    db = LeggerDatabase(
        {
            'db_path': path_legger_db
        },
        'spatialite'
    )
    db.create_and_check_fields()
    session = db.get_session()

    results = {culvert['code']: culvert for culvert in culvert_results}

    for culvert in session.query(DuikerSifonHevel):
        if culvert.code in results:
            culvert.debiet = results[culvert.code]['qend']
            # culvert['source']
            # culvert['code']

    log.info("Save 3di result (update) to database ")
    session.commit()


def get_timestamps(path_result_nc, parameter=None):
    """get timesteps available

    path_result_nc (str): path to 3di result netcdf
    parameter (str): parameter identification
    :return:
    """
    result_ds = NetcdfGroundwaterDataSource(path_result_nc)

    return result_ds.get_timestamps(parameter=parameter)
