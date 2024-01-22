import logging
from qgis.core import Qgis

try:
    from legger.utils.user_message import messagebar_message
    from legger.sql_models.legger_views import create_legger_views
    from legger.utils.network import Network, load_spatialite
except ImportError:
    import sys, os

    sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))

    from legger.utils.user_message import messagebar_message
    from legger.sql_models.legger_views import create_legger_views
    from legger.utils.network import Network, load_spatialite

log = logging.getLogger(__name__)


def redirect_flows(path_legger_db, change_flow_direction=True):
    network = Network(path_legger_db)
    network.build_graph_tables()
    if change_flow_direction:
        network.force_direction()
        network.re_distribute_flow()
    else:
        success, left_overs1 = network.re_distribute_flow(attempt=1)
        if not success:
            log.info("distribute_flow attempt 1 failed, try again with flow direction of undefined direction forced")
            network.force_direction(only_without_flow=True)
            success, left_overs = network.re_distribute_flow(attempt=2)
            if not success:
                msg = "Herverdeling is niet geheel gelukt, omdat voor een deel geen oplossing is gevonden. " \
                      "Deze hydrovakken hebben een debiet=null. Oorzaken kunnen zijn dat er (geisoleerderde) " \
                      "circulaire delen aanwezig zijn, de eindpunten zijn verbonden met hydrovakken zonder debiet " \
                      "of verkeerde richting, of te veel ontbrekende 3di debieten in een deel. Eventueel kan het " \
                      "draaien van deze taak met 'stroomrichting aanpasbaar' een oplossing geven. "
                if len(left_overs):
                    objs = []
                    for n in left_overs:
                        objs = objs + [*n.incoming()] + [*n.outgoing()]
                    objs = set([str(l.id) for l in objs])

                    msg += "Delen zonder oplossing omvatten in ieder geval de volgende hydroobjecten: {}".format(
                        ", ".join(objs))
                elif len(left_overs1):
                    objs = []
                    for n in left_overs1:
                        objs = objs + [*n.incoming()] + [*n.outgoing()]
                    objs = set([str(l.id) for l in objs])

                    msg += "Delen zonder oplossing omvatten mogelijk de volgende hydroobjecten: {}".format(
                        ", ".join(objs))

                messagebar_message(
                    'Fout',
                    msg,
                    Qgis.MessageLevel.Warning,
                    duration=-1
                )
                log.warning(msg)

    network.save_network_values()
    log.info("Save redirecting flow result (update) to database ")

    con_legger = load_spatialite(path_legger_db)
    create_legger_views(con_legger)

    #
    # con_legger = load_spatialite(path_legger_db)
    #
    # create_legger_views(con_legger)
    #
    # layer_manager = LeggerMapManager(iface, path_legger_db)
    #
    # line_layer = layer_manager.get_line_layer()
    # # init network
    # line_direct = layer_manager.get_line_layer(geometry_col='line')
    #
    # new_flows, arc_tree = redirect_flow_calculation(line_direct, line_layer)
    #
    # for arc in arc_tree.values():
    #     con_legger.execute("UPDATE hydroobject SET debiet = {0}, debiet_aangepast = {0} WHERE id = {1};".format(
    #         arc['flow_corrected'] if arc['flow_corrected'] is not None else 'NULL', arc['hydro_id']))
    #
    # log.info("Save redirecting flow result (update) to database ")
    # con_legger.commit()


if __name__ == '__main__':
    import sys
    import os

    os.environ["PROJ_LIB"] = "/Applications/QGIS-LTR.app/Contents/Resources/proj"
    os.environ["GDAL_DATA"] = "/Applications/QGIS-LTR.app/Contents/Resources/gdal"
    sys.path.append('/Users/bastiaanroos/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins')

    redirect_flows(
        '//Users/bastiaanroos/Documents/testdata/leggertool/legger_westerkogge2.sqlite',
        change_flow_direction=False
    )
