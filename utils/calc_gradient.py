import logging

from qgis._analysis import QgsVectorLayerDirector
from legger.sql_models.legger_database import load_spatialite, LeggerDatabase
from legger.utils.legger_map_manager import LeggerMapManager
from legger.utils.new_network import NewNetwork
from legger.sql_models.legger_views import create_legger_views
from legger.utils.link_duikers_to_hydrovakken import link_duikers_to_hydrovakken

log = logging.getLogger(__name__)


def calc_and_set_tot_verhang(network, arc, tot_verhang, target_level):
    arc_target_level = arc['target_level']
    if target_level != arc_target_level:
        tot_verhang = 0

    if tot_verhang is None:
        tot_verhang = None
    elif arc['soort_vak'] == 4:  # pseudovak - geen verhang
        tot_verhang = tot_verhang
    elif arc['soort_vak'] == 3:  # kunstwerkvak
        tot_verhang = tot_verhang
    elif arc['verhang'] is None:
        tot_verhang = tot_verhang
    else:
        verhang = arc['length'] * arc['verhang'] / 100000
        tot_verhang += verhang

    if arc['duiker_count']:
        # add 1 cm opstuwing voor elke duiker
        tot_verhang += tot_verhang + arc['duiker_count'] * 0.01

    arc['tot_verhang'] = tot_verhang

    for arc_nr in arc['upstream_arcs']:
        new_arc = network.arc_tree[arc_nr]
        calc_and_set_tot_verhang(network, new_arc, tot_verhang, arc_target_level)


def calc_gradient_for_network(network: NewNetwork):
    for start_arc in network.start_arcs:
        arc_nr = start_arc['arc_nr']
        arc = network.arc_tree[arc_nr]
        calc_and_set_tot_verhang(network, arc, 0, arc['target_level'])


def calc_gradient(iface, path_legger_db):
    # step 1: get network
    db = LeggerDatabase(
        {
            'db_path': path_legger_db
        },
        'spatialite'
    )
    db.create_and_check_fields()

    link_duikers_to_hydrovakken(path_legger_db)

    con_legger = load_spatialite(path_legger_db)
    create_legger_views(con_legger)

    layer_manager = LeggerMapManager(iface, path_legger_db)

    line_layer = layer_manager.get_line_layer()

    # init network
    line_direct = layer_manager.get_line_layer(geometry_col='line')
    field_nr = line_direct.fields().indexFromName('direction')
    director = QgsVectorLayerDirector(
        line_direct, field_nr, '2', '1', '3', 3)

    network = NewNetwork(
        line_direct, line_layer, director, None  # self.vl_tree_layer, self.vl_endpoint_layer
    )

    network.build_tree()

    calc_gradient_for_network(network)

    for arc in network.arc_tree.values():
        if arc['selected_variant_id'] is not None:
            con_legger.execute("UPDATE geselecteerd SET tot_verhang = {0} WHERE hydro_id = {1};".format(
                arc['tot_verhang'] if arc['tot_verhang'] is not None else 'NULL', arc['hydro_id']))

    log.info("Save gradient (update) to database ")
    con_legger.commit()
