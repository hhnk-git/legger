import logging
import sys, os

try:
    from legger.sql_models.legger_database import load_spatialite
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))

from legger.sql_models.legger_database import load_spatialite, LeggerDatabase
from legger.utils.network import Network
from legger.sql_models.legger_views import create_legger_views
from legger.utils.link_duikers_to_hydrovakken import link_duikers_to_hydrovakken

log = logging.getLogger(__name__)


def calc_and_set_tot_verhang(network, line, tot_verhang, target_level):
    line_target_level = line.target_level
    if target_level != line_target_level:
        tot_verhang = 0

    if tot_verhang is None:
        tot_verhang = None
    elif line.soort_vak == 4:  # pseudovak - geen verhang
        tot_verhang = tot_verhang
    elif line.soort_vak == 3:  # kunstwerkvak
        tot_verhang = tot_verhang
    elif line.verhang is None:
        tot_verhang = tot_verhang
    else:
        verhang = line.length * line.verhang / 100000
        tot_verhang = tot_verhang + verhang

    if line.duiker_count:
        # add 1 cm opstuwing voor elke duiker
        tot_verhang = tot_verhang + line.duiker_count * 0.01

    if line.tot_verhang is not None:
        if line.tot_verhang <= tot_verhang:
            # we already have a lower value for all upstream nodes, so break this iteration
            return
    line.tot_verhang = tot_verhang

    upstream_node = line.inflow_node(modus=line.DEBIET_DB)
    for line_upstream in upstream_node.inflow(modus=line.DEBIET_DB):
        calc_and_set_tot_verhang(network, line_upstream, tot_verhang, line_target_level)


def calc_gradient_for_network(network: Network):
    for start_node in network.graph.get_startnodes():
        for start_line in start_node.inflow(modus=start_node.DEBIET_DB):
            calc_and_set_tot_verhang(network, start_line, 0, start_line.target_level)


def calc_gradient(path_legger_db):
    # step 1: get network
    db = LeggerDatabase(path_legger_db)
    db.create_and_check_fields()

    link_duikers_to_hydrovakken(path_legger_db)

    con_legger = load_spatialite(path_legger_db)
    create_legger_views(con_legger)

    network = Network(path_legger_db)

    calc_gradient_for_network(network)
    con_legger.execute("UPDATE geselecteerd SET tot_verhang='NULL'")

    con_legger.executemany("""
        UPDATE geselecteerd SET
            tot_verhang=?
        WHERE 
            hydro_id=?      
    """, [(line.tot_verhang, line.id) for line in
          network.graph.lines if line.tot_verhang is not None])

    log.info("Save gradient (update) to database ")
    con_legger.commit()


if __name__ == '__main__':
    calc_gradient(r"/Users/bastiaanroos/Documents/testdata/leggertool/Westerkogge.sqlite")
