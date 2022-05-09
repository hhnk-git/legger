# -*- coding: utf-8 -*-
import logging
import sqlite3
from collections import OrderedDict
from typing import List

from qgis._core import QgsFeatureRequest, QgsFeature, QgsGeometry, QgsPointXY, QgsExpression
from legger.qt_models.legger_tree import hydrovak_class, LeggerTreeItem
from legger.utils.formats import make_type

DISTANCE_PROPERTER_NR = 0
FEAT_ID_PROPERTER_NR = 1

min_flow = 0.0000001

logger = logging.getLogger(__name__)


def set_spatial_extension(con, connection_record):
    """Load spatialite extension as described in
    https://geoalchemy-2.readthedocs.io/en/latest/spatialite_tutorial.html"""
    import sqlite3

    con.enable_load_extension(True)
    cur = con.cursor()
    libs = [
        # SpatiaLite >= 4.2 and Sqlite >= 3.7.17, should work on all platforms
        ("mod_spatialite", "sqlite3_modspatialite_init"),
        # SpatiaLite >= 4.2 and Sqlite < 3.7.17 (Travis)
        ("mod_spatialite.so", "sqlite3_modspatialite_init"),
        # SpatiaLite < 4.2 (linux)
        ("libspatialite.so", "sqlite3_extension_init"),
    ]
    found = False
    for lib, entry_point in libs:
        try:
            cur.execute("select load_extension('{}', '{}')".format(lib, entry_point))
        except sqlite3.OperationalError:
            logger.exception(
                "Loading extension %s from %s failed, trying the next", entry_point, lib
            )
            continue
        else:
            logger.info("Successfully loaded extension %s from %s.", entry_point, lib)
            found = True
            break
    if not found:
        raise RuntimeError("Cannot find any suitable spatialite module")
    cur.close()
    con.enable_load_extension(False)


def load_spatialite(path: str):
    con = sqlite3.connect(path)
    set_spatial_extension(con, '')
    return con


def merge_dicts(x, y):
    """merge two dictionaries into a new dictionary (for python 2.x).
    For python 3.x use {**x, **y}"""
    z = x.copy()
    z.update(y)
    return z


class Definitions(object):
    DEBIET_3DI = '3di'
    DEBIET_CORRECTED = 'corrected'
    FORCED = 'forced'
    DEBIET_DB = 'db'


class Line(Definitions):

    def __init__(self, oid, startnode_id, endnode_id, category, length,
                 debiet_3di, debiet, debiet_modified, target_level, has_startnode=False, **extra):
        self.graph = None
        self.nr = None
        self.id = oid
        self.startnode_id = startnode_id
        self.endnode_id = endnode_id
        self.startnode_nr = None
        self.endnode_nr = None
        self.has_startnode = has_startnode

        self.category = category
        self.length = length
        self.target_level = target_level
        self.debiet_3di = debiet_3di
        self.debiet_db = debiet
        self.debiet_modified = debiet_modified
        self.surplus = None

        self.reversed = False
        self.forced_direction = False

        self.extra_data = {
            **extra
        }

    def start_node(self):
        return self.graph.node(self.startnode_nr)

    def end_node(self):
        return self.graph.node(self.endnode_nr)

    def debiet(self, modus=Definitions.DEBIET_3DI):
        if modus == Definitions.DEBIET_CORRECTED:
            return self.debiet_modified
        elif modus == Definitions.DEBIET_DB:
            return self.debiet_db
        else:
            return self.debiet_3di

    def set_debiet_modified(self, debiet, surplus=None):
        if self.id == 140870:
            a = 1

        self.surplus = surplus
        if debiet is None:
            self.debiet_modified = debiet
        elif self.debiet_3di is not None and self.debiet_3di < 0 and not self.forced_direction:
            self.debiet_modified = -abs(debiet)
        else:
            self.debiet_modified = abs(debiet)

    def inflow_node(self, modus=Definitions.DEBIET_3DI):
        if self.debiet(modus) is None or self.debiet(modus) >= 0 or (
                modus == Definitions.FORCED and self.forced_direction):
            return self.graph.node(self.startnode_nr)
        else:
            return self.graph.node(self.endnode_nr)

    def outflow_node(self, modus=Definitions.DEBIET_3DI):
        if self.debiet(modus) is None or self.debiet(modus) >= 0 or (
                modus == Definitions.FORCED and self.forced_direction):
            return self.graph.node(self.endnode_nr)
        else:
            return self.graph.node(self.startnode_nr)

    def weight(self):
        if self.debiet_3di is None:
            return self.length
        elif self.debiet_3di > 0:
            return self.length / 5
        else:  # self.debiet_3di < 0:
            return self.length * 5

    def weight_rev(self):
        if self.debiet_3di is None:
            return self.length
        elif self.debiet_3di > 0:
            return self.length * 5
        else:  # self.debiet_3di < 0:
            return self.length / 5

    def reverse(self):
        self.reversed = not self.reversed
        if self.debiet_3di is not None:
            self.debiet_3di = -self.debiet_3di
        if self.debiet_modified is not None:
            self.debiet_modified = -self.debiet_modified
        if self.debiet_db is not None:
            self.debiet_db = -self.debiet_db

        start_node = self.start_node()
        end_node = self.end_node()

        # remove links on nodes on both sides
        start_node.outgoing_nrs = [nr for nr in start_node.outgoing_nrs if nr != self.nr]
        end_node.incoming_nrs = [nr for nr in end_node.incoming_nrs if nr != self.nr]

        # switch
        self.startnode_id, self.endnode_id = self.endnode_id, self.startnode_id
        self.startnode_nr, self.endnode_nr = self.endnode_nr, self.startnode_nr
        start_node, end_node = end_node, start_node

        # add links on nodes on both sides
        start_node.outgoing_nrs.append(self.nr)
        end_node.incoming_nrs.append(self.nr)


class Node(Definitions):

    def __init__(self, oid, point, **extra):
        self.graph = None
        self.nr = None
        self.id = oid

        self.min_category = 999

        self.point = point

        self.incoming_nrs = []
        self.outgoing_nrs = []

    def incoming(self):
        return [self.graph.line(nr) for nr in self.incoming_nrs]

    def outgoing(self):
        return [self.graph.line(nr) for nr in self.outgoing_nrs]

    def waterbalance(self, modus=Definitions.DEBIET_3DI):
        flow = self.flow(modus)

        return flow.get('outflow_debiet') - flow.get('inflow_debiet')

    def inflow(self, modus=Definitions.DEBIET_3DI, include_nones=False):
        out = []
        forced = modus == Definitions.FORCED
        for line in self.incoming():
            flow = line.debiet(modus)
            if flow is None or flow >= 0.0 or (forced and line.forced_direction):
                # keep defined direction
                out.append(line)
        for line in self.outgoing():
            flow = line.debiet(modus)
            if include_nones and flow is None:
                out.append(line)
            elif flow is not None and flow < 0.0 and (not forced or not line.forced_direction):
                out.append(line)
        return out

    def outflow(self, modus=Definitions.DEBIET_3DI, include_nones=False):
        out = []
        forced = modus == Definitions.FORCED
        for line in self.outgoing():
            flow = line.debiet(modus)
            if flow is None or flow >= 0.0 or (forced and line.forced_direction):
                # keep defined direction
                out.append(line)
        for line in self.incoming():
            flow = line.debiet(modus)
            if include_nones and flow is None:
                out.append(line)
            if flow is not None and flow < 0.0 and (not forced or not line.forced_direction):
                out.append(line)
        return out

    def flow(self, modus=Definitions.DEBIET_3DI):
        inflow = self.inflow(modus)
        outflow = self.outflow(modus)

        if modus == Definitions.DEBIET_CORRECTED:
            field = 'debiet_modified'
        else:
            field = 'debiet_3di'

        return {
            'inflow_nr': len(inflow),
            'outflow_nr': len(outflow),
            'inflow_debiet': sum([(abs(getattr(l, field)) if getattr(l, field) is not None else 0) for l in inflow]),
            'outflow_debiet': sum([(abs(getattr(l, field)) if getattr(l, field) is not None else 0) for l in outflow]),
        }

    def flow_modified(self):
        return self.flow('debiet_modified')


class Graph(Definitions):

    def __init__(self, nodes: List[Node], lines: List[Line]):
        self.nodes = nodes
        self.lines = lines

        self.manual_startnodes = []

        # set nr and update links
        for nr, node in enumerate(self.nodes):
            node.nr = nr
            node.graph = self
        for nr, line in enumerate(self.lines):
            line.nr = nr
            line.graph = self

        nodes_dict = {node.id: node for node in nodes}

        for line in lines:
            start_node = nodes_dict[line.startnode_id]
            start_node: Node
            line.startnode_nr = start_node.nr
            start_node.outgoing_nrs.append(line.nr)

            end_node = nodes_dict[line.endnode_id]
            end_node: Node
            line.endnode_nr = end_node.nr
            end_node.incoming_nrs.append(line.nr)

            start_node.min_category = min(start_node.min_category, line.category) \
                if line.category is not None else start_node.min_category
            end_node.min_category = min(end_node.min_category, line.category) \
                if line.category is not None else end_node.min_category

            if line.has_startnode:
                self.manual_startnodes.append(line.outflow_node())
        self.manual_startnodes = list(set(self.manual_startnodes))

    def node(self, nr) -> Node:
        return self.nodes[nr]

    def line(self, nr) -> Line:
        return self.lines[nr]

    def correct_direction_on_debiet_3di(self):
        for line in self.lines:
            if line.debiet_3di is not None and line.debiet_3di < 0:
                line.reverse()

    def correct_direction_on_debiet_modified(self):
        for line in self.lines:
            if line.debiet_modified is not None and line.debiet_modified < 0:
                line.reverse()

    def get_startnodes(self, modus=Definitions.DEBIET_3DI, ignore_manual=False) -> List[Node]:
        if not ignore_manual and len(self.manual_startnodes) > 0:
            return self.manual_startnodes
        start_nodes = []
        for node in self.nodes:
            if len(node.outflow(modus)) == 0:
                start_nodes.append(node)
        return start_nodes

    def get_endnodes(self, modus=Definitions.DEBIET_3DI) -> List[Node]:
        end_nodes = []
        for node in self.nodes:
            if len(node.inflow(modus)) == 0:
                end_nodes.append(node)
        return end_nodes


class Network(object):
    """Network class for providing network functions and direrequired for Legger tool"""

    # todo:
    #     - move virtual_layer and endpoint_layer outside this class
    #     - set endpoints on 90% or 10 meter before endpoint of hydrovak

    def __init__(self, spatialite_path, graph=None,
                 full_line_layer=None, virtual_tree_layer=None, endpoint_layer=None, id_field="id"):
        """
        spatialite_path (str): path to spatialite
        line_layer (QgsVectorLayer): input vector layer, with as geometry straight lines without in between vertexes
        full_line_layer (QgsVectorLayer): input vector layer, with original geometry (with in between vertexes)
        director (QgsLineVectorLayerDirector):
        distance_properter (Qgs Properter type): properter to get distance. used for shortest path at bidirectional
                islands
        virtual_tree_layer (QgsVectorLayer): layer used to visualize active tree
        endpoint_layer (QgsVectorLayer): layer used ot visualize endpoints of tree
        id_field (str): field used by features to identification field
        """

        self.spatialite_path = spatialite_path

        self._cursor = None

        self._graph = graph

        # init class attributes
        self.start_arcs = None  # list of dicts with arc_nr, point (x, y), list childs, parent
        self.start_arc_tree = None
        self.arc_tree = None  # dictionary with tree data in format {[arc_nr]: {**arc_data}}

        self.full_line_layer = full_line_layer
        self._virtual_tree_layer = virtual_tree_layer
        self._endpoint_layer = endpoint_layer
        self.id_field = id_field

    @property
    def db_cursor(self):
        if not self._cursor:
            con_legger = load_spatialite(self.spatialite_path)
            self._cursor = con_legger.cursor()

        return self._cursor

    def save_network_values(self):
        start_nodes = [n.id for n in self.graph.get_startnodes(modus=Definitions.DEBIET_3DI)]

        def final_debiet(line):
            is_positive = line.debiet_modified is None or \
                          (line.reversed and line.debiet_modified < 0) or \
                          (not line.reversed and line.debiet_modified >= 0)

            if is_positive:
                if line.category == 1:
                    return max(abs(line.debiet_3di or 0), abs(line.debiet_modified or 0))
                else:
                    return abs(line.debiet_modified)
            else:
                if line.category == 1:
                    return -max(abs(line.debiet_3di or 0), abs(line.debiet_modified or 0))
                else:
                    return -abs(line.debiet_modified)

        self.db_cursor.executemany("""
            UPDATE hydroobject SET
                eindpunt_potentieel=?,
                geforceerd_omgedraaid=?,
                routing_gewicht=?,
                debiet_aangepast=?,
                debiet=?
            WHERE 
                id=?      
        """, [(l.outflow_node().id in start_nodes,
               l.forced_direction,
               l.extra_data.get('weight'),
               -l.debiet_modified if l.debiet_modified is not None and l.reversed else l.debiet_modified,
               final_debiet(l),
               l.id) for l in self.graph.lines])

        self.db_cursor.executemany("""
            UPDATE graph_nodes SET
                org_wb=?,
                new_wb=?
            WHERE 
                id=?      
        """, [(n.waterbalance(modus=Definitions.DEBIET_3DI), n.waterbalance(modus=Definitions.DEBIET_CORRECTED), n.id)
              for n in self.graph.nodes])

        self.db_cursor.execute("""
                    UPDATE graph_nodes 
                    SET
                        wb_diff= (org_wb - new_wb)      
                """)

        self.db_cursor.execute("""SELECT UpdateLayerStatistics(\'hydroobject\');""")
        self.db_cursor.connection.commit()

    def build_graph_tables(self):

        self.db_cursor.executescript("""
        -- maak alle begin en eindpunten
            DROP TABLE IF EXISTS tmp_line_nodes
            ;

            CREATE TABLE tmp_line_nodes AS
            SELECT ROW_NUMBER() OVER (ORDER BY hydro_id, type DESC) as id, line_nodes.*
            FROM
            (SELECT id || 's' as ids, id as hydro_id, 'start' as type,  ST_Startpoint(GEOMETRY) AS geometry
                FROM hydroobject
                UNION ALL
                SELECT id || 'e' as ids, id as hydro_id, 'eind' as type,  ST_Endpoint(GEOMETRY) AS geometry
                FROM hydroobject) AS line_nodes
            ;

            -- registreer de geometry (alleen bij debuggen)
            --SELECT RecoverGeometryColumn( 'tmp_line_nodes' , 'geometry' , 28992 , 'POINT' );

            -- combineer begin en eindpunten tot unieke punten voor elke locatie 
            DROP TABLE IF EXISTS graph_nodes
            ;

            CREATE TABLE graph_nodes AS
            SELECT 
            ROW_NUMBER() OVER (ORDER BY id) as id, 
            geometry, 
            0.0 as org_wb, 
            0.0 as new_wb, 
            0.0 as wb_diff,
            group_concat(id) as tmp_line_node_id, group_concat(ids) as tmp_line_node_ids
            FROM
            tmp_line_nodes
            GROUP BY geometry
            ORDER BY id
            ;

            -- registreer de geometry
            SELECT RecoverGeometryColumn( 'graph_nodes' , 'geometry' , 28992 , 'POINT' );

            -- de graph lines als verlengde van de hydrovakken
            DROP TABLE IF EXISTS graph_lines
            ;

            CREATE TABLE graph_lines AS
            SELECT 
                ho.id as hydro_id, 
                gns.id as startnode_id, 
                gns.tmp_line_node_ids, 
                gne.id as endnode_id, 
                gne.tmp_line_node_ids,
                ST_X(gns.geometry) as startx,
                ST_Y(gns.geometry) as starty,
                ST_X(gne.geometry) as endx,
                ST_Y(gne.geometry) as endy
            FROM
            hydroobject ho
            INNER JOIN tmp_line_nodes tlns ON tlns.hydro_id =  ho.id AND tlns.type = 'start'
            INNER JOIN graph_nodes gns ON tlns.geometry = gns.geometry
            INNER JOIN tmp_line_nodes tlne ON tlne.hydro_id =  ho.id AND tlne.type = 'eind'
            INNER JOIN graph_nodes gne ON tlne.geometry = gne.geometry
            ;

            -- ruim tijdelijke tabellen op
            DROP TABLE IF EXISTS tmp_line_nodes
            ;
        """)
        self.db_cursor.execute('vacuum')

        return

    @property
    def graph(self):
        if self._graph is None:
            self.db_cursor.execute("""
                SELECT id, ST_X(geometry), ST_Y(geometry) FROM graph_nodes
            """)

            nodes = [Node(nr=nr, oid=r[0], point=(r[1], r[2])) for nr, r in enumerate(self.db_cursor.fetchall())]

            self.db_cursor.execute("""
                SELECT gl.hydro_id as hydro_id, gl.startnode_id, gl.endnode_id, ho.categorieoppwaterlichaam, 
                ST_LENGTH(ho.geometry), ho.debiet_3di, ho.debiet, ho.streefpeil, ho.eindpunt_geselecteerd
                FROM 
                    graph_lines gl
                INNER JOIN hydroobject ho ON ho.id = gl.hydro_id
            """)
            lines = [
                Line(nr=nr, oid=r[0], startnode_id=r[1], endnode_id=r[2], category=r[3], length=r[4], debiet_3di=r[5],
                     debiet=r[6], debiet_modified=None, target_level=r[7],
                     has_startnode=r[8])
                for nr, r in enumerate(self.db_cursor.fetchall())]

            self._graph = Graph(
                lines=lines,
                nodes=nodes
            )
        return self._graph

    def force_direction(self, mode=Definitions.DEBIET_3DI, do_reverse=True, ignore_manual_startnodes=False,
                        start_min_category=1):
        # line_nr, weight, freeze
        tree = [[None, float("inf"), False, n.min_category] for n in self.graph.nodes]

        # node, tot_weight
        all_start_nodes = [[n, 0, n.min_category] for n in
                           self.graph.get_startnodes(mode, ignore_manual=ignore_manual_startnodes)
                           if n.min_category <= start_min_category]

        primary_start_nodes = [n[:2] for n in all_start_nodes
                               if n[2] <= start_min_category]

        queue = primary_start_nodes

        # first see what can be reached using the current directions or in case of None values be walking over these
        # primary weight is length and in case of none debiet 2x length
        while len(queue) > 0:
            node, tot_weight = queue.pop()
            for line in node.inflow(mode, include_nones=True):
                # if line.id in [474252, 140888]:
                #     a = 1

                line: Line
                if line.category == 1:
                    to_node = line.inflow_node(mode)
                    if line.debiet(mode) is None:
                        factor = 2
                    else:
                        factor = 1
                    weight = tot_weight + factor * line.length
                    if weight < tree[to_node.nr][1]:
                        tree[to_node.nr][0] = line.nr
                        tree[to_node.nr][1] = weight
                        queue.append([to_node, weight])

        # freeze all values made based on connection with startnodes
        tree = [[line_nr, weight, line_nr is not None, min_cat] for line_nr, weight, freeze, min_cat in tree]

        # second, force primary not yet reached by routing
        # primary weight is length, in case of none debiet 2x length and in case of negative debiet 3x length
        # now add 'shortest' route algoritm for 'shortest' path to both flow locations
        # include all done nodes in queue
        queue = [[self.graph.node(node_nr), weight] for node_nr, [line_nr, weight, freeze, min_cat] in enumerate(tree)
                 if line_nr is not None and min_cat == 1]
        while len(queue) > 0:
            node, tot_weight = queue.pop()
            for line in node.outgoing():
                line: Line
                if line.category == 1:
                    to_node = line.end_node()
                    if tree[to_node.nr][2]:
                        # freezed, so skip
                        continue
                    if line.debiet(mode) is None:
                        factor = 2
                    elif line.debiet(mode) >= 0:
                        factor = 1
                    else:
                        factor = 3
                    weight = tot_weight + factor * line.length
                    if weight < tree[to_node.nr][1]:
                        tree[to_node.nr][0] = line.nr
                        tree[to_node.nr][1] = weight
                        queue.append([to_node, weight])

            for line in node.incoming():
                line: Line
                if line.category == 1:
                    to_node = line.start_node()
                    if tree[to_node.nr][2]:
                        # freezed, so skip
                        continue
                    if line.debiet(mode) is None:
                        factor = 2
                    elif line.debiet(mode) <= 0:
                        factor = 1
                    else:
                        factor = 3
                    weight = tot_weight + factor * line.length
                    if weight < tree[to_node.nr][1]:
                        tree[to_node.nr][0] = line.nr
                        tree[to_node.nr][1] = weight
                        queue.append([to_node, weight])

        # freeze all values made based on connection with
        tree = [[line_nr, weight, line_nr is not None, min_cat] for line_nr, weight, freeze, min_cat in tree]

        # third, link other categories following the flow_direction.
        # category 2 weight is 1.5 X length, other categories are 2 x length
        # for debiet is none values x 2
        # include all done nodes in queue
        queue = [[self.graph.node(node_nr), weight] for node_nr, [line_nr, weight, freeze, min_cat] in enumerate(tree)
                 if line_nr is not None]

        while len(queue) > 0:
            node, tot_weight = queue.pop()
            # if node.id == 113:
            #     a = 1

            for line in node.inflow(mode, include_nones=True):
                line: Line
                to_node = line.inflow_node(mode)
                if tree[to_node.nr][2]:
                    # freezed, so skip
                    continue

                if line.debiet(mode) is None:
                    factor = 2
                else:
                    factor = 1
                if line.category == 2:
                    factor *= 1.5
                else:
                    factor *= 2
                weight = tot_weight + factor * line.length
                if weight < tree[to_node.nr][1]:
                    tree[to_node.nr][0] = line.nr
                    tree[to_node.nr][1] = weight
                    queue.append([to_node, weight])

        # freeze all values made based on connection with
        tree = [[line_nr, weight, line_nr is not None, min_cat] for line_nr, weight, freeze, min_cat in tree]

        # last, force water not yet reached by routing
        # category 2 weight is 1.5 X length, other categories are 2 x length
        # for debiet is none values x 2, in case of negative debiet x 3
        # now add 'shortest' route algoritm for 'shortest' path to both flow locations
        # include all done nodes in queue
        queue = [[self.graph.node(node_nr), weight] for node_nr, [line_nr, weight, freeze, min_cat] in enumerate(tree)
                 if line_nr is not None]

        # additional startnodes

        while len(queue) > 0:
            node, tot_weight = queue.pop()
            # if node.id == 113:
            #     a = 1

            for line in node.outgoing():
                line: Line
                to_node = line.end_node()
                if tree[to_node.nr][2]:
                    # freezed, so skip
                    continue
                if line.debiet(mode) is None:
                    factor = 2
                elif line.debiet(mode) >= 0:
                    factor = 1
                else:
                    factor = 3

                if line.category == 2:
                    factor *= 1.5
                else:
                    factor *= 2

                weight = tot_weight + factor * line.length
                if weight < tree[to_node.nr][1]:
                    tree[to_node.nr][0] = line.nr
                    tree[to_node.nr][1] = weight
                    queue.append([to_node, weight])

            for line in node.incoming():
                line: Line
                to_node = line.start_node()
                if tree[to_node.nr][2]:
                    # freezed, so skip
                    continue
                if line.debiet(mode) is None:
                    factor = 2
                elif line.debiet(mode) <= 0:
                    factor = 1
                else:
                    factor = 3

                if line.category == 2:
                    factor *= 1.5
                else:
                    factor *= 2

                weight = tot_weight + factor * line.length
                if weight < tree[to_node.nr][1]:
                    tree[to_node.nr][0] = line.nr
                    tree[to_node.nr][1] = weight
                    queue.append([to_node, weight])

        for node_nr, [line_nr, weight, _, _] in enumerate(tree):
            if line_nr is not None:
                line = self.graph.line(line_nr)
                # if line.id == 1084919:
                #     a = 1

                line.extra_data['weight'] = weight
                if line.inflow_node(mode).nr != node_nr:
                    if line.debiet(mode) is None or line.debiet(mode) > 0:
                        if do_reverse:
                            line.reverse()
                    line.forced_direction = True

        for line in self.graph.lines:
            if line.extra_data.get('weight') is None:
                # if line.id == 377326:
                #     a = 1
                weight_inflow = tree[line.inflow_node(mode).nr][1]
                weight_outflow = tree[line.outflow_node(mode).nr][1]
                if weight_inflow is not None and weight_outflow is not None:
                    if True and line.debiet(mode) is not None:
                        line.extra_data['weight'] = weight_outflow
                    elif weight_inflow < weight_outflow:
                        line.extra_data['weight'] = weight_outflow
                        if line.debiet(mode) is None or line.debiet(mode) > 0:
                            if do_reverse:
                                line.reverse()
                        line.forced_direction = True
                    else:
                        line.extra_data['weight'] = weight_inflow
                line.extra_data['tree_end'] = True

        # freeze all values made based on connection with
        tree = [[line_nr, weight, line_nr is not None, min_cat] for line_nr, weight, freeze, min_cat in tree]

        queue = []
        # last, add additional endnodes....
        for node, weight, min_cat in all_start_nodes:

            if tree[node.nr][0] is None:
                queue.append([node, 100000])

        while len(queue) > 0:
            node, tot_weight = queue.pop()
            # if node.id == 113:
            #     a = 1

            for line in node.outgoing():
                line: Line
                to_node = line.end_node()
                if tree[to_node.nr][2]:
                    # freezed, so skip
                    continue
                if line.debiet(mode) is None:
                    factor = 2
                elif line.debiet(mode) >= 0:
                    factor = 1
                else:
                    factor = 3

                if line.category == 2:
                    factor *= 1.5
                else:
                    factor *= 2

                weight = tot_weight + factor * line.length
                if weight < tree[to_node.nr][1]:
                    tree[to_node.nr][0] = line.nr
                    tree[to_node.nr][1] = weight
                    queue.append([to_node, weight])

            for line in node.incoming():
                line: Line
                to_node = line.start_node()
                if tree[to_node.nr][2]:
                    # freezed, so skip
                    continue
                if line.debiet(mode) is None:
                    factor = 2
                elif line.debiet(mode) <= 0:
                    factor = 1
                else:
                    factor = 3

                if line.category == 2:
                    factor *= 1.5
                else:
                    factor *= 2

                weight = tot_weight + factor * line.length
                if weight < tree[to_node.nr][1]:
                    tree[to_node.nr][0] = line.nr
                    tree[to_node.nr][1] = weight
                    queue.append([to_node, weight])

        for node_nr, [line_nr, weight, _, _] in enumerate(tree):
            if line_nr is not None:
                line = self.graph.line(line_nr)
                # if line.id == 1084919:
                #     a = 1

                line.extra_data['weight'] = weight
                if line.inflow_node(mode).nr != node_nr:
                    if line.debiet(mode) is None or line.debiet(mode) > 0:
                        if do_reverse:
                            line.reverse()
                    line.forced_direction = True

        for line in self.graph.lines:
            if line.extra_data.get('weight') is None:
                # if line.id == 377326:
                #     a = 1
                weight_inflow = tree[line.inflow_node(mode).nr][1]
                weight_outflow = tree[line.outflow_node(mode).nr][1]
                if weight_inflow is not None and weight_outflow is not None:
                    if True and line.debiet(mode) is not None:
                        line.extra_data['weight'] = weight_outflow
                    elif weight_inflow < weight_outflow:
                        line.extra_data['weight'] = weight_outflow
                        if line.debiet(mode) is None or line.debiet(mode) > 0:
                            if do_reverse:
                                line.reverse()
                        line.forced_direction = True
                    else:
                        line.extra_data['weight'] = weight_inflow
                line.extra_data['tree_end'] = True

        return tree

    def re_distribute_flow(self):

        node_done = [False for i in self.graph.nodes]

        # set initial vertex_queue on points with no upstream vertexes
        node_queue = OrderedDict([(node.id, node) for node in self.graph.get_endnodes(modus=Definitions.FORCED)])
        # remove duplicates

        last_node = list(node_queue.values())[-1]

        c_last_repeated = 0

        while len(node_queue) > 0:
            node = node_queue.popitem(last=False)[1]
            # if node == last_added_node:
            #     break

            # if 140870 in [l.id for l in node.outflow(modus=Definitions.FORCED)]:
            #     a = 1

            flow = node.flow(modus=Definitions.FORCED)
            flow_3di = node.flow(modus=Definitions.DEBIET_3DI)
            added_flow_on_point = flow_3di['outflow_debiet'] - flow_3di['inflow_debiet']
            if flow['outflow_nr'] == 0:
                # end, ready
                node_done[node.nr] = True
                if len(node_queue) > 0:
                    last_node = [*node_queue.values()][-1]
            else:
                all_inflows_not_known = len(
                    [l for l in node.inflow(modus=Definitions.FORCED) if l.debiet_modified is None])
                # if 140870 in [l.id for l in node.outflow(modus=Definitions.FORCED)] and all_inflows_not_known == 0:
                #     a = 1

                if all_inflows_not_known == 0:
                    # all inflows are known, so we can process this node
                    modified_flow_in = sum(
                        [abs(l.debiet_modified) for l in node.inflow(modus=Definitions.FORCED)]) + added_flow_on_point
                    modified_surplus_in = sum(
                        [l.surplus for l in node.inflow(modus=Definitions.FORCED) if l.surplus is not None])
                    # for forced points
                    modified_flow_out = sum(
                        [abs(l.debiet_modified) for l in node.outflow(modus=Definitions.FORCED) if
                         l.debiet_modified is not None])

                    modified_flow_in = modified_flow_in - modified_flow_out - modified_surplus_in

                    primary_out = [l for l in node.outflow(modus=Definitions.FORCED) if
                                   l.category == 1 and l.debiet_modified is None]
                    other_out = [l for l in node.outflow(modus=Definitions.FORCED) if
                                 l.category != 1 and l.debiet_modified is None]

                    min_flow_out = (len(primary_out) + len(other_out)) * min_flow
                    if modified_flow_in < min_flow_out:
                        surplus = abs(modified_flow_in - min_flow_out)
                        modified_flow_in = 0
                    else:
                        surplus = 0

                    if len(primary_out) == 1:
                        primary_out[0].set_debiet_modified(max(modified_flow_in, min_flow), surplus)
                        for line in other_out:
                            line.set_debiet_modified(min_flow, 0)
                    elif len(primary_out) == 0:
                        if len(other_out) == 1:
                            other_out[0].set_debiet_modified(max(modified_flow_in, min_flow), surplus)
                        else:
                            # multiple other.....
                            # filter out None values and zet minimum to min_flow (for example when direction is forced)
                            total_outflow = sum([max(min_flow, abs(l.debiet_3di if l.debiet_3di is not None else 0))
                                                 for l in other_out])
                            if total_outflow > 0:
                                factor = modified_flow_in / total_outflow
                            else:
                                factor = None
                            for line in other_out:
                                if factor is None:
                                    line.set_debiet_modified(min_flow, surplus / len(other_out))
                                else:
                                    flow = max(min_flow, abs(line.debiet_3di) if line.debiet_3di else min_flow)
                                    line.set_debiet_modified(flow * factor if flow != min_flow else min_flow,
                                                             surplus * factor)
                    else:
                        # multiple primary.....
                        # filter out None values and zet minimum to min_flow (for example when direction is forced)
                        total_primary_outflow = sum([max(min_flow, abs(l.debiet_3di if l.debiet_3di is not None else 0))
                                                     for l in primary_out])
                        if total_primary_outflow > 0:
                            factor = modified_flow_in / total_primary_outflow
                        else:
                            factor = None
                        for line in primary_out:
                            if factor is None:
                                line.set_debiet_modified(min_flow, surplus / len(primary_out))
                            else:
                                flow = max(min_flow, abs(line.debiet_3di) if line.debiet_3di else min_flow)
                                line.set_debiet_modified(flow * factor if flow != min_flow else min_flow,
                                                         factor * surplus)

                        for line in other_out:
                            line.set_debiet_modified(min_flow, 0)

                    node_done[node.nr] = True
                    for line in primary_out + other_out:
                        add_node = line.outflow_node(modus=Definitions.FORCED)
                        if not node_done[add_node.nr]:
                            node_queue[add_node.id] = add_node

                    if len(node_queue) > 0:
                        last_node = [*node_queue.values()][-1]
                        c_last_repeated = 0
                else:
                    # wait with this node. add it to the end of the stack.
                    node_queue[node.id] = node
                    # at this moment nothing has processed last loop
                    if node == last_node:
                        print(node.nr)
                        category = 3
                        all_category = 3
                        if c_last_repeated >= 4:
                            category = 1
                        if c_last_repeated >= 2:
                            category = 2
                        if c_last_repeated >= 8:
                            all_category = 2
                        if c_last_repeated >= 10:
                            all_category = 1

                        for node in [*node_queue.values()]:
                            # for circulars set tree end parts in current endnode list to minflow
                            for line in node.inflow(modus=Definitions.FORCED):
                                if line.extra_data.get('tree_end') and \
                                        line.debiet_modified is None and \
                                        line.category >= category:
                                    line.set_debiet_modified(min_flow)
                                    # line.debiet_3di if line.debiet_3di is not None else min_flow, 0)

                        if c_last_repeated in [6, 8, 10]:
                            # for larger circulars set all tree end parts to minflow and add these endpoints to the queue

                            for line in self.graph.lines:
                                if line.extra_data.get('tree_end') and \
                                        line.debiet_modified is None and \
                                        line.category >= all_category:
                                    line.set_debiet_modified(min_flow)
                                    # line.debiet_3di if line.debiet_3di is not None else min_flow, 0)
                                    add_node = line.outflow_node(modus=Definitions.FORCED)
                                    if not node_done[add_node.nr]:
                                        node_queue[add_node.id] = add_node
                            if len(node_queue) > 0:
                                last_node = [*node_queue.values()][-1]

                        c_last_repeated += 1
                        if c_last_repeated > 12:
                            print(
                                "loop over 'open' nodes, without fixing one. seems we are in an endless loop. Probably "
                                "there is a loop flow.")
                            return False, node_queue

        return True, []

    def hydrovak_class_tree_with_data(self):

        line_tree = {}
        tree = self.force_direction(mode=Definitions.DEBIET_DB, do_reverse=True, ignore_manual_startnodes=True,
                                    start_min_category=4)
        # create dicts with lines (arcs), required information and mark vertexes in bi-directional islands
        for line in self.graph.lines:
            ids = line.id
            exp = QgsExpression('"id" = {}'.format(ids))
            request = QgsFeatureRequest(exp)
            line_feature = None
            try:
                line_feature = next(self.full_line_layer.getFeatures(request))
            except StopIteration:
                msg = ('Fout: Er is geen object in de tabel hydroobjects_kenmerken met id {}. '
                       'Mogelijk is in de DAMO export in de tabel hydroobject de id leeg.').format(ids)
                logger.warning(msg)
                # todo: message to user.
                raise Exception(msg)

            inflow_node = line.inflow_node(Definitions.DEBIET_DB)
            outflow_node = line.outflow_node(Definitions.DEBIET_DB)

            line_tree[line.nr] = hydrovak_class(
                data_dict={
                    # basic information
                    'feat_id': line_feature.id(),  # feat_id,
                    'id': ids,
                    'line_nr': line.nr,
                    'in_node': inflow_node.nr,
                    'out_node': outflow_node.nr,
                    'weight': line.length,
                    'category': line.category,
                    'tree_end': line.extra_data.get('tree_end'),
                    # info need to be generated later
                    'downstream_line_nr': tree[outflow_node.nr][0],
                    'upstream_line_nrs': None,
                    'min_category_in_path': 4,
                    'modified_flow': None,
                    'cum_weight': 0,
                    'area_start_line_nr': None,
                },
                feature=line_feature,
            )

        return line_tree

    def build_tree(self):
        """
        function that analyses tree and creates tree structure of network.
        Sets self.arc_tree and self.start_arcs

        returns (tuple): tuple with dictionary of arc_tree en list of start arc_nrs
        """
        line_tree = self.hydrovak_class_tree_with_data()
        start_lines = {}
        in_between_lines = {}

        # for each arc, set downstream arc. When multiple, select one with highest flow.
        # also identify start arcs and inbetween arcs (areas are group of arcs with same targetlevel). Inbetween arcs
        # are arcs after a target_level change
        for line_nr, hline in line_tree.items():
            out_node = self.graph.node(hline['out_node'])
            # link line with highest flow
            downstream_line_old = next(iter(sorted(out_node.outflow(Definitions.DEBIET_DB),
                                                   key=lambda l: (
                                                                     abs(l.debiet_db) if l.debiet_db is not None else 0) + (
                                                                     0 if (l.target_level is None or hline[
                                                                         'target_level'] is None or hline[
                                                                               'target_level'] < l.target_level) else 1000),
                                                   reverse=True)), None)
            # hline['downstream_line_nr'] = downstream_line.nr if downstream_line else None
            downstream_line = self.graph.lines[hline['downstream_line_nr']] if hline[
                                                                                   'downstream_line_nr'] is not None else None

            if hline.get('id') in [110688, 110766]:
                a = 1

            if hline['downstream_line_nr'] is None or not downstream_line_old:
                a = 1

                start_lines[line_nr] = {
                    'line_nr': line_nr,
                    'point': out_node.point,
                    'children': [],
                    'parent': None,
                    # attributes to be filled later
                    'target_level': None,
                    'distance': None,
                    'cum_weight': None,
                    'min_category_in_path': None
                }
            elif (downstream_line.target_level is not None and
                  hline.get('target_level') is not None and
                  downstream_line.target_level != hline.get('target_level')):
                in_between_lines[line_nr] = {
                    'line_nr': line_nr,
                    'point': out_node.point,  # out_vertex.point()
                    'children': [],
                    'parent': None,
                    # attributes to be filled later
                    'target_level': None,
                    'distance': None,
                    'cum_weight': None,
                    'min_category_in_path': None
                }

        # for all lines, set upstream arcs. Set only the one, who has the current arc as downstream arc (so joining
        # streams are forced into a tree structure with no alternative paths to same point
        for line_nr, hline in line_tree.items():
            if hline.get('id') in [474252, 140888]:
                a = 1

            if hline.get('tree_end'):
                hline['upstream_line_nrs'] = []
            else:
                hline['upstream_line_nrs'] = [
                    line.nr for line in self.graph.node(hline['in_node']).inflow(Definitions.DEBIET_DB)]

        # order upstream arcs based on 'cum weight'. An (arbitrary) weight to select the long bigger flows as
        # main branch
        def get_cum_weight_min_category(hline):
            """sub-function for recursive weight calculation"""
            line_cum_weight = hline['weight']
            line_min_category = 4 if hline['category'] is None else hline['category']
            for upstream_line_nr in hline['upstream_line_nrs']:
                cum_weight, min_category = get_cum_weight_min_category(line_tree[upstream_line_nr])
                line_cum_weight += cum_weight
                line_min_category = min(line_min_category, min_category)
            hline['cum_weight'] = max(hline['cum_weight'], line_cum_weight)
            hline['min_category_in_path'] = min(hline['min_category_in_path'], line_min_category)
            return line_cum_weight, line_min_category

        # get cum_weight and sort upstream_arcs
        for start_sline in start_lines.values():
            start_sline['cum_weight'], start_sline['min_category_in_path'] = get_cum_weight_min_category(
                line_tree[start_sline['line_nr']])
            start_sline['target_level'] = line_tree[start_sline['line_nr']]['target_level']
            start_sline['weight'] = start_sline['cum_weight']
            # todo: set distance correct
            # start_arc['distance'] = start_arc['distance']

        # get cum_weight and sort upstream_arcs
        for start_bline in in_between_lines.values():
            start_bline['cum_weight'], start_bline['min_category_in_path'] = get_cum_weight_min_category(
                line_tree[start_bline['line_nr']])
            start_bline['target_level'] = line_tree[start_bline['line_nr']]['target_level']
            start_bline['weight'] = start_bline['cum_weight']
            # todo: set distance correct
            # start_arc['distance'] = start_arc['distance']

        for hline in line_tree.values():
            hline['upstream_line_nrs'].sort(key=lambda nr: line_tree[nr]['cum_weight'], reverse=True)

        # link arcs to start and inbetween arcs to get area structure
        def loop(start_line_nr, line_nr):
            hline = line_tree[line_nr]
            hline['area_start_line_nr'] = start_line_nr
            if line_nr in in_between_lines:
                start_line_nr = line_nr
            for upstream_line_nr in hline['upstream_line_nrs']:
                loop(start_line_nr, upstream_line_nr)

        for start_line_nr in start_lines.keys():
            loop(start_line_nr, start_line_nr)
        # for in_between_arc in start_arcs.keys():
        #     loop(in_between_arc, in_between_arc)

        # make start arc tree structure to link upstream areas to start arcs
        for inbetween_line_nr, in_between_item in in_between_lines.items():
            hline = line_tree[inbetween_line_nr]
            downstream_area_line_nr = line_tree[hline['downstream_line_nr']]['area_start_line_nr']
            if downstream_area_line_nr in start_lines:
                start_lines[downstream_area_line_nr]['children'].append(in_between_item)
            elif downstream_area_line_nr in in_between_lines:
                in_between_lines[downstream_area_line_nr]['children'].append(in_between_item)
            else:
                # this should not happen!
                pass

        # sort area start arcs and nested (inbetween) area arcs
        start_lines = sorted(start_lines.values(), key=lambda start_l: start_l['cum_weight'], reverse=True)

        def sort_line_list_on_weight(area_line):
            area_line['children'].sort(key=lambda arc_d: arc_d['cum_weight'], reverse=True)
            for arc_child in area_line['children']:
                sort_line_list_on_weight(arc_child)

        for start_line in start_lines:
            sort_line_list_on_weight(start_line)

        # store tree and start points
        self.arc_tree = line_tree
        self.start_arcs = start_lines

        return line_tree, start_lines

    def get_tree_data(self, root_node, category=4):
        # todo: move part outside of this class?
        # called when startpoint has been selected
        # get layers and make them empty
        if self.start_arc_tree is None:
            return

        if self.arc_tree is None:
            self.build_tree()

        ids = [feat.id() for feat in self._virtual_tree_layer.getFeatures()]
        self._virtual_tree_layer.dataProvider().deleteFeatures(ids)

        ids = [feat.id() for feat in self._endpoint_layer.getFeatures()]
        self._endpoint_layer.dataProvider().deleteFeatures(ids)

        features = []
        points = []

        def add_line(hydrovak):
            feat = QgsFeature()
            feat.setGeometry(hydrovak['feature'].geometry())

            feat.setAttributes([
                hydrovak['length'],
                int(hydrovak.get('feat_id')),
                int(hydrovak.get('hydro_id')),
                make_type(hydrovak.get('depth'), float),
                make_type(hydrovak.get('variant_min_depth'), float),
                make_type(hydrovak.get('variant_max_depth'), float),
                make_type(hydrovak.get('target_level'), float),
                make_type(hydrovak.get('category'), int),
            ])
            features.append(feat)
            return feat

        def add_point(feat_id, typ, node_nr):
            """create endpoint and add to addpoint layer"""
            p = self.graph.node(node_nr).point
            feat = QgsFeature()
            feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(p[0], p[1])))
            feat.setAttributes([
                int(feat_id),
                str(feat_id),
                typ,
                node_nr])
            points.append(feat)
            return feat

        def get_stat(branch_value, recursive_value, func=min):
            branch_value = make_type(branch_value, float, None)
            if recursive_value is None and branch_value is None:
                new_value = None
            else:
                new_value = func([val for val in [recursive_value, branch_value] if val is not None])
            return new_value

        def loop_upstream_arcs(hydrovak, parent_tree_item, tree_item, target_level, distance):
            hydrovak['end_arc_type'] = ''

            for i, upstream_line_nr in enumerate(hydrovak['upstream_line_nrs']):
                new_parent_tree_item = None
                upstream_hydrovak = self.arc_tree[upstream_line_nr]

                if upstream_hydrovak['target_level'] is not None and \
                        upstream_hydrovak['target_level'] != target_level:
                    # do something with keeping last target_level
                    endpoint_feature = add_point(hydrovak['feat_id'], 'target', hydrovak['in_node'])
                    # stop for this branch
                    hydrovak['end_arc_type'] = 'target'
                    continue
                elif upstream_hydrovak['min_category_in_path'] > category:
                    continue

                upstream_hydrovak.update({
                    'new_depth': get_stat(upstream_hydrovak['depth'], hydrovak['new_depth'], min),
                    'new_variant_min_depth': get_stat(upstream_hydrovak['variant_min_depth'],
                                                      hydrovak['new_variant_min_depth'], max),
                    'new_variant_max_depth': get_stat(upstream_hydrovak['variant_max_depth'],
                                                      hydrovak['new_variant_max_depth'], min),
                    'distance': distance + hydrovak['length']
                })

                feature = add_line(upstream_hydrovak)

                if i == 0:
                    new_tree_item = LeggerTreeItem(upstream_hydrovak, parent_tree_item)
                    if parent_tree_item is None:
                        a = 1

                    parent_tree_item.appendChild(new_tree_item)
                    new_parent_tree_item = parent_tree_item
                elif i == 1:
                    new_tree_item = LeggerTreeItem(upstream_hydrovak, tree_item)
                    tree_item.appendChild(new_tree_item)
                    new_parent_tree_item = tree_item
                else:
                    # first insert dummy split
                    split_hydrovak = hydrovak_class(
                        {'hydro_id': 'tak {0}'.format(i),
                         'tak': True,
                         # 'line_feature': upstream_hydrovak['feature'],  # todo: correct??
                         'distance': round(distance)
                         },
                        feature=feature)
                    split_item = LeggerTreeItem(split_hydrovak, parent_tree_item)
                    tree_item.insertChild(i - 2, split_item)
                    new_tree_item = LeggerTreeItem(upstream_hydrovak, split_item)
                    split_item.appendChild(new_tree_item)
                    new_parent_tree_item = split_item

                if len(upstream_hydrovak['upstream_line_nrs']) == 0:
                    endpoint_feature = add_point(upstream_hydrovak['feat_id'], 'end', upstream_hydrovak['in_node'])
                    upstream_hydrovak['end_arc_type'] = 'end'

                loop_upstream_arcs(
                    upstream_hydrovak,
                    new_parent_tree_item,
                    new_tree_item,
                    (upstream_hydrovak['target_level']
                     if upstream_hydrovak['target_level'] is not None else target_level),
                    distance + hydrovak['length']
                )

        hydrovak = self.arc_tree[self.start_arc_tree]

        hydrovak.update({
            'new_depth': hydrovak['depth'],
            'new_variant_min_depth': hydrovak['variant_min_depth'],
            'new_variant_max_depth': hydrovak['variant_max_depth'],
            'distance': 0
        })

        feature = add_line(hydrovak)

        new_tree_item = LeggerTreeItem(hydrovak, root_node)
        root_node.appendChild(new_tree_item)

        loop_upstream_arcs(
            hydrovak,
            root_node,
            new_tree_item,
            hydrovak['target_level'],
            0
        )

        self._virtual_tree_layer.dataProvider().addFeatures(features)
        self._virtual_tree_layer.commitChanges()
        self._virtual_tree_layer.updateExtents()
        self._virtual_tree_layer.triggerRepaint()

        self._endpoint_layer.dataProvider().addFeatures(points)
        self._endpoint_layer.commitChanges()
        self._endpoint_layer.updateExtents()
        self._endpoint_layer.triggerRepaint()

        return True

    def get_start_arc_tree(self):
        """
        get tree with startarcs.
        return: start_arc_tree, example of structure
            {
                'children': [
                   ...startpoints,
                   {
                    'children': []
                ]
            }
        """
        if self.arc_tree is None:
            self.build_tree()

        # todo: target level change arcs ...
        start_arc_tree = {
            'children': self.start_arcs
        }

        return start_arc_tree

    def set_tree_start_arc(self, id_start_arc):
        """
        set point (initial or next point) for expending path

        qgs_start_point (int): start point of tree for (extension) of path
        return: None
        """

        if id_start_arc == -1:
            return

        self.start_arc_tree = id_start_arc
