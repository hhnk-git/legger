import unittest

from legger.tests.utilities import get_qgis_app

QGIS_APP = get_qgis_app()

from PyQt5.QtCore import QVariant

from qgis.core import (
    QgsVectorLayer, QgsFeature, QgsPoint, QgsField, QgsGeometry)

from random import shuffle
from qgis.analysis import QgsVectorLayerDirector
from legger.utils.network import Network


# todo: rewrite for Network class (instead of new Network class))

# @unittest.skipIf()
class TestTheoreticalNetwork(unittest.TestCase):

    def setUp(self):

        self.one_simple_line_network = [
            {'id': 1, 'debiet': 1.5, 'direction': 1, 'categorieoppwaterlichaam': 1, 'geometry': [(0, 1), (0, 0)]},
            {'id': 2, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 2), (0, 1)]},
            {'id': 3, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': None, 'geometry': [(1, 1), (0, 1)]},
            {'id': 4, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 1, 'geometry': [(0, 3), (0, 2)]},
            {'id': 5, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 3, 'geometry': [(0, 4), (0, 3)]},
        ]

    def test_startnrs_one_network(self):
        """ test init network en get start nrs  with one simple network
        network:
              o
              |
              3
              |
              v
        o <-1- o <-2- o <-4- o <-5- o
        """
        line_layer, director, distance_properter = self.get_line_layer_and_director(self.one_simple_line_network)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()
        arc_nrs = sorted([arc_dict.get(arc['arc_nr']).feature.id() for arc in start_arcs])

        # self.assertTupleEqual((0, 0), (point.x(), point.y()))
        self.assertListEqual([1], arc_nrs)

    def test_startnrs_two_networks(self):
        """ test get start nrs with one simple network with 2 end points
        network:
              o
              |
              3
              |
              v
        o <-1- o <-2- o <-4- o -5-> o
        """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[4]['direction'] = 2

        line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()
        arc_nrs = sorted([arc_dict.get(arc['arc_nr']).feature.id() for arc in start_arcs])

        # self.assertEqual(2, len(vertex_nrs))
        self.assertListEqual([1, 5], arc_nrs)

    def test_startnrs_one_shared_startpoint(self):
        """ test get start nrs with one simple network with 2 end points
        network:
              o
              |
              v
        o --> o <-- o <-- o <-- o
        """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[0]['direction'] = 2

        line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()
        arc_nrs = sorted([arc_dict.get(arc['arc_nr']).feature.id() for arc in start_arcs])

        # self.assertEqual(1, len(vertex_nrs))
        # self.assertTupleEqual((0, 1), (point.x(), point.y()))
        self.assertListEqual([1, 2, 3], arc_nrs)

    def test_startnrs_splitted_stream(self):
        """ test get start nrs with one simple network with 2 end points
        network:
              o
              ^
              |
        o <-- o <-- o <-- o <-- o
        """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[2]['direction'] = 2

        line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()
        arc_nrs = sorted([arc_dict.get(arc['arc_nr']).feature.id() for arc in start_arcs])

        # self.assertEqual(2, len(vertex_nrs))
        self.assertListEqual([1, 3], arc_nrs)

    def test_startnrs_shuffled_input(self):
        """ test with network of test 'test_startnrs_one_shared_startpoint', with different
        input order (to make sure order of input don't influence results) """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[0]['direction'] = 2

        for i in range(0, 5):
            shuffle(layer_data)
            line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

            network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
            arc_dict, start_arcs = network.build_tree()
            arc_nrs = sorted([arc_dict.get(arc['arc_nr']).feature.id() for arc in start_arcs])

            # self.assertEqual(1, len(vertex_nrs))
            # self.assertTupleEqual((0, 1), (point.x(), point.y()))
            self.assertListEqual([1, 2, 3], arc_nrs)

    def test_build_tree_simple_network(self):
        """ test init network en get start nrs  with one simple network
        network:
              o
              |
              v
        o <-- o <-- o <-- o <-- o
        """
        line_layer, director, distance_properter = self.get_line_layer_and_director(self.one_simple_line_network)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()

        self.assertEqual(1, len(start_arcs))
        self.assertEqual(5.5, start_arcs[0]['cum_weight'])

        elem_two = [arc for arc in arc_dict.values() if arc['feat_id'] == 2][0]
        self.assertEqual(1, elem_two['min_category_in_path'])

    def test_build_tree_splitted_stream(self):
        """ test get start nrs with one simple network with 2 end points
        network:
              o
              ^
              |
        o <-- o <-- o <-- o <-- o
        """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[2]['direction'] = 2
        layer_data[2]['debiet'] = 0.5

        line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()

        self.assertEqual(2, len(start_arcs))
        self.assertEqual(1, arc_dict[start_arcs[0]['arc_nr']]['feat_id'])
        self.assertEqual(4.5, start_arcs[0]['cum_weight'])
        self.assertEqual(1, start_arcs[0]['min_category_in_path'])
        self.assertEqual(3, arc_dict[start_arcs[1]['arc_nr']]['feat_id'])
        self.assertEqual(0.5, start_arcs[1]['cum_weight'])
        self.assertEqual(4, start_arcs[1]['min_category_in_path'])

        elem_tree = [arc for arc in arc_dict.values() if arc['feat_id'] == 3][0]
        self.assertListEqual([], elem_tree['upstream_arcs'])

    def test_build_tree_small_bidirectional(self):
        """ test init network en get start nrs  with one simple network
        network:
              o
              |
              v
        o <-- o <-- o <-> o <-- o
        """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[3]['direction'] = 3
        layer_data[3]['debiet'] = None

        line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()

        self.assertEqual(1, len(start_arcs))
        self.assertEqual(4.5, start_arcs[0]['cum_weight'])

        elem_two = [arc for arc in arc_dict.values() if arc['feat_id'] == 2][0]
        self.assertEqual(1, elem_two['min_category_in_path'])

    def test_rerouting_of_flow(self):
        """

                  o <-1(6)- o <-----
                  |         ^       2(8)
                  1(7)      1(5)     \
                  v         |         \
        o <-1(9)- o <-2(1)- o <-2(2)- o <-2(3)- o <-2(4)- o

        :return:
        """

        one_simple_line_network_for_redistrubution = [
            {'id': 1, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 1), (0, 0)]},
            {'id': 2, 'debiet': 0.8, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 2), (0, 1)]},
            {'id': 3, 'debiet': 1.2, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 3), (0, 2)]},
            {'id': 4, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 4), (0, 3)]},
            {'id': 5, 'debiet': 0.4, 'direction': 2, 'categorieoppwaterlichaam': 1, 'geometry': [(1, 1), (0, 1)]},
            {'id': 6, 'debiet': 1.2, 'direction': 1, 'categorieoppwaterlichaam': 1, 'geometry': [(1, 1), (1, 0)]},
            {'id': 7, 'debiet': 1.4, 'direction': 1, 'categorieoppwaterlichaam': 1, 'geometry': [(1, 0), (0, 0)]},
            {'id': 8, 'debiet': 0.6, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 2), (1, 1)]},
            {'id': 9, 'debiet': 2.6, 'direction': 2, 'categorieoppwaterlichaam': 1, 'geometry': [(0, -1), (0, 0)]},
        ]

        line_layer, director, distance_properter = self.get_line_layer_and_director(
            one_simple_line_network_for_redistrubution)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        new_flows, arc_tree = network.re_distribute_flow()

        self.assertEqual(len(new_flows), len(one_simple_line_network_for_redistrubution))
        self.assertAlmostEqual(new_flows[0], 0, 5)
        self.assertEqual(new_flows[1], 0.8, 5)
        self.assertEqual(new_flows[2], 1.2, 5)
        self.assertEqual(new_flows[3], 1.0, 5)
        self.assertAlmostEqual(new_flows[4], -1.4, 5)
        self.assertAlmostEqual(new_flows[5], 2.2, 5)
        self.assertAlmostEqual(new_flows[6], 2.4, 5)
        self.assertEqual(new_flows[7], 0.6, 5)
        self.assertEqual(new_flows[8], -2.6, 5)

    def test_fill_bidirectional_gaps(self):
        """

                  o <-(6)- o <-----
                  |        ^      (8)
                 (7)      (5)       \
                  v        v         v
        o <-(9)-> o <-(1)- o <-(2)-> o <-(3)-> o <-(4)- o

        :return:
        """

        one_simple_line_network_for_redistrubution = [
            {'id': 1, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 1), (0, 0)]},
            {'id': 2, 'debiet': None, 'direction': 3, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 2), (0, 1)]},
            {'id': 3, 'debiet': 1.2, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 3), (0, 2)]},
            {'id': 4, 'debiet': 1.0, 'direction': 1, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 4), (0, 3)]},
            {'id': 5, 'debiet': None, 'direction': 3, 'categorieoppwaterlichaam': 1, 'geometry': [(1, 1), (0, 1)]},
            {'id': 6, 'debiet': 1.2, 'direction': 1, 'categorieoppwaterlichaam': 1, 'geometry': [(1, 1), (1, 0)]},
            {'id': 7, 'debiet': 1.4, 'direction': 1, 'categorieoppwaterlichaam': 1, 'geometry': [(1, 0), (0, 0)]},
            {'id': 8, 'debiet': None, 'direction': 3, 'categorieoppwaterlichaam': 2, 'geometry': [(0, 2), (1, 1)]},
            {'id': 9, 'debiet': None, 'direction': 3, 'categorieoppwaterlichaam': 1, 'geometry': [(0, -1), (0, 0)]},
        ]

        line_layer, director, distance_properter = self.get_line_layer_and_director(
            one_simple_line_network_for_redistrubution)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')

        # line_layer_updates = network.fill_bidirectional_gaps(bidirectional_islands)

    def get_line_layer_and_director(self, line_list):
        """ help function to setup line layer and director """

        line_layer = QgsVectorLayer(
            "linestring?crs={0}".format(28992),
            "netwerk",
            "memory")

        dp = line_layer.dataProvider()

        dp.addAttributes([
            QgsField("id", QVariant.LongLong),
            QgsField("debiet", QVariant.Double),
            QgsField("debiet_3di", QVariant.Double),
            QgsField("debiet_aangepast", QVariant.Double),
            QgsField("direction", QVariant.Int),
            QgsField("reversed", QVariant.Int),
            QgsField("categorieoppwaterlichaam", QVariant.Int),
            QgsField("length", QVariant.Double),
        ])
        line_layer.updateFields()
        features = []

        for l in line_list:
            feat = QgsFeature(dp.fields())
            feat['id'] = l['id']
            feat['debiet'] = l['debiet']
            feat['debiet_3di'] = l['debiet']
            feat['debiet_aangepast'] = l.get('debiet_aangepast')
            feat['direction'] = l['direction']
            feat['reversed'] = 1 if l['direction'] == 2 else 0
            feat['categorieoppwaterlichaam'] = l['categorieoppwaterlichaam']
            geom = QgsGeometry().fromPolyline([QgsPoint(150000 + p[0], 150000 + p[1]) for p in l['geometry']])
            feat.setGeometry(geom)
            features.append(feat)
            feat['length'] = geom.length()
            # line_layer.addFeature(feat)

        line_layer.dataProvider().addFeatures(features)
        line_layer.updateExtents()
        # setup director which is direction sensitive
        field_nr = line_layer.fields().indexFromName('direction')
        director = QgsVectorLayerDirector(
            line_layer, field_nr, '2', '1', '3', 3)

        distance_properter = AttributeProperter('length', 0)

        return line_layer, director, distance_properter
