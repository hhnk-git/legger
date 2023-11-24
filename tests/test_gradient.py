import unittest

from legger.tests.utilities import get_qgis_app
from utils.calc_gradient import calc_gradient_for_network

QGIS_APP = get_qgis_app()

from PyQt5.QtCore import QVariant

from qgis.core import (
    QgsVectorLayer, QgsFeature, QgsPoint, QgsField, QgsGeometry)

from qgis.analysis import QgsVectorLayerDirector
from legger.utils.network import Network


# todo: rewrite for Network class (instead of new Network class))

# @unittest.skipIf()
class TestGradientCalculationNetwork(unittest.TestCase):

    def setUp(self):
        self.one_simple_line_network = [
            {'id': 1, "streefpeil": 1, 'debiet': 1.5, 'verhang': 2, 'direction': 1, 'categorieoppwaterlichaam': 1,
             'geometry': [(0, 1000), (0, 0)]},
            {'id': 2, "streefpeil": 1, 'debiet': 1.0, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 2,
             'geometry': [(0, 2000), (0, 1000)]},
            {'id': 3, "streefpeil": 1, 'debiet': 1.0, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': None,
             'geometry': [(1000, 1000), (0, 1000)]},
            {'id': 4, "streefpeil": 1, 'debiet': 1.0, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 1,
             'geometry': [(0, 3000), (0, 2000)]},
            {'id': 5, "streefpeil": 1, 'debiet': 1.0, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 3,
             'geometry': [(0, 4000), (0, 3000)]},
        ]

    def test_simple_network(self):
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

        calc_gradient_for_network(network)

        mapping_hydro_id = {arc['hydro_id']: arc['tot_verhang'] for arc in network.arc_tree.values()}

        self.assertEqual(mapping_hydro_id[1], 0.02)
        self.assertEqual(mapping_hydro_id[3], 0.03)
        self.assertEqual(mapping_hydro_id[5], 0.05)

    def test_change_streefpeil(self):
        """ test init network en get start nrs  with one simple network
        network:
              o
              |
              3
              |
              v
        o <-1- o <-2- o ||<-4- o <-5- o
        """
        layer_data = [row.copy() for row in self.one_simple_line_network]
        layer_data[3]['streefpeil'] = 2
        layer_data[4]['streefpeil'] = 2

        line_layer, director, distance_properter = self.get_line_layer_and_director(layer_data)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')
        arc_dict, start_arcs = network.build_tree()

        calc_gradient_for_network(network)

        mapping_hydro_id = {arc['hydro_id']: arc['tot_verhang'] for arc in network.arc_tree.values()}

        self.assertEqual(mapping_hydro_id[1], 0.02)
        self.assertEqual(mapping_hydro_id[3], 0.03)
        self.assertEqual(mapping_hydro_id[4], 0.01)
        self.assertEqual(mapping_hydro_id[5], 0.02)

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
            {'id': 1, "streefpeil": 1, 'debiet': 1.0, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 2,
             'geometry': [(0, 1000), (0, 0)]},
            {'id': 2, "streefpeil": 1, 'debiet': 0.8, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 2,
             'geometry': [(0, 2000), (0, 1000)]},
            {'id': 3, "streefpeil": 1, 'debiet': 1.2, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 2,
             'geometry': [(0, 3000), (0, 2000)]},
            {'id': 4, "streefpeil": 1, 'debiet': 1.0, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 2,
             'geometry': [(0, 4000), (0, 3000)]},
            {'id': 5, "streefpeil": 1, 'debiet': 0.4, 'verhang': 1, 'direction': 2, 'categorieoppwaterlichaam': 1,
             'geometry': [(1000, 1000), (0, 1000)]},
            {'id': 6, "streefpeil": 1, 'debiet': 1.2, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 1,
             'geometry': [(1000, 1000), (1000, 0)]},
            {'id': 7, "streefpeil": 1, 'debiet': 1.4, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 1,
             'geometry': [(1000, 0), (0, 0)]},
            {'id': 8, "streefpeil": 1, 'debiet': 0.6, 'verhang': 1, 'direction': 1, 'categorieoppwaterlichaam': 2,
             'geometry': [(0, 2000), (1000, 1000)]},
            {'id': 9, "streefpeil": 1, 'debiet': 2.6, 'verhang': 1, 'direction': 2, 'categorieoppwaterlichaam': 1,
             'geometry': [(0, -1000), (0, 0)]},
        ]

        line_layer, director, distance_properter = self.get_line_layer_and_director(
            one_simple_line_network_for_redistrubution)

        network = Network(line_layer, line_layer, director, distance_properter, id_field='id')

        arc_dict, start_arcs = network.build_tree()
        calc_gradient_for_network(network)

        mapping_hydro_id = {arc['hydro_id']: arc['tot_verhang'] for arc in network.arc_tree.values()}

        self.assertEqual(mapping_hydro_id[2], 0.03)
        self.assertEqual(mapping_hydro_id[3], 0.04)
        self.assertAlmostEqual(mapping_hydro_id[8], 0.04414, 5)

    def get_line_layer_and_director(self, line_list):
        """ help function to setup line layer and director """

        line_layer = QgsVectorLayer(
            "linestring?crs={0}".format(28992),
            "netwerk",
            "memory")

        dp = line_layer.dataProvider()

        dp.addAttributes([
            QgsField("id", QVariant.LongLong),
            QgsField("streefpeil", QVariant.Double),
            QgsField("debiet", QVariant.Double),
            QgsField("debiet_3di", QVariant.Double),
            QgsField("debiet_aangepast", QVariant.Double),
            QgsField("direction", QVariant.Int),
            QgsField("verhang", QVariant.Double),
            QgsField("reversed", QVariant.Int),
            QgsField("categorieoppwaterlichaam", QVariant.Int),
            QgsField("length", QVariant.Double),
            QgsField("lengte", QVariant.Double),
            QgsField("tot_verhang", QVariant.Double),
        ])
        line_layer.updateFields()
        features = []

        for l in line_list:
            feat = QgsFeature(dp.fields())
            feat['id'] = l['id']
            feat["streefpeil"] = l['streefpeil']
            feat['debiet'] = l['debiet']
            feat['debiet_3di'] = l['debiet']
            feat['verhang'] = l['verhang']
            feat['debiet_aangepast'] = l.get('debiet_aangepast')
            feat['direction'] = l['direction']
            feat['reversed'] = 1 if l['direction'] == 2 else 0
            feat['categorieoppwaterlichaam'] = l['categorieoppwaterlichaam']
            geom = QgsGeometry().fromPolyline([QgsPoint(150000 + p[0], 150000 + p[1]) for p in l['geometry']])
            feat['lengte'] = geom.length()
            feat['length'] = geom.length()
            feat.setGeometry(geom)
            features.append(feat)

            # line_layer.addFeature(feat)

        line_layer.dataProvider().addFeatures(features)
        line_layer.updateExtents()
        # setup director which is direction sensitive
        field_nr = line_layer.fields().indexFromName('direction')
        director = QgsVectorLayerDirector(
            line_layer, field_nr, '2', '1', '3', 3)

        distance_properter = AttributeProperter('length', 0)

        return line_layer, director, distance_properter
