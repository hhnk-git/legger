import logging
import os
from collections import OrderedDict

from PyQt4.QtCore import QMetaObject, QSize, Qt, pyqtSignal
from PyQt4.QtGui import (QApplication, QComboBox, QDockWidget, QGroupBox, QHBoxLayout, QLabel, QPlainTextEdit,
                         QPushButton, QSizePolicy, QSpacerItem, QTabWidget, QVBoxLayout, QWidget)
from legger.qt_models.area_tree import AreaTreeItem, AreaTreeModel, area_class
from legger.qt_models.legger_tree import LeggerTreeItem, LeggerTreeModel
from legger.qt_models.profile import ProfileModel
from legger.sql_models.legger import (BegroeiingsVariant, GeselecteerdeProfielen, HydroObject, Kenmerken,
                                      ProfielFiguren, Varianten)
from legger.sql_models.legger_database import LeggerDatabase
from legger.utils.formats import try_round
from legger.utils.legger_map_manager import LeggerMapManager
from legger.utils.network_utils import LeggerMapVisualisation
from legger.utils.new_network import NewNetwork
from legger.utils.user_message import messagebar_message
from legger.views.input_widget import NewWindow
from network_graph_widgets import LeggerPlotWidget, LeggerSideViewPlotWidget
from qgis.core import QgsFeature, QgsGeometry, QgsMapLayerRegistry
from qgis.networkanalysis import QgsLineVectorLayerDirector
from sqlalchemy import and_, or_
import inspect

from .network_table_widgets import LeggerTreeWidget, StartpointTreeWidget, VariantenTable

log = logging.getLogger('legger.' + __name__)

precision = 0.000001

try:
    _encoding = QApplication.UnicodeUTF8


    def _translate(context, text, disambig):
        return QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QApplication.translate(context, text, disambig)

SHOW_ALL = 'alle'
PRE_SELECTED = 'opgegeven'

STRATEGY_THIS = 'dit hydrovak'
STRATEGY_DONWSTREAM_ALL = 'benedenstr. altijd'
STRATEGY_DONWSTREAM_LESS = 'benedenstr. of meer'

performance_log = []


def perf_log(linenr, step, obj=None):
    global performance_log
    now = datetime.datetime.now()
    if len(performance_log) > 0:
        last = performance_log[-1][3]
        performance_log.append((linenr, step, obj, now, now - last))
    else:
        performance_log.append((linenr, step, obj, now, None))


def log_performance():
    global performance_log
    # write as csv
    with open(os.path.join(os.path.dirname(__file__), os.pardir, 'performance_log.csv'), 'w') as f:
        for row in performance_log:
            f.write(f'{row[0]};{row[1]};{row[2]};{row[3]};{row[4]}\n')
    performance_log = []


def interpolated_color(value, color_map, alpha=255):
    for i, cm in enumerate(color_map):
        if value <= cm[0]:
            if i == 0:
                return list(cm[1]) + [alpha]
            else:
                prev = color_map[i - 1]
                fraction = (value - prev[0]) / (cm[0] - prev[0])
                return [p * (1 - fraction) + n * fraction for p, n in zip(prev[1], cm[1])] + [alpha]
    return list(color_map[-1][1]) + [alpha]


class LeggerWidget(QDockWidget):
    """Legger Network widget with tree tables, cross section and sideview and
    legger profile selection"""
    # todo:
    #   - category filter on map and tree instead of shortcut
    #   - improve performance 'initial loop tree'

    closingWidget = pyqtSignal()

    def __init__(self, parent=None, iface=None, path_legger_db=None):
        """Constructor."""
        super(LeggerWidget, self).__init__(parent)

        # store arguments
        self.iface = iface
        self.path_legger_db = path_legger_db

        # init parameters
        self.measured_model = ProfileModel()
        self.variant_model = ProfileModel()
        self.legger_model = LeggerTreeModel()
        self.area_model = AreaTreeModel()

        if not path_legger_db:
            messagebar_message("Database selectie", "Selecteer eerst een legger database", level=1)
            raise Exception("Selecteer eerst een legger database")

        # create session (before setup_ui)
        db = LeggerDatabase(
            {'db_path': path_legger_db},
            'spatialite'
        )
        db.create_and_check_fields()
        self.session = db.get_session()
        # todo: request something to test connection and through error message otherwise
        hydro_object_count = self.session.query(HydroObject).count()

        if hydro_object_count == 0:
            messagebar_message("Database selectie", "Database bevat geen hydrovakken", level=1)
            raise Exception("Database bevat geen hydrovakken")

        # initial values
        self.selected_hydrovak = None
        self.active_begroeiings_variant = SHOW_ALL
        self.active_begroeiings_variant_strategy = None

        # setup ui
        self.setup_ui(self)

        self.legger_model.setTreeWidget(self.legger_tree_widget)
        self.area_model.setTreeWidget(self.startpoint_tree_widget)

        self.category_combo.insertItems(0, ['4', '3', '2', '1'])
        self.category_combo.setCurrentIndex(0)
        self.category_filter = 4

        self.begroeiings_varianten = OrderedDict(
            [(SHOW_ALL, 'all'), (PRE_SELECTED, 'pre_selected'), ] +
            [(v.naam, v) for v in self.session.query(BegroeiingsVariant)]
        )

        self.begroeiings_combo.insertItems(
            0, self.begroeiings_varianten.keys())

        self.begroeiings_variant_strategies = OrderedDict((
            ('alleen dit hydrovak', 'only_this_hydrovak'),
            ('alle bovenstroomse hydrovakken', 'all_upstream'),
        ))

        self.begroeiings_strategy_combo.insertItems(0, self.begroeiings_variant_strategies.keys())
        self.begroeiings_strategy_combo.setCurrentIndex(0)

        self.child_selection_strategies = OrderedDict((
            ('gekozen traject tot waarde', 'selected_branch_till_value'),
            ('gekozen traject tot eind', 'selected_branch_till_end'),
            ('alleen dit hydrovak', 'selected_hydrovak'),
            ('bovenstrooms (met zijtakken) tot waarde ', 'upstream_till_value'),
            ('bovenstrooms (met zijtakken) tot eind', 'upstream_till_end'),
        ))

        self.child_selection_strategy_combo.insertItems(0, self.child_selection_strategies.keys())
        self.child_selection_strategy_combo.setCurrentIndex(0)

        # create line layer and add to map
        self.layer_manager = LeggerMapManager(self.iface, self.path_legger_db)

        self.line_layer = self.layer_manager.get_line_layer(add_to_map=True)
        self.vl_tree_layer = self.layer_manager.get_virtual_tree_layer(add_to_map=True)
        self.vl_endpoint_layer = self.layer_manager.get_endpoint_layer(add_to_map=True)
        self.vl_track_layer = self.layer_manager.get_track_layer(add_to_map=True)
        self.vl_hover_layer = self.layer_manager.get_hover_layer(add_to_map=True)
        self.vl_selected_layer = self.layer_manager.get_selected_layer(add_to_map=True)
        self.vl_startpoint_hover_layer = self.layer_manager.get_hover_startpoint_layer(add_to_map=True)

        self.map_visualisation = LeggerMapVisualisation(
            self.iface, self.line_layer.crs())

        # init network
        line_direct = self.layer_manager.get_line_layer(geometry_col='line')
        field_nr = line_direct.fieldNameIndex('direction')
        director = QgsLineVectorLayerDirector(
            line_direct, field_nr, '2', '1', '3', 3)

        self.network = NewNetwork(
            line_direct, self.line_layer, director, None, self.vl_tree_layer, self.vl_endpoint_layer
        )

        # add listeners
        self.category_combo.currentIndexChanged.connect(self.category_change)
        self.variant_model.dataChanged.connect(self.data_changed_variant)
        self.legger_model.dataChanged.connect(self.data_changed_legger_tree)
        self.area_model.dataChanged.connect(self.data_changed_area_model)
        self.show_manual_input_button.clicked.connect(
            self.show_manual_input_window)
        self.next_endpoint_button.clicked.connect(
            self.set_next_endpoint)
        self.begroeiings_combo.currentIndexChanged.connect(self.onSelectBegroeiingsVariant)

        # self.begroeiings_strategy_combo.currentIndexChanged.connect(self.onSelectBegroeiingsVariantStrategy)

        # create and init startpoint (AreaTree) model
        def loop_over(parent, data):
            for child in data['children']:
                area = area_class(child)
                item = AreaTreeItem(area, parent)
                parent.appendChild(item)
                loop_over(item, child)

        # get startingpoints and select first
        sp_tree = self.network.get_start_arc_tree()

        root = AreaTreeItem(None, None)
        loop_over(root, sp_tree)
        self.area_model.setNewTree(root.childs)

        # initial, select first area
        first_area = root.child(0)
        self.area_model.setDataItemKey(first_area, 'selected', True)

    def category_change(self, nr):
        """
        filters the tree and re-initialize legger tree
        nr: nr of selected option (hydrovak category) of combobox
        return: -
        """
        self.category_filter = int(self.category_combo.currentText())
        root = LeggerTreeItem(None, None)
        self.network.get_tree_data(root, self.category_filter)
        self.legger_model.setNewTree(root.childs)
        self.legger_model.set_column_sizes_on_view(self.legger_tree_widget)
        if len(root.childs) > 0:
            self.loop_tree(root.childs[0], initial=True)

    def show_manual_input_window(self):
        self._new_window = NewWindow(
            self.legger_model.selected,
            self.session,
            callback_on_save=self.update_available_profiles)
        self._new_window.show()

    def set_next_endpoint(self):
        """
        select the next endpoint in a traject with no selected variant
        called by the next_endpoint_button

        returns: -
        """
        sp = self.legger_model.sp
        if sp is None:
            messagebar_message('Fout',
                               'Selecteer eerst een startpunt (kolom sp)',
                               level=1,  # Warning
                               duration=15)
            return

        missing_values, endpoint = self.legger_model.find_endpoint_traject_without_legger_profile(sp)
        if not missing_values or endpoint is None:
            messagebar_message('Eindpunt selectie',
                               'Geen traject gevonden met ontbrekende legger',
                               duration=15)
        else:
            self.legger_model.open_till_endpoint(endpoint, close_other=True)
            self.legger_model.setDataItemKey(endpoint, 'ep', True)

    def loop_tree(self,
                  node,
                  depth=None,
                  initial=False,
                  hover=False,
                  begroeiingsvariant=None,
                  variant_id=None,
                  child_strategy='selected_branch_till_value',
                  begroeiings_strategy='pre_selected',
                  traject_nodes=None):
        """
        recursive loop over younger items where depth can be applied according to
        available profiles

        initial (bool): initiele loop om aantal berekende velden te bepalen
        child_strategy (str):
                options:
                   - 'selected_branch_till_value',
                   - 'selected_branch_till_end',
                   - 'selected_hydrovak',
                   - 'upstream_till_value',
                   - 'upstream_till_end'
        begroeiings_strategy (str):
                options:
                    - 'only_this_hydrovak'
                    - 'all_upstream'
                    - 'minimum' --> not implemented yet
                    - 'maximum' --> not implemented yet
        :return:
        """
        output_hydrovakken = [node]
        if initial:
            variant_id = node.hydrovak.get('selected_variant_id')
            depth = node.hydrovak.get('selected_depth')
        elif (variant_id is None and child_strategy in ['selected_branch_till_value', 'upstream_till_value'] and
              node.hydrovak.get('selected_variant_id')):
            # stop here, already value there
            return

        if initial and variant_id is None:
            pass
        elif node.hydrovak['variant_min_depth'] is None:
            # no variants available, so skip this one and continue downstream
            pass
        else:
            # get selected variant. if variant_id is None, try based on depth and begroeiingsvariant
            if variant_id is not None:
                profile_variant = self.session.query(Varianten).filter(Varianten.id == variant_id)
                if begroeiingsvariant is None or begroeiingsvariant == 'all' and profile_variant.count():
                    begroeiingsvariant = profile_variant[0].begroeiingsvariant_id
            else:
                # use given begroeiingsvariant if stategy is all_upstream otherwise use begroeiingsgraad
                # set on hydrovak or the default begroeiingsgraad
                # (correct begroeiingsvariant of first hydrovak is selected by setting variant_id)
                #  "type(begroeiingsvariant) != str" is to filter out setting 'all'
                if begroeiings_strategy == 'all_upstream' and begroeiingsvariant is not None and type(
                        begroeiingsvariant) != str:
                    profile_variant = self.session.query(Varianten).filter(
                        Varianten.hydro_id == node.hydrovak.get('hydro_id'),
                        Varianten.begroeiingsvariant_id == begroeiingsvariant,
                        Varianten.diepte < depth + precision,
                        Varianten.diepte > depth - precision
                    )
                else:
                    profile_variant = self.session.query(Varianten).filter(
                        Varianten.hydro_id == node.hydrovak.get('hydro_id'),
                        or_(Varianten.hydro.has(
                            HydroObject.begroeiingsvariant_id == Varianten.begroeiingsvariant_id),
                            and_(Varianten.hydro.has(HydroObject.begroeiingsvariant_id == None),
                                 Varianten.begroeiingsvariant.has(is_default=True))),
                        Varianten.diepte < depth + precision,
                        Varianten.diepte > depth - precision
                    )

            if profile_variant.count() > 0:

                if hover:
                    # self.legger_model.setDataItemKey(node, 'sel d', depth)
                    self.legger_model.setDataItemKey(node, 'selected_depth_tmp', depth)
                else:
                    # get all info to display in legger table
                    over_depth = node.hydrovak.get('depth') - depth if node.hydrovak.get('depth') is not None else None
                    profilev = profile_variant.first()
                    width = profilev.waterbreedte
                    over_width = node.hydrovak.get('width') - width \
                        if node.hydrovak.get('width') is not None else None

                    figuren = profilev.figuren
                    score = None
                    if len(figuren) > 0:
                        figuur = figuren[0]
                        over_width = "{0:.2f}".format(figuur.t_overbreedte_l + figuur.t_overbreedte_r) \
                            if figuur.t_overbreedte_l is not None else over_width
                        score = "{0:.2f}".format(figuur.t_fit)
                        over_depth = "{0:.2f}".format(
                            figuur.t_overdiepte) if figuur.t_overdiepte is not None else over_depth
                    else:
                        over_depth = "{}*".format(try_round(over_depth, 2, '-'))
                        over_width = "{}*".format(try_round(over_width, 2, '-'))

                    verhang = try_round(profilev.verhang, 1, '-')
                    self.legger_model.setDataItemKey(node, 'selected_depth', depth)
                    self.legger_model.setDataItemKey(node, 'selected_width', width)
                    self.legger_model.setDataItemKey(node, 'selected_variant_id', profilev.id)
                    self.legger_model.setDataItemKey(node, 'selected_begroeiingsvariant_id',
                                                     profilev.begroeiingsvariant_id)
                    self.legger_model.setDataItemKey(node, 'verhang', verhang)
                    self.legger_model.setDataItemKey(node, 'score', score)
                    self.legger_model.setDataItemKey(node, 'over_depth', over_depth)
                    self.legger_model.setDataItemKey(node, 'over_width', over_width)

                    if not initial:
                        # save selected variant
                        selected = self.session.query(GeselecteerdeProfielen).filter(
                            GeselecteerdeProfielen.hydro_id == node.hydrovak.get('hydro_id')).first()
                        if selected:
                            selected.variant = profilev
                        else:
                            selected = GeselecteerdeProfielen(
                                hydro_id=node.hydrovak.get('hydro_id'),
                                variant_id=profilev.id
                            )
                        self.session.add(selected)
            elif not initial:
                # no variant which fits criteria. stop iteration here
                return

        if begroeiings_strategy == 'only_this_hydrovak':
            begroeiingsvariant = None
        elif begroeiings_strategy == 'all_upstream':
            # keep variant as it is
            pass
        # elif begroeiings_strategy == 'minimum':
        #     pass
        # elif begroeiings_strategy == 'maximum':
        #     pass

        if child_strategy == 'selected_hydrovak':
            loop_childs = []
        elif child_strategy in ['upstream_till_value', 'upstream_till_end'] or initial:
            loop_childs = node.younger()
        else:  # 'selected_branch_till_value', 'selected_branch_till_end'
            if traject_nodes is None or len(traject_nodes) == 0:
                loop_childs = []
            else:
                child = traject_nodes.pop(0)
                loop_childs = [child]

        for young in loop_childs:
            hydrovakken = self.loop_tree(
                young,
                depth=depth,
                initial=initial,
                hover=hover,
                begroeiingsvariant=begroeiingsvariant,
                child_strategy=child_strategy,
                begroeiings_strategy=begroeiings_strategy,
                traject_nodes=traject_nodes,
            )
            if hydrovakken is not None:
                output_hydrovakken += hydrovakken

        return output_hydrovakken

    def data_changed_legger_tree(self, index, to_index):
        """
        changes during selection and hover of hydrovak / legger tree
        index (QIndex): index of changed field
        """

        # activate draw
        node = self.legger_model.data(index, role=Qt.UserRole)

        if self.legger_model.columns[index.column()].get('field') == 'hover':
            ids = [feat.id() for feat in self.vl_hover_layer.getFeatures()]
            self.vl_hover_layer.dataProvider().deleteFeatures(ids)

            if node.hydrovak.get('hover'):
                features = []

                feat = QgsFeature()
                feat.setGeometry(node.hydrovak.get('feature').geometry())

                try:
                    feat.setAttributes([
                        node.hydrovak.get('feature')['id']])

                    features.append(feat)
                    self.vl_hover_layer.dataProvider().addFeatures(features)
                except KeyError:
                    pass

            self.vl_hover_layer.commitChanges()
            self.vl_hover_layer.updateExtents()
            self.vl_hover_layer.triggerRepaint()

        elif self.legger_model.columns[index.column()].get('field') == 'selected':
            if self.legger_model.data(index, role=Qt.CheckStateRole) == Qt.Unchecked:
                self.save_remarks()
                self.selected_variant_remark.setPlainText('')
                self.selected_variant_remark.setDisabled(True)

                ids = [feat.id() for feat in self.vl_selected_layer.getFeatures()]
                self.vl_selected_layer.dataProvider().deleteFeatures(ids)

            if node.hydrovak.get('selected'):
                if node.hydrovak.get('tak'):
                    self.legger_model.setDataItemKey(node.younger()[1], 'selected', Qt.Checked)
                else:
                    features = []

                    feat = QgsFeature()
                    feat.setGeometry(node.hydrovak.get('feature').geometry())

                    feat.setAttributes([
                        node.hydrovak.get('feature')['id']])

                    features.append(feat)
                    self.vl_selected_layer.dataProvider().addFeatures(features)

            self.vl_selected_layer.commitChanges()
            self.vl_selected_layer.updateExtents()
            self.vl_selected_layer.triggerRepaint()

            if node.hydrovak.get('tak'):
                pass
            elif node.hydrovak.get('selected'):
                self.on_select_edit_hydrovak(self.legger_model.data(index, role=Qt.UserRole))
                self.show_manual_input_button.setDisabled(False)

            elif (self.legger_model.selected is None or
                  self.legger_model.data(index, role=Qt.UserRole) == self.legger_model.selected):
                self.variant_model.removeRows(0, len(self.variant_model.rows))
                self.show_manual_input_button.setDisabled(True)

        elif self.legger_model.columns[index.column()].get('field') in ['ep', 'sp']:
            # clear current track
            if self.legger_model.sp is None or self.legger_model.ep is None:
                ids = [feat.id() for feat in self.vl_track_layer.getFeatures()]
                self.vl_track_layer.dataProvider().deleteFeatures(ids)
                self.vl_track_layer.commitChanges()
                self.vl_track_layer.triggerRepaint()
            elif node.hydrovak.get('tak'):
                if self.legger_model.columns[index.column()].get('field') == 'ep':
                    self.legger_model.setDataItemKey(node.younger()[1], 'ep', Qt.Checked)
                if self.legger_model.columns[index.column()].get('field') == 'sp':
                    self.legger_model.setDataItemKey(node.younger()[1], 'sp', Qt.Checked)
            elif self.legger_model.sp and self.legger_model.ep:
                features = []

                def loop_rec(node):
                    if node.hydrovak.get('tak'):
                        node = node.older()
                    else:
                        feat = QgsFeature()
                        feat.setGeometry(node.hydrovak.get('feature').geometry())

                        feat.setAttributes([
                            node.hydrovak.get('feature')['id']])

                        features.append(feat)

                    if node != self.legger_model.sp:
                        loop_rec(node.older())

                loop_rec(self.legger_model.ep)

                self.vl_track_layer.dataProvider().addFeatures(features)
                self.vl_track_layer.commitChanges()
                self.vl_track_layer.updateExtents()
                self.vl_track_layer.triggerRepaint()

            if self.legger_model.sp is not None:
                self.next_endpoint_button.setDisabled(False)
            else:
                self.next_endpoint_button.setDisabled(True)

    def data_changed_area_model(self, index, to_index):
        """
        changes during selection and hover of area (start point) table

        index (QIndex): index of changed field
        """

        self.performance_log = []

        if self.area_model.columns[index.column()].get('field') == 'selected':
            # clear display elements

            if self.area_model.data(index, role=Qt.CheckStateRole) == Qt.Checked:
                self.variant_model.removeRows(0, len(self.variant_model.rows))
                self.legger_model.set_column_value('hover', False)
                self.legger_model.set_column_value('selected', False)
                self.legger_model.set_column_value('ep', False)
                self.legger_model.set_column_value('sp', False)

                area_item = self.area_model.data(index, role=Qt.UserRole)

                self.network.set_tree_start_arc(area_item.area.get('arc_nr'))

                self.legger_model.clear()

                root = LeggerTreeItem(None, None)
                self.network.get_tree_data(root, self.category_filter)
                self.legger_model.setNewTree(root.childs)
                self.legger_model.set_column_sizes_on_view(self.legger_tree_widget)
                if len(root.childs) > 0:
                    self.loop_tree(root.childs[0], initial=True)

                canvas = self.iface.mapCanvas()
                extent = self.vl_tree_layer.extent()
                if extent:
                    extent.scale(1.2)
                    canvas.setExtent(extent)
        elif self.area_model.columns[index.column()].get('field') == 'hover':
            ids = [feat.id() for feat in self.vl_startpoint_hover_layer.getFeatures()]
            self.vl_startpoint_hover_layer.dataProvider().deleteFeatures(ids)

            value = self.area_model.data(index, role=Qt.DisplayRole)

            if self.area_model.data(index, role=Qt.CheckStateRole) == Qt.Checked:
                features = []

                node = self.area_model.data(index, role=Qt.UserRole)
                feat = QgsFeature()

                feat.setGeometry(QgsGeometry.fromPoint(node.area.get('point')))
                feat.setAttributes([
                    node.area.get('vertex_id')])
                features.append(feat)

                self.vl_startpoint_hover_layer.dataProvider().addFeatures(features)

            self.vl_startpoint_hover_layer.commitChanges()
            self.vl_startpoint_hover_layer.updateExtents()
            self.vl_startpoint_hover_layer.triggerRepaint()

    def data_changed_variant(self, index):
        """
        changes during selection and hover of variant table

        index (QIndex): index of changed field
        """
        item = self.variant_model.rows[index.row()]
        if self.variant_model.columns[index.column()].name == 'active':
            if item.active.value:
                # only one selected at the time
                item.color.value = list(item.color.value)[:3] + [255]
                for row in self.variant_model.rows:
                    if row.active.value and row != item:
                        row.active.value = False

                depth = item.depth.value
                selected_variant_id = item.name.value
                traject = []

                if self.legger_model.ep:
                    traject = self.legger_model.ep.up(self.legger_model.selected)
                    traject.reverse()
                    if len(traject) > 0:
                        traject.pop(0)
                else:
                    messagebar_message(
                        'Traject nodig',
                        'Selecteer eerst een traject (sp en ep) voordat diepte kan worden doorgetrokken.',
                        1,
                        15)
                    return

                self.loop_tree(
                    self.legger_model.selected,
                    depth=depth,
                    initial=False,
                    variant_id=selected_variant_id,
                    begroeiingsvariant=self.get_begroeiings_variant(),
                    child_strategy=self.get_child_selection_strategy(),
                    begroeiings_strategy=self.get_begroeiings_strategy(),
                    traject_nodes=traject
                )
                self.session.commit()
                # trigger repaint of sideview
                self.sideview_widget.draw_selected_lines(self.sideview_widget._get_data())
            else:
                item.color.value = list(item.color.value)[:3] + [20]
                # trigger repaint of sideview
                self.sideview_widget.draw_selected_lines(self.sideview_widget._get_data())

        elif self.variant_model.columns[index.column()].name == 'hover':
            if item.hover.value:
                # only one selected at the time
                item.color.value = list(item.color.value)[:3] + [255]
                for row in self.variant_model.rows:
                    if row.hover.value and row != item:
                        row.hover.value = False

                depth = item.depth.value
                selected_variant_id = item.name.value
                traject = []
                self.legger_model.set_column_value('selected_depth_tmp', None)

                if self.legger_model.ep:
                    traject = self.legger_model.ep.up(self.legger_model.selected)
                    traject.reverse()
                    if len(traject) > 0:
                        traject.pop(0)

                else:
                    messagebar_message(
                        'Traject nodig',
                        'Selecteer eerst een traject (sp en ep) voordat diepte kan worden doorgetrokken.',
                        1,
                        15)
                    return

                hydrovakken = self.loop_tree(
                    self.legger_model.selected,
                    depth=depth,
                    initial=False,
                    hover=True,
                    variant_id=selected_variant_id,
                    begroeiingsvariant=self.get_begroeiings_variant(),
                    child_strategy=self.get_child_selection_strategy(),
                    begroeiings_strategy=self.get_begroeiings_strategy(),
                    traject_nodes=traject
                )

                # set map visualisation of selected hydrovakken
                self.network._virtual_tree_layer.setSubsetString(
                    '"hydro_id" in (\'{ids}\')'.format(
                        ids='\',\''.join([str(hydrovak.hydrovak['hydro_id']) for hydrovak in hydrovakken])))
                # trigger repaint of sideview
                self.sideview_widget.draw_selected_lines(self.sideview_widget._get_data())
            else:
                self.legger_model.set_column_value('selected_depth_tmp', None)
                # reset map visualisation
                self.network._virtual_tree_layer.setSubsetString('')
                # trigger repaint of sideview
                self.sideview_widget.draw_selected_lines(self.sideview_widget._get_data())

    def on_select_edit_hydrovak(self, item):
        """
        set elements after selection of a hydrovak for profile selection

        item (LeggerTreeItem): selected hydrovak LeggerTreeItem
        return: -
        """

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno, 'on_select_edit_hydrovak - start')

        hydro_object = self.session.query(HydroObject).filter_by(id=item.hydrovak.get('hydro_id')).first()
        if hydro_object is None:
            self.selected_variant_remark.setPlainText('')
            self.selected_variant_remark.setDisabled(True)
            return None

        self.selected_hydrovak = item

        self.selected_hydrovak = item
        self.selected_hydrovak_db = hydro_object

        self.selected_variant_remark.setDisabled(False)
<<<<<<< Updated upstream
        self.selected_variant_remark.setPlainText(item.hydrovak.get('selected_remarks'))
=======
        self.kijk_variant_knop.setDisabled(False)
        self.selected_variant_remark.setPlainText(item.hydrovak.get('opmerkingen'))

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'on_select_edit_hydrovak - till update_available_variants')

>>>>>>> Stashed changes
        self.update_available_variants()

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'on_select_edit_hydrovak - finished update available variants')

        log_performance()

    def save_remarks(self):
        if self.selected_hydrovak:
            if self.selected_hydrovak_db:
                self.selected_hydrovak_db.opmerkingen = self.selected_variant_remark.toPlainText()
                self.session.add(self.selected_hydrovak_db)
                self.session.commit()

                self.legger_model.setDataItemKey(
                    self.selected_hydrovak,
                    'selected_remarks',
                    self.selected_variant_remark.toPlainText())

    def update_available_variants(self):

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'update_available_variants - start')

        item = self.selected_hydrovak
        hydro_object = self.selected_hydrovak_db
        self.variant_model.removeRows(0, len(self.variant_model.rows))

        selected_variant_id = item.hydrovak.get('selected_variant_id')

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'update_available_variants - start query variants')

        var = self.session.query(Varianten) \
            .join(BegroeiingsVariant) \
            .outerjoin(ProfielFiguren) \
            .filter(Varianten.hydro == hydro_object) \
            .order_by(Varianten.diepte)

        if self.active_begroeiings_variant == SHOW_ALL:
            pass
        elif self.active_begroeiings_variant == PRE_SELECTED:
            var = var.filter(or_(Varianten.begroeiingsvariant == hydro_object.begroeiingsvariant,
                                 Varianten.id == selected_variant_id))
        elif self.active_begroeiings_variant is not None:
            var = var.filter(or_(BegroeiingsVariant.naam == self.active_begroeiings_variant,
                                 Varianten.id == selected_variant_id))

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'update_available_variants - finished definition variants')
        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 str(var))
        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 f"hydro_id = {hydro_object.id}, selected_variant_id = {selected_variant_id}")

        from legger import settings
        verhang = 3.0
        color_map = (
            (1.0, settings.LOW_COLOR),
            (3.0, settings.OK_COLOR),
            (4.0, settings.HIGH_COLOR),
        )
        profs = []

        # force query
        profiles = list(var.all())

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'update_available_variants - finished fetching variants - start loop')

        for profile in profiles:
            active = selected_variant_id == profile.id
            over_width = None
            over_depth = None

            if profile.figuren:
                over_width = profile.figuren[0].t_overbreedte_l + profile.figuren[0].t_overbreedte_r
                over_depth = profile.figuren[0].t_overdiepte
            else:
                if profile.hydro.kenmerken and profile.hydro.kenmerken[0].diepte is not None and profile.diepte is not None:
                    over_depth = profile.hydro.kenmerken[0].diepte - profile.diepte
                if profile.hydro.kenmerken and profile.hydro.kenmerken[0].breedte is not None and profile.waterbreedte is not None:
                    over_width = profile.hydro.kenmerken[0].breedte - profile.waterbreedte

            profs.append({
                'name': profile.id,
                'active': active,  # digits differ far after the
                'depth': profile.diepte,
                'begroeiingsvariant': profile.begroeiingsvariant.naam,
                'score': profile.figuren[0].t_fit if profile.figuren else None,
                'over_depth': over_depth if over_depth is not None else None,
                'over_width': over_width if over_depth is not None else None,
                'over_width_color': [255, 0, 0] if over_width < 0 else [255, 255, 255],
                'verhang': profile.verhang,
                'color': interpolated_color(value=profile.verhang, color_map=color_map,
                                            alpha=(255 if active else 80)),
                'points': [
                    (-0.5 * profile.waterbreedte, hydro_object.streefpeil),
                    (-0.5 * profile.bodembreedte, hydro_object.streefpeil - profile.diepte),
                    (0.5 * profile.bodembreedte, hydro_object.streefpeil - profile.diepte),
                    (0.5 * profile.waterbreedte, hydro_object.streefpeil),
                ]
            })

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'update_available_variants - finished loop - start update model')

        self.variant_model.insertRows(profs)

        perf_log(inspect.getframeinfo(inspect.currentframe()).lineno,
                 'update_available_variants - finished update model')

    def update_available_profiles(self, item, variant):
        """
            used for updating ranges after adding a profile manually
        """

        # update variant table
        self.on_select_edit_hydrovak(item)
        diepte = float(variant.diepte)

        if item.hydrovak.get('variant_max_depth') is None or diepte > item.hydrovak.get('variant_max_depth'):
            self.legger_model.setDataItemKey(item, 'variant_max_depth', diepte)

        if item.hydrovak.get('variant_min_depth') is None or diepte < item.hydrovak.get('variant_min_depth'):
            self.legger_model.setDataItemKey(item, 'variant_min_depth', diepte)

    def onSelectBegroeiingsVariant(self):
        self.active_begroeiings_variant = self.begroeiings_combo.currentText()
        self.update_available_variants()

    def get_begroeiings_variant(self):
        return self.begroeiings_varianten[self.begroeiings_combo.currentText()]

    def get_begroeiings_strategy(self):
        return self.begroeiings_variant_strategies[self.begroeiings_strategy_combo.currentText()]

    def get_child_selection_strategy(self):

        return self.child_selection_strategies[self.child_selection_strategy_combo.currentText()]

    def closeEvent(self, event):
        """
        close event for widget, including removal of layers and disconnection of listeners
        event: close event
        return: None
        """
        self.save_remarks()

        if self.vl_tree_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.vl_tree_layer)
        if self.line_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.line_layer)
        if self.vl_endpoint_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.vl_endpoint_layer)
        if self.vl_track_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.vl_track_layer)
        if self.vl_hover_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.vl_hover_layer)
        if self.vl_selected_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.vl_selected_layer)
        if self.vl_startpoint_hover_layer in QgsMapLayerRegistry.instance().mapLayers().values():
            QgsMapLayerRegistry.instance().removeMapLayer(self.vl_startpoint_hover_layer)

        self.category_combo.currentIndexChanged.disconnect(self.category_change)
        self.show_manual_input_button.clicked.disconnect(self.show_manual_input_window)
        self.next_endpoint_button.clicked.disconnect(self.set_next_endpoint)
        self.variant_model.dataChanged.disconnect(self.data_changed_variant)
        self.legger_model.dataChanged.disconnect(self.data_changed_legger_tree)
        self.area_model.dataChanged.disconnect(self.data_changed_area_model)
        self.begroeiings_combo.currentIndexChanged.disconnect(self.onSelectBegroeiingsVariant)

        self.legger_model.setTreeWidget(None)

        self.closingWidget.emit()
        event.accept()

    def setup_ui(self, dock_widget):
        """
        initiate main Qt building blocks of interface
        :param dock_widget: QDockWidget instance
        """

        dock_widget.setObjectName("dock_widget")
        dock_widget.setAttribute(Qt.WA_DeleteOnClose)

        self.dock_widget_content = QWidget(self)
        self.dock_widget_content.setObjectName("dockWidgetContent")

        self.main_vlayout = QVBoxLayout(self)
        self.dock_widget_content.setLayout(self.main_vlayout)

        # add button to add objects to graphs
        self.button_bar_hlayout = QHBoxLayout(self)

        self.show_manual_input_button = QPushButton(self)
        self.button_bar_hlayout.addWidget(self.show_manual_input_button)
        self.show_manual_input_button.setDisabled(True)

        self.button_bar_hlayout.addWidget(QLabel("filter t/m categorie:"))
        self.category_combo = QComboBox(self)
        self.button_bar_hlayout.addWidget(self.category_combo)

        self.next_endpoint_button = QPushButton(self)
        self.button_bar_hlayout.addWidget(self.next_endpoint_button)
        self.next_endpoint_button.setDisabled(True)

        self.child_selection_strategy_combo = QComboBox(self)
        self.button_bar_hlayout.addWidget(QLabel("doortrekken tot:"))
        self.button_bar_hlayout.addWidget(self.child_selection_strategy_combo)

        spacer_item = QSpacerItem(40,
                                  20,
                                  QSizePolicy.Expanding,
                                  QSizePolicy.Minimum)
        self.button_bar_hlayout.addItem(spacer_item)

        # self.button_bar_hlayout.addItem(QLabel("doortrekken tot:"))
        # self.button_bar_hlayout.addItem(self.begroeiings_strategy_combo)

        self.main_vlayout.addLayout(self.button_bar_hlayout)
        # add tabWidget for graphWidgets
        self.contentLayout = QHBoxLayout(self)

        self.tree_table_tab = QTabWidget(self)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(
            self.tree_table_tab.sizePolicy().hasHeightForWidth())
        self.tree_table_tab.setSizePolicy(sizePolicy)
        self.tree_table_tab.setMinimumSize(QSize(850, 0))

        self.contentLayout.addWidget(self.tree_table_tab)

        # startpointTree
        self.startpoint_tree_widget = StartpointTreeWidget(self, self.area_model)
        # sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        # sizePolicy.setHorizontalStretch(0)
        # sizePolicy.setVerticalStretch(0)
        # sizePolicy.setHeightForWidth(
        #     self.legger_tree_widget.sizePolicy().hasHeightForWidth())
        # self.legger_tree_widget.setSizePolicy(sizePolicy)
        # self.legger_tree_widget.setMinimumSize(QSize(750, 0))

        self.tree_table_tab.addTab(self.startpoint_tree_widget, 'startpunten')

        # LeggerTree
        self.legger_tree_widget = LeggerTreeWidget(self, self.legger_model)
        # sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        # sizePolicy.setHorizontalStretch(0)
        # sizePolicy.setVerticalStretch(0)
        # sizePolicy.setHeightForWidth(
        #     self.legger_tree_widget.sizePolicy().hasHeightForWidth())
        # self.legger_tree_widget.setSizePolicy(sizePolicy)
        # self.legger_tree_widget.setMinimumSize(QSize(750, 0))

        self.tree_table_tab.addTab(self.legger_tree_widget, 'hydrovakken')

        # graphs
        self.graph_vlayout = QVBoxLayout(self)

        # Graph
        self.plot_widget = LeggerPlotWidget(
            self, session=self.session,
            legger_model=self.legger_model,
            variant_model=self.variant_model)
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(1)
        sizePolicy.setVerticalStretch(1)
        sizePolicy.setHeightForWidth(
            self.plot_widget.sizePolicy().hasHeightForWidth())
        self.plot_widget.setSizePolicy(sizePolicy)
        self.plot_widget.setMinimumSize(QSize(250, 150))

        self.graph_vlayout.addWidget(self.plot_widget, 2)

        # Sideview Graph
        self.sideview_widget = LeggerSideViewPlotWidget(
            self, session=self.session,
            legger_model=self.legger_model)
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(1)
        sizePolicy.setVerticalStretch(1)
        sizePolicy.setHeightForWidth(
            self.sideview_widget.sizePolicy().hasHeightForWidth())
        self.sideview_widget.setSizePolicy(sizePolicy)
        self.sideview_widget.setMinimumSize(QSize(250, 150))

        self.graph_vlayout.addWidget(self.sideview_widget)

        self.contentLayout.addLayout(self.graph_vlayout, 2)

        self.rightVstack = QVBoxLayout(self)
        sizePolicy = QSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)

        self.begroeiings_combo = QComboBox(self)
        self.begroeiings_strategy_combo = QComboBox(self)
        self.groupBox_begroeiings = QGroupBox(self)
        self.groupBox_begroeiings.setTitle("begroeiingsfilter en voor welk deel")
        vbox_strat = QVBoxLayout()
        vbox_strat.addWidget(self.begroeiings_combo)
        vbox_strat.addWidget(self.begroeiings_strategy_combo)
        self.groupBox_begroeiings.setLayout(vbox_strat)
        self.rightVstack.addWidget(self.groupBox_begroeiings)

        # variantentable
        self.plot_item_table = VariantenTable(self, variant_model=self.variant_model)
        self.plot_item_table.setMinimumWidth(380)

        self.rightVstack.addWidget(self.plot_item_table)

        self.selected_variant_remark = QPlainTextEdit(self)
        self.selected_variant_remark.setFixedHeight(100)
        self.selected_variant_remark.setDisabled(True)

        self.rightVstack.addWidget(self.selected_variant_remark)

        self.contentLayout.addLayout(self.rightVstack, 0)

        self.main_vlayout.addLayout(self.contentLayout)

        # add dockwidget
        dock_widget.setWidget(self.dock_widget_content)
        self.retranslate_ui(dock_widget)
        QMetaObject.connectSlotsByName(dock_widget)

    def retranslate_ui(self, dock_widget):
        pass
        dock_widget.setWindowTitle(_translate(
            "DockWidget", "Legger", None))
        self.show_manual_input_button.setText(_translate(
            "DockWidget", "Voeg profiel toe", None))
        self.next_endpoint_button.setText(_translate(
            "DockWidget", "Volgend eindpunt", None))
