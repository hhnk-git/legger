""" The graphs of the legger network tool"""

import logging

from collections import OrderedDict

import pyqtgraph as pg
from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtGui import QBrush, QColor
import numpy as np
from legger.sql_models.legger import HydroObject, ProfielFiguren, Varianten
from shapely.wkt import loads
from qgis.core import NULL

from legger import settings

log = logging.getLogger('legger.' + __name__)

precision = 0.000001

pg.setConfigOption('background', 'w')
pg.setConfigOption('foreground', 'k')


class LeggerPlotWidget(pg.PlotWidget):

    def __init__(self, parent=None, name="", session=None, legger_model=None, variant_model=None):

        super(LeggerPlotWidget, self).__init__(parent)
        # init parameters
        self.series = {}
        self.variant_model = None
        self.legger_model = None

        # store arguments
        self.name = name
        self.session = session
        self.reference_level = 0
        if legger_model:
            self.setLeggerModel(legger_model)
        if variant_model:
            self.setVariantModel(variant_model)

        self.measured_plots = []
        self.hover_measured_plots = []
        self.hover_selected_plot = None
        self.selected_measured_plots = []

        # set other
        self.showGrid(True, True, 0.5)
        self.setLabel("bottom", "breedte", "m")
        self.setLabel("left", "hoogte", "m tov waterlijn")

        self.def_measured_opacity = 80

    def setLeggerModel(self, model):
        # todo: remove listeners to old model?
        self.legger_model = model
        self.legger_model.dataChanged.connect(self.data_changed_legger)
        self.legger_model.rowsInserted.connect(self.data_changed_legger)
        self.legger_model.rowsAboutToBeRemoved.connect(
            self.data_changed_legger)

    def setVariantModel(self, model):
        # todo: remove listeners to old model?
        self.variant_model = model
        self.variant_model.dataChanged.connect(self.data_changed_variant)
        self.variant_model.rowsInserted.connect(self.add_variant_lines)
        self.variant_model.rowsAboutToBeRemoved.connect(
            self.clear_variant_lines)

    def clear_variant_lines(self, index, to_index=None):

        for item in self.variant_model.rows:
            if hasattr(item, '_plot'):
                self.removeItem(item._plot)

    def add_variant_lines(self, index, to_index=None):

        for item in self.variant_model.rows:

            # todo: bind a better way to get the reference level. This is working, because for a
            # hydrovak is selected, the row of the hydrovak is hovered and the reference level
            # is set. But this is probably not a fully bullet proof solution.
            width = [p[0] for p in item.points.value]
            height = [p[1] - self.reference_level for p in item.points.value]

            if item.active.value:
                line_width = 3
            else:
                line_width = 1

            plot_item = pg.PlotDataItem(
                x=width,
                y=height,
                connect='finite',
                pen=pg.mkPen(color=item.color.value, width=line_width))

            # keep reference
            item._plot = plot_item
            self.addItem(item._plot)

        self.autoRange()

    def data_changed_variant(self, index, to_index=None):
        """
         add width logic to hover variant table
         :param index: index of changed field
         """
        model = self.variant_model
        if model.columns[index.column()].name in ['color', 'active']:
            item = model.rows[index.row()]
            if item.active.value:
                item._plot.setPen(color=item.color.value,
                                  width=3)
            elif item.hover.value:
                item._plot.setPen(color=item.color.value,
                                  width=3)
            else:
                item._plot.setPen(color=item.color.value,
                                  width=1)

    def clear_measured_plots(self):

        for prof in self.measured_plots:
            if 'plot' in prof:
                self.removeItem(prof['plot'])

        self.measured_plots = []

    def get_measured_plot(self, prof, opacity=255, midpoint=None):
        """

        profiles (dict): dictionaries with 'name', 'color' and 'points' with a list of tuples with (x, y)
        return: plotItem
        """

        # points is a polygon, so last point is same as first. Therefor take [-2]
        if midpoint is None:
            midpoint = (prof['points'][0][0] + prof['points'][-2][0]) / 2
        # todo: use calculated offset

        width = [p[0] - midpoint for p in prof['points']]
        height = [p[1] for p in prof['points']]

        pen_params = {
            'color': list(prof['color'])[:3] + [opacity],
            'width': prof.get('width', 1)
        }
        if 'style' in prof:
            pen_params['style'] = prof.get('style')

        return pg.PlotDataItem(
            x=width,
            y=height,
            connect='finite',
            pen=pg.mkPen(**pen_params))

    def data_changed_legger(self, index, to_index=None):

        field = self.legger_model.column(index.column())['field']

        if field in ['ep', 'sp']:

            if self.legger_model.ep is not None:
                up = self.legger_model.ep.up(end=self.legger_model.sp, include_point=True)
                ids = []
                for line in reversed(up):
                    if line.hydrovak is not None:
                        ids.append(line.hydrovak.get('hydro_id'))

                if len(ids):
                    hydro_objects = self.session.query(HydroObject).filter(HydroObject.id.in_(ids)).all()

                    self.reference_level = up[0].hydrovak.get('target_level', 0.0)
                    self.clear_measured_plots()
                    # add measured profiles over full track
                    for obj in hydro_objects:
                        for profile in obj.figuren.filter_by(type_prof='m').all():
                            prof = {
                                'name': profile.profid,
                                'color': (100, 100, 100),
                                'points': [(p[0], p[1] - profile.peil)
                                           for p in loads(profile.coord).exterior.coords[:]]
                            }

                            prof['plot'] = self.get_measured_plot(prof, self.def_measured_opacity)

                            # keep reference
                            self.measured_plots.append(prof)
                            self.addItem(prof['plot'])
            else:
                self.clear_measured_plots()

        elif field == 'hover':
            item = self.legger_model.data(index, role=Qt.UserRole)

            if item.hydrovak.get('hover'):

                # add measured profiles
                self.reference_level = item.hydrovak.get('target_level', 0.0)
                for profile in self.session.query(ProfielFiguren).filter(
                        HydroObject.id == ProfielFiguren.hydro_id,
                        HydroObject.id == item.hydrovak.get('hydro_id'),
                        ProfielFiguren.type_prof == 'm').all():
                    # exterior.coords[:] --> without [:] the wrong numbers are returned
                    prof = {
                        'name': profile.profid,
                        'color': settings.HOVER_COLOR,
                        'style': Qt.DotLine,
                        'width': 2,
                        'points': [(p[0], p[1] - profile.peil)
                                   for p in loads(profile.coord).exterior.coords[:]]
                    }  # add extra step with zip and xy, because loop over coords sometimes provide only empty numbers

                    prof['plot'] = self.get_measured_plot(prof, 255)
                    # keep reference
                    self.hover_measured_plots.append(prof)
                    self.addItem(prof['plot'])

                # add shape based on width and estimated depth in case no measured profile in this hydrovak
                if len(self.hover_measured_plots) == 0:
                    width = item.hydrovak.get('width')
                    depth = item.hydrovak.get('depth')
                    if width is not None and depth is not None:
                        prof = {
                            'name': 'extents',
                            'color': settings.HOVER_COLOR,
                            'style': Qt.DotLine,
                            'width': 2,
                            'points': [(-width / 2, 0), (-width / 2, -depth), (width / 2, -depth), (width / 2, 0),
                                       (-width / 2, 0)]
                        }

                        prof['plot'] = self.get_measured_plot(prof, 255, 0)
                        # keep reference
                        self.hover_measured_plots.append(prof)
                        self.addItem(prof['plot'])

                # show selected legger profile
                selected_variant_id = item.hydrovak.get('selected_variant_id')
                if selected_variant_id is not None and selected_variant_id != NULL:
                    profile_variant = self.session.query(Varianten).filter(
                        Varianten.id == selected_variant_id
                    )

                    if profile_variant.count() > 0:
                        profile = profile_variant.first()
                        prof = {
                            'name': profile.id,
                            'color': settings.HOVER_COLOR,
                            'width': 3,
                            'points': [
                                (-0.5 * profile.waterbreedte, 0),
                                (-0.5 * profile.bodembreedte, -1 * profile.diepte),
                                (0.5 * profile.bodembreedte, -1 * profile.diepte),
                                (0.5 * profile.waterbreedte, 0),
                            ]
                        }

                        prof['plot'] = self.hover_selected_plot = self.get_measured_plot(prof, 255, 0)
                        self.hover_selected_plot = prof
                        self.addItem(prof['plot'])

            else:
                for prof in self.hover_measured_plots:
                    if 'plot' in prof:
                        self.removeItem(prof['plot'])

                if self.hover_selected_plot is not None:
                    self.removeItem(self.hover_selected_plot['plot'])
                self.hover_measured_plots = []

        elif field == 'selected':
            item = self.legger_model.data(index, role=Qt.UserRole)

            if item.hydrovak.get('selected'):
                # add measured profiles
                self.reference_level = item.hydrovak.get('target_level', 0.0)
                for profile in self.session.query(ProfielFiguren).filter(
                        HydroObject.id == ProfielFiguren.hydro_id,
                        HydroObject.id == item.hydrovak.get('hydro_id'),
                        ProfielFiguren.type_prof == 'm').all():
                    prof = {
                        'name': profile.profid,
                        'color': settings.SELECT_COLOR,
                        'style': Qt.DashLine,
                        'width': 2,
                        'points': [(p[0], p[1] - profile.peil)
                                   for p in loads(profile.coord).exterior.coords[:]]
                    }

                    prof['plot'] = self.get_measured_plot(prof, 255)

                    # keep reference
                    self.selected_measured_plots.append(prof)
                    self.addItem(prof['plot'])

                # add shape based on width and estimated depth in case no measured profile in this hydrovak
                if len(self.selected_measured_plots) == 0:
                    width = item.hydrovak.get('width')
                    depth = item.hydrovak.get('depth')
                    if width is not None and depth is not None:
                        prof = {
                            'name': 'extents',
                            'color': settings.SELECT_COLOR,
                            'style': Qt.DashLine,
                            'width': 2,
                            'points': [(-width / 2, 0), (-width / 2, -depth), (width / 2, -depth), (width / 2, 0),
                                       (-width / 2, 0)]
                        }

                        prof['plot'] = self.get_measured_plot(prof, 255, 0)
                        # keep reference
                        self.selected_measured_plots.append(prof)
                        self.addItem(prof['plot'])

            else:
                # clear list
                for prof in self.selected_measured_plots:
                    if 'plot' in prof:
                        self.removeItem(prof['plot'])

                self.selected_measured_plots = []


class LeggerSideViewPlotWidget(pg.PlotWidget):

    def __init__(self, parent=None, name="", session=None, legger_model=None, relative_depth=True):
        super(LeggerSideViewPlotWidget, self).__init__(parent)

        # init parameters
        self.legger_model = None
        self.variant_model = None
        self.series = {}
        self.line_data = {}

        # store arguments
        self.name = name
        self.session = session
        self.relative_depth = relative_depth
        if legger_model:
            self.setLeggerModel(legger_model)

        # set other
        self.showGrid(True, True, 0.5)
        self.setLabel("bottom", "lengte", "m")
        self.setLabel("left", "hoogte", "m tov waterlijn")
        self.disableAutoRange()

        self.target_level_plot = pg.PlotDataItem(
            x=[],
            y=[],
            # connect='finite',
            pen=pg.mkPen(color=[30, 144, 255, 150], width=2))

        self.depth_plot = pg.PlotDataItem(
            x=[], y=[],
            connect='finite',
            pen=pg.mkPen(color=[0, 100, 255, 150], width=2))

        self.depth_plot_interpolated = pg.PlotDataItem(
            x=[], y=[],
            # connect='finite',
            pen=pg.mkPen(color=[0, 100, 255, 150], width=2))

        self.water_fill = pg.FillBetweenItem(self.target_level_plot, self.depth_plot_interpolated,
                                             brush=pg.mkBrush(color=[0, 100, 255, 25]))

        self.min_depth_plot = pg.PlotDataItem(
            x=[], y=[],
            connect='finite',
            pen=pg.mkPen(color=[246, 166, 0, 150], width=2))

        self.max_depth_plot = pg.PlotDataItem(
            x=[], y=[],
            connect='finite',
            pen=pg.mkPen(color=[246, 80, 0, 150], width=2))

        self.min_depth_plot_interpolated = pg.PlotDataItem(
            x=[], y=[],
            # connect='finite',
            pen=pg.mkPen(None))

        self.max_depth_plot_interpolated = pg.PlotDataItem(
            x=[], y=[],
            # connect='finite',
            pen=pg.mkPen(None))

        self.available_fill = pg.FillBetweenItem(self.min_depth_plot_interpolated, self.max_depth_plot_interpolated,
                                                 brush=pg.mkBrush(QBrush(QColor(246, 80, 0, 10), Qt.SolidPattern)))

        self.selected_depth_plot = pg.PlotDataItem(
            x=[],
            y=[],
            connect='finite',
            pen=pg.mkPen(color=[0, 0, 0, 220], width=3))

        self.tmp_selected_depth_plot = pg.PlotDataItem(
            x=[],
            y=[],
            connect='finite',
            pen=pg.mkPen(color=[60, 60, 60, 120], width=3))

        self.hover_start = pg.InfiniteLine(None,
                                           pen=pg.mkPen(color=QColor(*settings.HOVER_COLOR), width=3))
        self.hover_end = pg.InfiniteLine(None,
                                         pen=pg.mkPen(color=QColor(*settings.HOVER_COLOR), width=3))

        self.selected_start = pg.InfiniteLine(None,
                                              pen=pg.mkPen(color=QColor(*settings.SELECT_COLOR), width=3))
        self.selected_end = pg.InfiniteLine(None,
                                            pen=pg.mkPen(color=QColor(*settings.SELECT_COLOR), width=3))

        # self.hover_fill = pg.FillBetweenItem(self.hover_start, self.hover_end,
        #                                    brush=pg.mkBrush(QBrush(QColor(100, 100, 100, 10), Qt.SolidPattern)))

        for item in [self.hover_start, self.hover_end,
                     self.selected_start, self.selected_end,
                     self.water_fill, self.target_level_plot, self.depth_plot,
                     self.available_fill, self.min_depth_plot, self.max_depth_plot,
                     self.selected_depth_plot, self.tmp_selected_depth_plot]:
            self.addItem(item)

    def setLeggerModel(self, model):
        # todo: remove listeners to old model?
        self.legger_model = model
        self.legger_model.dataChanged.connect(self.data_changed_legger)
        self.legger_model.rowsInserted.connect(self.data_changed_legger)
        self.legger_model.rowsAboutToBeRemoved.connect(
            self.data_changed_legger)

    def clear_graph(self):
        self.depth_plot.setData(x=[], y=[])
        self.depth_plot_interpolated.setData(x=[], y=[])
        self.min_depth_plot.setData(x=[], y=[])
        self.max_depth_plot.setData(x=[], y=[])
        self.min_depth_plot_interpolated.setData(x=[], y=[])
        self.max_depth_plot_interpolated.setData(x=[], y=[])

        self.target_level_plot.setData(x=[], y=[])

    def draw_base_lines(self, data):

        dist = []
        depth = []
        min_depth = []
        max_depth = []
        target_level = []

        for item in data.values():
            if self.relative_depth:
                ref_level = 0
            else:
                ref_level = item['target_level']

            d = ref_level - item['depth'] if item['depth'] is not None else np.nan
            mind = ref_level - item['variant_min_depth'] if item['variant_min_depth'] is not None else np.nan
            maxd = ref_level - item['variant_max_depth'] if item['variant_max_depth'] is not None else np.nan

            dist.append(item['b_distance'])
            dist.append(item['e_distance'])
            depth.append(d)
            depth.append(d)
            min_depth.append(mind)
            min_depth.append(mind)
            max_depth.append(maxd)
            max_depth.append(maxd)
            target_level.append(ref_level)
            target_level.append(ref_level)

        self.depth_plot.setData(x=dist, y=depth)
        self.depth_plot_interpolated.setData(x=dist, y=depth)
        self.min_depth_plot.setData(x=dist, y=min_depth)
        self.max_depth_plot.setData(x=dist, y=max_depth)
        self.min_depth_plot_interpolated.setData(x=dist, y=min_depth)
        self.max_depth_plot_interpolated.setData(x=dist, y=max_depth)

        self.target_level_plot.setData(x=dist, y=target_level)

    def draw_selected_lines(self, data):

        dist = []
        depth = []
        tmp_depth = []

        for item in data.values():
            if self.relative_depth:
                ref_level = 0
            else:
                ref_level = item['target_level']

            d = ref_level - item['selected_depth'] if item['selected_depth'] is not None else np.nan
            tmp_d = ref_level - item['selected_depth_tmp'] if item['selected_depth_tmp'] is not None else np.nan

            dist.append(item['b_distance'])
            dist.append(item['e_distance'])
            depth.append(d)
            depth.append(d)
            tmp_depth.append(tmp_d)
            tmp_depth.append(tmp_d)

        self.selected_depth_plot.setData(x=dist, y=depth)
        self.tmp_selected_depth_plot.setData(x=dist, y=tmp_depth)

    def data_changed_legger(self, index):
        """ draw start and endlines of selected or hovered hydrovaks (green en red vertical lines) """

        model = self.legger_model
        field = model.column(index.column())['field']
        item = self.legger_model.data(index, role=Qt.UserRole)

        if field == 'hover':
            if not item.hydrovak['hover'] or item.hydrovak['hydro_id'] not in self.line_data:
                self.hover_start.setValue(0)
                self.hover_end.setValue(0)
            else:
                self.disableAutoRange()
                # show only location in graph when hydrovak is on selected part
                item_data = self.line_data[item.hydrovak['hydro_id']]

                self.hover_start.setValue(item_data['b_distance'])
                self.hover_end.setValue(item_data['e_distance'])

        elif field == 'selected':

            if not item.hydrovak['selected'] or item.hydrovak['hydro_id'] not in self.line_data:
                self.selected_start.setValue(0)
                self.selected_end.setValue(0)
            else:
                self.disableAutoRange()
                # show only location in graph when hydrovak is on selected part
                item_data = self.line_data[item.hydrovak['hydro_id']]

                self.selected_start.setValue(item_data['b_distance'])
                self.selected_end.setValue(item_data['e_distance'])

        elif field in ['sp', 'ep']:
            if item.hydrovak.get('tak'):
                self.clear_graph()
            elif self.legger_model.data(index, role=Qt.CheckStateRole) and self.legger_model.ep is not None:
                data = self._get_data()
                self.draw_base_lines(data)
                self.draw_selected_lines(data)
                self.autoRange()
            else:
                self.clear_graph()

        elif field in ['selected_depth', 'selected_depth_tmp']:
            pass
            # switched off, because each individual hydrovak change triggered a repaint (slow)
            # now triggered in function
            # self.draw_selected_lines(self._get_data())

        elif field in ['variant_min_depth', 'variant_max_depth']:
            self.draw_base_lines(self._get_data())

    def _get_data(self):
        if self.legger_model.ep is None:
            return OrderedDict([])
        up = self.legger_model.ep.up(end=self.legger_model.sp)
        out = []
        before = up[-1]
        if before.hydrovak:
            dist = before.hydrovak.get('distance')
        else:
            dist = 0

        for line in reversed(up):
            # skip extra 'tak' nodes
            if line.hydrovak.get('tak'):
                continue

            out.append(
                {
                    'id': line.hydrovak.get('hydro_id'),
                    'b_distance': line.hydrovak.get('distance') - dist,
                    'e_distance': line.hydrovak.get('distance') + line.hydrovak.get('length') - dist,
                    'depth': line.hydrovak.get('depth'),
                    'target_level': line.hydrovak.get('target_level'),
                    'variant_min_depth': line.hydrovak.get('variant_min_depth'),
                    'variant_max_depth': line.hydrovak.get('variant_max_depth'),
                    'selected_depth': line.hydrovak.get('selected_depth'),
                    'selected_depth_tmp': line.hydrovak.get('selected_depth_tmp'),
                })

        self.line_data = OrderedDict([(l['id'], l) for l in out])
        return self.line_data
