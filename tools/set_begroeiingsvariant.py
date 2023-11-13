# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans, see LICENSE.rst.
# bewerking threedi_result_selection.py van de ThreeDiToolbox QGis plugin

import logging
import os

from qgis.PyQt.QtCore import Qt, QObject
from qgis.utils import plugins
from qgis.gui import QgsMessageBar
from qgis.PyQt.QtWidgets import QAction, QMenu
from qgis.PyQt.QtGui import QIcon

from legger.utils.user_message import messagebar_message
from legger.views.calculating_profiles import ProfileCalculationWidget
from legger.sql_models.legger import BegroeiingsVariant
from legger.sql_models.legger_database import LeggerDatabase

log = logging.getLogger(__name__)


class SetBegroeiingsvariant(QObject):
    """QGIS Plugin Implementation."""

    tool_name = 'zet begroeiingsvariant'

    def __init__(self, iface, root_tool):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :param root_tool: A reference to the parent window where the reference to databases and files are declared.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        QObject.__init__(self)
        self.iface = iface
        self.root_tool = root_tool

        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.icon_path = ':/plugins/legger/media/calculator-icon.png'
        self.menu_text = u'Zet begroeiingsvariant voor selectie'

        # add listereners
        self.root_tool.polderDatasourceChanged.connect(self.set_variant_items)

        self.is_active = False
        self.dialog = None

    def set_variant_items(self, polder_datasource):

        def callback_factory(variant_id):
            return lambda: self.attach_variant(variant_id=variant_id)

        try:
            if polder_datasource:
                self.remove_variant_items()
                db = LeggerDatabase(polder_datasource)
                db.create_and_check_fields()
                self.session = db.get_session()
                for variant in self.session.query(BegroeiingsVariant):
                    action = QAction(variant.naam, self.iface.mainWindow())
                    self.popupMenu.addAction(action)
                    action.triggered.connect(callback_factory(variant.id))
        except Exception as e:
            log.warning('not able to get begroeiingsvarianten. Melding: %s', e)

    def remove_variant_items(self):

        for action in self.popupMenu.actions():
            # action.triggered.connect(lambda variant_id=variant.id: self.attach_variant(variant_id))
            self.popupMenu.removeAction(action)

    def get_action(self):

        self.action_base = QAction(u"zet begroeiing", self.iface.mainWindow())
        self.popupMenu = QMenu(self.iface.mainWindow())
        self.set_variant_items(self.root_tool.polder_datasource)
        self.action_base.setMenu(self.popupMenu)
        # self.iface.addToolBarIcon(self.action_base)
        return self.action_base

    def attach_variant(self, variant_id, *args, **kwargs):
        a = 1
        for layer in self.iface.legendInterface().selectedLayers():
            if any([a.name() == 'begroeiingsvariant_id' for a in layer.pendingFields()]):
                # if not layer.isEditable():
                #     messagebar_message('Fout', 'Laag is niet bewerkbaar', QgsMessageBar.WARNING, 15)
                #     return

                features = layer.selectedFeatures()
                layer.startEditing()
                count = 0
                for feature in features:
                    feature['begroeiingsvariant_id'] = variant_id
                    count += 1
                    layer.updateFeature(feature)

                layer.commitChanges()

                messagebar_message('Gelukt', '{0} features op begroeiingsvariant gezet.'.format(count),
                                   QgsMessageBar.INFO, 15)

                if self.iface.mapCanvas().isCachingEnabled():
                    layer.setCacheImage(None)
                else:
                    self.iface.mapCanvas().refresh()

            else:
                pass
                # todo: select editable layer with link to begroeiingsVariant
                messagebar_message('Fout', 'Laag bevat geen link naar begroeiingsVariant', QgsMessageBar.WARNING, 15)
                return

    def on_unload(self):
        """Cleanup necessary items here when dialog is closed"""
        self.root_tool.polderDatasourceChanged.disconnect(self.set_variant_items)

    def on_close_dialog(self):
        """Cleanup necessary items here when dialog is closed"""

        self.dialog.closingDialog.disconnect(self.on_close_dialog)

        self.dialog = None
        self.is_active = False
