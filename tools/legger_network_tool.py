import os.path

from legger.utils.user_message import messagebar_message
from legger.views.legger_network_widget import LeggerWidget
from qgis.PyQt.QtCore import Qt


class LeggerNetworkTool:
    """QGIS Plugin Implementation."""

    def __init__(self, iface, root_tool):
        """Constructor.

        iface (QgsInterface): An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        """
        # Save reference to the QGIS interface
        self.iface = iface
        self.root_tool = root_tool

        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)

        self.icon_path = ':/plugins/legger/media/icon_legger.png'
        self.menu_text = u'legger berekenen'

        self.dock_widget = None

    def on_unload(self):
        """
        on close of graph plugin
        """
        if self.dock_widget is not None:
            self.dock_widget.close()

    def on_close_widget(self):
        """Cleanup necessary items here when plugin dockwidget is closed"""

        # close widget
        self.dock_widget.closingWidget.disconnect(self.on_close_widget)

        self.dock_widget = None

    def run(self):
        """
        Run method that loads and starts the plugin (docked graph widget)
        """
        # create the dockwidget

        db_path = self.root_tool.polder_datasource

        # db_path = os.path.join(
        #     os.path.dirname(__file__),
        #     os.path.pardir,
        #     'tests', 'data',
        #     'test_spatialite_with_matchprof.sqlite'
        # )

        if not self.root_tool.polder_datasource:
            messagebar_message('input', 'Er is geen leggerdatabase opgegeven',
                               level=2)
            self.root_tool.read_database.run()

            return

        if self.dock_widget is None:
            self.dock_widget = LeggerWidget(
                iface=self.iface,
                parent=None,
                path_legger_db=db_path
            )
            # connect cleanup on closing of dockwidget
            self.dock_widget.closingWidget.connect(self.on_close_widget)

            # show the dockwidget
            self.iface.addDockWidget(Qt.BottomDockWidgetArea, self.dock_widget)

        self.dock_widget.show()
