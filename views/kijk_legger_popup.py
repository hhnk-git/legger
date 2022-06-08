# -*- coding: utf-8 -*-
from __future__ import division

import datetime
import logging
from typing import TYPE_CHECKING

from legger.sql_models.legger_database import load_spatialite
from qgis.PyQt import QtCore, QtWidgets
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import QWidget

if TYPE_CHECKING:
    from legger.views.legger_network_widget import LeggerWidget

log = logging.getLogger(__name__)

try:
    _encoding = QtWidgets.QApplication.UnicodeUTF8


    def _translate(context, text, disambig):
        return QtWidgets.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtWidgets.QApplication.translate(context, text, disambig)


class KijkProfielPopup(QWidget):  # , FORM_CLASS):
    """Dialog for selecting model (spatialite and result files netCDFs)"""
    closingDialog = pyqtSignal()

    def __init__(
            self, parent, iface, netwerktool: "LeggerWidget"):
        """Constructor

        :parent: Qt parent Widget
        :iface: QGiS interface
        :polder_datasource: Polder spatialite instance
        :parent_class: the tool class which instantiated this widget. Is used
             here for storing volatile information
        """
        super().__init__()

        self.iface = iface
        self.setup_ui()

        self.netwerktool = netwerktool

        init_width = netwerktool.selected_hydrovak.hydrovak.get('kijkp_breedte')
        init_depth = netwerktool.selected_hydrovak.hydrovak.get('kijkp_diepte')
        init_talud = netwerktool.selected_hydrovak.hydrovak.get('kijkp_talud')
        init_reason = netwerktool.selected_hydrovak.hydrovak.get('kijkp_reden')

        self.input_width.setValue(init_width if init_width is not None else netwerktool.init_width)
        self.input_depth.setValue(init_depth if init_depth is not None else netwerktool.init_depth)
        self.input_talud.setValue(init_talud if init_talud is not None else netwerktool.init_talud)
        self.input_reason.setCurrentText(init_reason if init_reason is not None else netwerktool.init_reason)

        self.input_reason.insertItems(
            0,
            ['',
             'Waterkwaliteit & ecologie',
             'Baggeren',
             'Oeverstabiliteit',
             'Maaien',
             'Wateraanvoer',
             'Vis(sserij)',
             'Zwemwater',
             'Vaarwegen',
             'Woonboten',
             'Belangen omgeving'
             ])

    def save_to_db(self, width, depth, talud, reason):
        # ids = [feat[0] for feat in self.netwerktool.vl_track_layer.getFeatures()]
        # one hydrovak, as requested on 7-6-2022
        idv = self.netwerktool.selected_hydrovak.hydrovak.get('id')

        if idv:
            session = load_spatialite(self.netwerktool.path_legger_db)

            # save to database
            session.execute(
                """
                UPDATE 
                    hydroobject 
                SET
                 kijkp_breedte = ?,
                 kijkp_diepte = ?,
                 kijkp_talud = ?,
                 kijkp_reden = ?
                 WHERE 
                  id = ?
            """,
                [width, depth, talud, reason, idv]
            )
            # update table
            # set new value to trigger repaint
            self.netwerktool.legger_model.setDataItemKey(
                self.netwerktool.selected_hydrovak,
                'kijkp_breedte',
                datetime.datetime.now().isoformat())

            self.netwerktool.selected_hydrovak.hydrovak.update({
                'kijkp_breedte': width,
                'kijkp_diepte': depth,
                'kijkp_talud': talud,
                'kijkp_reden': reason,
            })
            # for node in self.netwerktool.selected_hydrovak.hydrovak:
            #     node.hydrovak.update({
            #         'kijkp_breedte': width,
            #         'kijkp_diepte': depth,
            #         'kijkp_talud': talud,
            #         'kijkp_reden': reason,
            #     })

            session.commit()

    def save(self):
        self.save_to_db(
            self.input_width.value(),
            self.input_depth.value(),
            self.input_talud.value(),
            self.input_reason.currentText()
        )
        self.close()

    def close(self):
        self.netwerktool.init_width = self.input_width.value()
        self.netwerktool.init_depth = self.input_depth.value()
        self.netwerktool.init_talud = self.input_talud.value()
        self.netwerktool.init_reason = self.input_reason.currentText()
        super().close()

    def clear(self):
        self.save_to_db(
            None,
            None,
            None,
            None
        )
        super().close()

    def closeEvent(self, event):
        """

        :
        return:
        """
        self.closingDialog.emit()
        self.close()
        event.accept()

    def setup_ui(self):
        self.setMinimumWidth(300)

        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.verticalLayout.setObjectName("verticalLayout")

        # form
        self.form_row = QtWidgets.QGridLayout(self)

        self.input_width = QtWidgets.QDoubleSpinBox(self)
        self.input_width.setSuffix(" m")
        self.input_width.setSingleStep(0.1)
        self.input_width.setObjectName("Invoer_waterbreedte")
        lbl = QtWidgets.QLabel(self)
        lbl.setText("Waterbreedte")
        self.form_row.addWidget(lbl, 0, 0)
        self.form_row.addWidget(self.input_width, 0, 1)

        self.input_depth = QtWidgets.QDoubleSpinBox(self)
        self.input_depth.setSuffix(" m")
        self.input_depth.setSingleStep(0.1)
        self.input_depth.setObjectName("Invoer_diepte")
        lbl = QtWidgets.QLabel(self)
        lbl.setText("Diepte")
        self.form_row.addWidget(lbl, 1, 0)
        self.form_row.addWidget(self.input_depth, 1, 1)

        self.input_talud = QtWidgets.QDoubleSpinBox(self)
        self.input_talud.setSuffix(" b:h")
        self.input_talud.setSingleStep(0.1)
        self.input_talud.setObjectName("Invoer_talud")
        lbl = QtWidgets.QLabel(self)
        lbl.setText("Talud")
        self.form_row.addWidget(lbl, 2, 0)
        self.form_row.addWidget(self.input_talud, 2, 1)

        self.input_reason = QtWidgets.QComboBox(self)
        self.input_reason.setObjectName("Invoer_reden")
        lbl = QtWidgets.QLabel(self)
        lbl.setText("Reden")
        self.form_row.addWidget(lbl, 3, 0)
        self.form_row.addWidget(self.input_reason, 3, 1)

        self.verticalLayout.addLayout(self.form_row)
        # bottom
        self.bottom_row = QtWidgets.QHBoxLayout()
        self.bottom_row.setObjectName("Bottom row")

        self.clear_button = QtWidgets.QPushButton(self)
        self.clear_button.setObjectName("Close")
        self.clear_button.clicked.connect(self.clear)
        self.bottom_row.addWidget(self.clear_button)

        self.verticalLayout.addLayout(self.bottom_row)

        self.cancel_button = QtWidgets.QPushButton(self)
        self.cancel_button.setObjectName("Close")
        self.cancel_button.clicked.connect(self.close)
        self.bottom_row.addWidget(self.cancel_button)

        self.verticalLayout.addLayout(self.bottom_row)

        self.save_button = QtWidgets.QPushButton(self)
        self.save_button.setObjectName("Save")
        self.save_button.clicked.connect(self.save)
        self.bottom_row.addWidget(self.save_button)

        self.retranslateUi(self)
        QtCore.QMetaObject.connectSlotsByName(self)

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(_translate("Dialog", "Definieer brede kijkprofiel", None))
        self.cancel_button.setText(_translate("Dialog", "Sluiten", None))
        self.save_button.setText(_translate("Dialog", "Opslaan", None))
        self.clear_button.setText(_translate("Dialog", "Maak leeg", None))
