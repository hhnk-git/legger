# -*- coding: utf-8 -*-
from __future__ import division

import logging
import os
from sqlalchemy.sql import text
from qgis.PyQt import QtCore, QtGui, QtWidgets
from qgis.PyQt.QtCore import QSettings, pyqtSignal
from qgis.PyQt.QtWidgets import QFileDialog, QWidget
from legger.sql_models.legger_views import create_legger_views
from legger.utils.read_data_and_make_leggerdatabase import CreateLeggerSpatialite
from legger.utils.user_message import messagebar_message
import sqlite3
from legger.sql_models.legger_database import load_spatialite

log = logging.getLogger(__name__)


try:
    _encoding = QtWidgets.QApplication.UnicodeUTF8


    def _translate(context, text, disambig):
        return QtWidgets.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtWidgets.QApplication.translate(context, text, disambig)


class PolderSelectionWidget(QWidget):  # , FORM_CLASS):
    """Dialog for selecting model (spatialite and result files netCDFs)"""
    closingDialog = pyqtSignal()

    def __init__(
            self, parent, iface, parent_class, root_tool):
        """Constructor

        :parent: Qt parent Widget
        :iface: QGiS interface
        :polder_datasource: Polder spatialite instance
        :parent_class: the tool class which instantiated this widget. Is used
             here for storing volatile information
        """
        super(PolderSelectionWidget, self).__init__(parent)

        self.parent_class = parent_class
        self.iface = iface
        self.setup_ui()

        self.root_tool = root_tool  # "root tool meegeven aan nieuw scherm. Verwijzing naar een class, i.p.v. een nieuwe variabele"

        self.var_text_DAMO.setText("...")
        self.var_text_leggerdatabase.setText(
            self.root_tool.polder_datasource)  # De tekst verwijst naar de tekst in de root_tool totdat deze geupdated wordt.

    def closeEvent(self, event):
        """

        :return:
        """
        self.closingDialog.emit()
        self.close()
        event.accept()

    def explain_leggerdatabase(self):
        self.msg_upper_row = QtWidgets.QMessageBox(self)
        self.msg_upper_row.setIcon(QtWidgets.QMessageBox.Information)
        self.msg_upper_row.setText("<b>Het selecteren van een leggerdatabase<b>")
        self.msg_upper_row.setInformativeText(
            "Voor de toewijzing van leggerprofielen wordt een aparte 'leggerdatabase' "
            "gemaakt. Deze database is een aparte .sqlite bestand waar data uit "
            "DAMO en 3Di modelresultaat zijn gecombineerd "
            "voor de leggerprofielen, zoals breedte en talud per hydro-object.\n"
            "Wanneer een nieuwe leggerdatabase gemaakt moet worden, selecteer dan bij "
            "voorkeur de DAMO die ook voor de opbouw van het 3di model zijn "
            "gebruikt.\n"
            "Is er al een 'leggerdatabase' aangemaakt, sla deze stap dan over en zorg "
            "dat dit bestand (met als extentie .sqlite) in de tweede stap geselecteerd "
            "wordt. Let wel op: opnieuw uitgevoerde stappen en leggerkeuzes zullen "
            "bestaande data overschrijven.")
        self.box_explanation.addWidget(self.msg_upper_row)

    def select_DAMO(self):
        """
        Select a dump or export of the DAMO database (.gdb)
        :return:
        """
        settings = QSettings('leggertool', 'filepaths')
        try:
            init_path = settings.value('last_used_DAMO_path', type=str)
        except:
            init_path = os.path.expanduser("~")  # get path to respectively "user" folder

        DAMO_datasource = QFileDialog.getExistingDirectory(
            self,
            'Selecteer DAMO FileGeoDatabase (.gdb) van polder',
            init_path
        )

        if DAMO_datasource:
            self.var_text_DAMO.setText(DAMO_datasource)
            settings.setValue('last_used_DAMO_path',
                              os.path.dirname(DAMO_datasource))
        return

    def select_spatialite(self):
        """
        Open file dialog on click on button 'load
        :return: Boolean, if file is selected
        """
        settings = QSettings('leggertool', 'filepaths')
        try:
            init_path = settings.value('last_used_legger_spatialite_path', type=str)
        except:
            init_path = os.path.expanduser("~")  # get path to respectively "user" folder

        database = QFileDialog.getOpenFileName(
            self,
            'Selecteer leggerdatabase',
            init_path,
            'Spatialite (*.sqlite)'
        )

        if database:
            database = database[0]
            self.root_tool.polder_datasource = database
            self.var_text_leggerdatabase.setText(database)

            settings.setValue('last_used_legger_spatialite_path',
                              os.path.dirname(database))
        return

    def create_spatialite_database(self):
        # todo:
        #  feedback if input is incorrect.

        settings = QSettings('leggertool', 'filepaths')
        try:
            init_path = settings.value('last_used_legger_spatialite_path', type=str)
        except:
            init_path = os.path.expanduser("~")  # get path to respectively "user" folder

        database = QFileDialog.getSaveFileName(
            self,
            'Selecteer bestandslocatie voor nieuwe leggerdatabase',
            init_path,
            'Spatialite (*.sqlite)'
        )

        if not database:
            return

        database = database[0]
        self.var_text_leggerdatabase.setText(database)

        settings.setValue('last_used_legger_spatialite_path',
                          os.path.dirname(database))

        database_path = self.var_text_leggerdatabase.text()
        filepath_DAMO = self.var_text_DAMO.text()

        if not os.path.exists(filepath_DAMO):
            messagebar_message(
                'Aanmaken leggerdatabase mislukt',
                'Opgegeven DAMO database bestaat niet',
                level=2,
                duration=10)

            raise Exception('Geselecteerde DAMO database mist')

        legger_class = CreateLeggerSpatialite(
            filepath_DAMO,
            database_path,
        )

        legger_class.full_import_ogr2ogr()

        # set root_tool as last, because this triggers other actions
        self.root_tool.polder_datasource = database

        # create views
        con_legger = load_spatialite(self.root_tool.polder_datasource)

        create_legger_views(con_legger)

        messagebar_message(
            'Aanmaken leggerdatabase',
            'Aanmaken leggerdatabase is gelukt',
            level=3,
            duration=10)

    def setup_ui(self):
        self.setMinimumWidth(700)

        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.verticalLayout.setObjectName("verticalLayout")

        self.explanation_row = QtWidgets.QVBoxLayout()
        self.explanation_row.setObjectName("Uitleg row")
        self.maak_leggerdatabase_row = QtWidgets.QVBoxLayout()
        self.maak_leggerdatabase_row.setObjectName("Maak leggerdatabase row")
        self.kies_leggerdatabase_row = QtWidgets.QVBoxLayout()
        self.kies_leggerdatabase_row.setObjectName("Kies leggerdatabase row")
        self.bottom_row = QtWidgets.QHBoxLayout()
        self.bottom_row.setObjectName("Bottom row")

        # connect signals
        # select input files for the creation of legger database
        self.load_DAMO_dump_button = QtWidgets.QPushButton(self)
        self.load_DAMO_dump_button.setObjectName("Load DAMO")
        self.load_DAMO_dump_button.clicked.connect(self.select_DAMO)
        self.load_DAMO_dump_button.setMinimumWidth(190)

        self.explanation_button = QtWidgets.QPushButton(self)
        self.explanation_button.setObjectName("Explain")
        self.explanation_button.clicked.connect(self.explain_leggerdatabase)

        # make database routine
        self.create_leggerdatabase_button = QtWidgets.QPushButton(self)
        self.create_leggerdatabase_button.setObjectName("Create database")
        self.create_leggerdatabase_button.clicked.connect(self.create_spatialite_database)

        # select legger database
        self.load_leggerdatabase_button = QtWidgets.QPushButton(self)
        self.load_leggerdatabase_button.setObjectName("Load")
        self.load_leggerdatabase_button.clicked.connect(self.select_spatialite)
        self.load_leggerdatabase_button.setMinimumWidth(190)

        # close screen
        self.cancel_button = QtWidgets.QPushButton(self)
        self.cancel_button.setObjectName("Close")
        self.cancel_button.clicked.connect(self.close)
        self.bottom_row.addWidget(self.cancel_button)

        # Feedback what document is chosen
        self.var_text_DAMO = QtWidgets.QLineEdit(self)
        self.var_text_DAMO.setText("leeg")

        self.var_text_leggerdatabase = QtWidgets.QLineEdit(self)
        self.var_text_leggerdatabase.setText("leeg")

        # Assembling
        # explanation button pop-up row
        self.box_explanation = QtWidgets.QVBoxLayout()
        self.box_explanation.addWidget(self.explanation_button)

        # Create buttons with functions to select damo and database creation and add it to rows
        self.box_leggerdatabase_create = QtWidgets.QVBoxLayout()
        self.hbox_DAMO = QtWidgets.QHBoxLayout()
        self.hbox_DAMO.addWidget(self.var_text_DAMO)
        self.hbox_DAMO.addWidget(self.load_DAMO_dump_button)
        self.box_leggerdatabase_create.addLayout(self.hbox_DAMO)

        self.box_leggerdatabase_create.addWidget(self.create_leggerdatabase_button)

        self.box_leggerdatabase_input = QtWidgets.QVBoxLayout()
        self.hbox_LDB = QtWidgets.QHBoxLayout()
        self.hbox_LDB.addWidget(self.var_text_leggerdatabase)
        self.hbox_LDB.addWidget(self.load_leggerdatabase_button)
        self.box_leggerdatabase_input.addLayout(self.hbox_LDB)

        # Create groupbox and add H or VBoxes to it
        self.groupBox_explanation = QtWidgets.QGroupBox(self)
        self.groupBox_explanation.setTitle("Uitleg")
        self.groupBox_explanation.setLayout(self.box_explanation)

        self.groupBox_leggerdatabase_create = QtWidgets.QGroupBox(self)
        self.groupBox_leggerdatabase_create.setTitle("GDB bestanden voor aanmaken leggertool sqlite")
        self.groupBox_leggerdatabase_create.setLayout(self.box_leggerdatabase_create)

        self.groupBox_leggerdatabase_input = QtWidgets.QGroupBox(self)
        self.groupBox_leggerdatabase_input.setTitle("Leggertool sqlite kiezen als deze al bestaat")
        self.groupBox_leggerdatabase_input.setLayout(self.box_leggerdatabase_input)

        # Add groupbox to row
        self.explanation_row.addWidget(self.groupBox_explanation)
        self.maak_leggerdatabase_row.addWidget(self.groupBox_leggerdatabase_create)
        self.kies_leggerdatabase_row.addWidget(self.groupBox_leggerdatabase_input)

        # Add row to ui
        self.verticalLayout.addLayout(self.explanation_row)
        self.verticalLayout.addLayout(self.maak_leggerdatabase_row)
        self.verticalLayout.addLayout(self.kies_leggerdatabase_row)
        self.verticalLayout.addLayout(self.bottom_row)

        self.retranslateUi(self)
        QtCore.QMetaObject.connectSlotsByName(self)

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(_translate("Dialog", "Selecteer en/of maak de leggerdatabase van de polder", None))
        self.explanation_button.setText(_translate("Dialog", "Klik hier voor meer uitleg", None))
        self.load_DAMO_dump_button.setText(_translate("Dialog", "Selecteer FileGeoDatabase (.gdb) van polder", None))
        self.create_leggerdatabase_button.setText(_translate("Dialog", "Aanmaken leggertool sqlite", None))
        self.load_leggerdatabase_button.setText(_translate("Dialog", "Selecteer leggertool sqlite", None))
        self.cancel_button.setText(_translate("Dialog", "Sluiten", None))
