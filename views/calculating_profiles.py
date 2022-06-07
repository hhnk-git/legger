# todo:
#  - 'opstuwingsnorm' selection?
#  - correct or selectable friction values


# -*- coding: utf-8 -*-

import logging
import time

from legger.sql_models.legger import BegroeiingsVariant, HydroObject
from legger.sql_models.legger_database import LeggerDatabase
from legger.sql_models.legger_database import load_spatialite
from legger.utils.calc_gradient import calc_gradient
from legger.utils.profile_match_a import doe_profinprof, maaktabellen
from legger.utils.read_tdi_results import (get_timestamps, read_tdi_culvert_results, read_tdi_results,
                                           write_tdi_culvert_results_to_db, write_tdi_results_to_db)
from legger.utils.redirect_flows_to_main_branches import redirect_flows
from legger.utils.snap_points import snap_points
from legger.utils.theoretical_profiles import create_theoretical_profiles, write_theoretical_profile_results_to_db
from qgis.PyQt import QtCore, QtWidgets
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import QComboBox, QWidget

log = logging.getLogger(__name__)


class ProfileCalculationWidget(QWidget):  # , FORM_CLASS):
    """Dialog for making the pre-process steps for the Legger"""
    closingDialog = pyqtSignal()

    def __init__(
            self, parent, iface, polder_datasource, ts_datasource,
            parent_class):
        """Constructor

        parent (QtWidget): Qt parent Widget
        iface (QgisInterface: QGiS interface
        polder_datasource (str): Path to the 'legger' spatialite of a polder
        ts_datasource (TimeseriesDatasourceModel): 3di datasource of results
        parent_class: the tool class which instantiated this widget. Is used
             here for storing volatile information
        returns: None
        """
        super(ProfileCalculationWidget, self).__init__(parent)

        self.parent_class = parent_class
        self.iface = iface
        self.polder_datasource = polder_datasource
        self.ts_datasource = ts_datasource
        self.timestep = -1
        self.surge_selection = -1

        errormessage = "Kies eerst 3di output (model en netCDF) in de 'Select 3di results' van 3di plugin."
        try:
            self.path_model_db = self.ts_datasource.model_spatialite_filepath
        except:
            self.path_model_db = errormessage

        try:

            self.path_result_db = self.ts_datasource.rows[0].datasource_layer_helper.sqlite_gridadmin_filepath.replace('\\', '/')

        except:
            self.path_result_db = errormessage

        try:
            self.path_result_nc = self.ts_datasource.rows[0].file_path.value
        except:
            self.path_result_nc = errormessage

        if self.path_model_db is None:
            self.path_model_db = errormessage

        # timestep combobox
        self.last_timestep_text = 'laatste tijdstap'
        self.timestamps = []

        try:
            self.timestamps = get_timestamps(self.path_result_nc)
        except:
            pass

        self.setup_ui()

        # fill timestep combobox
        choices = [self.last_timestep_text] + ['%.0f' % t for t in self.timestamps]
        self.timestep_combo_box.insertItems(0, choices)
        self.timestep_combo_box.setCurrentIndex(0)

        # set time combobox listeners
        self.timestep_combo_box.currentIndexChanged.connect(
            self.timestep_selection_change)

        # surge combobox
        self.last_surge_text = "kies opstuwingsnorm"

        # fill surge combobox
        surge_choices = ['%s' % s for s in [1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]]
        self.surge_combo_box.insertItems(0, surge_choices)
        self.surge_combo_box.setCurrentIndex(3)
        self.surge_combo_inlaat_box.insertItems(0, surge_choices)
        self.surge_combo_inlaat_box.setCurrentIndex(3)

    def timestep_selection_change(self, nr):
        """Proces new selected timestep in combobox

        :param nr:
        :return:
        """
        text = self.timestep_combo_box.currentText()
        if text == self.last_timestep_text:
            self.timestep = -1
        else:
            self.timestep = nr - 1

    def closeEvent(self, event):
        """
        event (QtEvent): event triggering close
        returns: None
        """
        # set listeners
        self.timestep_combo_box.currentIndexChanged.disconnect(
            self.timestep_selection_change)

        self.closingDialog.emit()
        self.close()
        event.accept()

    def save_spatialite(self):
        """Change active modelsource. Called by combobox when selected
        spatialite changed
        returns: None
        """

        self.close()

    def explain_step_old_3di(self):
        """
        Uitleg van stap 3si
        """
        # detailed information on UPPER ROW groupbox
        self.msg_upper_row = QtWidgets.QMessageBox(self)
        self.msg_upper_row.setIcon(QtWidgets.QMessageBox.Information)
        self.msg_upper_row.setText("<b>Het selecteren van een tijdstap voor de leggerdatabase<b>")
        self.msg_upper_row.setInformativeText("In het netCDF bestand waar de 3di resultaten zijn opgeslagen is per "
                                              "'flowline' voor elke tijdstap informatie beschikbaar. Dit betekent dat "
                                              "eerst een tijdstap gekozen moet worden om de resultaten van deze tijdstap "
                                              "op te kunnen halen.\n"
                                              "In het geval van de hydraulische toets, wat gebruikt wordt voor de legger, "
                                              "zijn we geinteresseerd in de debieten over de 'flowlines' waarbij "
                                              "de neerslag som een stationair evenwicht heeft berijkt.\n\n"
                                              "Tip: In de BWN studie rekenen we een som door van 1 dag droog, 5 "
                                              "dagen regen en weer 2 dagen droog. Voor het meest stationaire moment "
                                              "selecteer de tijdstap aan het einde van de bui. Standaard is dit de"
                                              "tijdstap net onder de waarde 518400, maar kan afwijken. De slider van"
                                              "de tijdstappen moet tussen ongeveer 2/3 en 3/4 van de grootste tijdstap")

        self.box_step3diold.addWidget(self.msg_upper_row)

    def execute_step_old_3di(self):
        """
       Eerste stap in klaarzetten van de data:
        - update legger database met data vanuit de netcdf (koppeling debieten van gekozen tijdstap)
        returns: None
        """
        # first update legger spatialite according to schema, using sqlAlchemy
        db = LeggerDatabase(
            {
                'db_path': self.polder_datasource
            },
            'spatialite'
        )
        # db.create_and_check_fields()
        # do one query, don't know what the reason was for this...
        session = db.get_session()
        session.query(HydroObject)

        timestamp = 'laatst' if self.timestep == 0 else '{0} op {1:.0f} s'.format(
            self.timestep + 1,
            self.timestamps[self.timestep])

        self.feedbackmessage = 'Neem tijdstap {0}'.format(timestamp)

        result = None
        if True:
            # read 3di channel results
            result = read_tdi_results(
                self.path_model_db,
                self.path_result_db,
                self.path_result_nc,
                self.polder_datasource,
                self.timestep
            )
            self.feedbackmessage += "\nDatabases zijn gekoppeld."

        # except Exception as e:
        #     self.feedbackmessage += "\nDatabases zijn niet gekoppeld. melding: {0}\n".format(e)
        # finally:
        #     self.feedbacktext.setText(self.feedbackmessage)

        if result is not None:
            try:
                # write 3di channel result to legger spatialite
                write_tdi_results_to_db(
                    result,
                    self.polder_datasource)

                self.feedbackmessage = self.feedbackmessage + "\n3Di resultaten weggeschreven naar polder database."
            except:
                self.feedbackmessage = self.feedbackmessage + "\nFout in wegschrijven 3Di resultaten naar polder database."
            finally:
                self.feedbacktext.setText(self.feedbackmessage)

        culv_results = None
        try:
            # read 3di culvert results
            culv_results = read_tdi_culvert_results(
                self.path_model_db,
                self.path_result_db,
                self.path_result_nc,
                self.polder_datasource,
                self.timestep
            )
            self.feedbackmessage = self.feedbackmessage + "\n3Di culverts ingelezen."
        except:
            self.feedbackmessage = self.feedbackmessage + "\nFout, 3Di culverts niet ingelezen."
        finally:
            self.feedbacktext.setText(self.feedbackmessage)

        if culv_results is not None:
            try:
                # write 3di culvert results to legger spatialite
                write_tdi_culvert_results_to_db(culv_results, self.polder_datasource)
                self.feedbackmessage = self.feedbackmessage + "\n3Di culvert resultaten weggeschreven."
            except:
                self.feedbackmessage = self.feedbackmessage + "\nFout, 3Di culvert resultaten niet weggeschreven."
            finally:
                self.feedbacktext.setText(self.feedbackmessage)

    def execute_redirect_flows(self):

        redirect_flows(self.iface, self.polder_datasource)
        self.feedbacktext.setText("Debieten zijn aangepast.")

    def explain_step2(self):
        """
        Uitleg van stap 1
        """
        # detailed information on UPPER ROW groupbox
        self.msg_middle_row = QtWidgets.QMessageBox(self)
        self.msg_middle_row.setIcon(QtWidgets.QMessageBox.Information)
        self.msg_middle_row.setText("<b>Het berekenen van de varianten voor de leggerdatabase<b>")
        self.msg_middle_row.setInformativeText("Alle randvoorwaarden zijn nu bekend:\n"
                                               "breedte, diepte, Q, talud.\n"
                                               "Met de gekozen norm voor verhang wordt met iteraties berekend "
                                               "welke mogelijke leggerprofielen er mogelijk zijn. Dit betekent dat "
                                               "per hydro-object idealiter een hele lijst van 'mogelijke profielen' "
                                               "wordt berekend. Dat betekent dat er vanuit hydraulisch oogpunt dus "
                                               "ruimte is voor keuze.\n"
                                               "Let op: als een bestaande 'leggerdatabase' is ingelezen, dan kan het "
                                               "zo zijn dat door deze actie uit te voeren bestaande varianten worden "
                                               "overschreven met nieuwe (omdat randvoorwaarde verhang nu anders is.")

        self.box_step2.addWidget(self.msg_middle_row)

    def execute_step2(self):

        db = LeggerDatabase(
            {
                'db_path': self.polder_datasource
            },
            'spatialite'
        )
        db.create_and_check_fields()
        # do one query, don't know what the reason was for this...
        session = db.get_session()

        # instance = session.query(BegroeiingsVariant).filter_by(naam='basis').first()

        # session.query('Select * FROM varianten')

        opstuw_norm = float(self.surge_combo_box.currentText())
        opstuw_norm_inlaat = float(self.surge_combo_inlaat_box.currentText())

        for bv in session.query(BegroeiingsVariant).all():

            try:
                profiles = create_theoretical_profiles(self.polder_datasource, opstuw_norm, opstuw_norm_inlaat, bv)
                self.feedbackmessage = "Profielen zijn berekend."
            except Exception as e:
                log.error(e)
                self.feedbackmessage = "Fout, profielen konden niet worden berekend."
            finally:
                self.feedbacktext.setText(self.feedbackmessage)

            try:
                write_theoretical_profile_results_to_db(session, profiles, opstuw_norm, bv)
                self.feedbackmessage = self.feedbackmessage + ("\nProfielen opgeslagen in legger db.")

            except Exception as e:
                log.error(e)
                self.feedbackmessage = self.feedbackmessage + ("\nFout, profielen niet opgeslagen in legger database. melding: {}")
            finally:
                self.feedbacktext.setText(self.feedbackmessage)

    def execute_step3(self):

        con_legger = load_spatialite(self.polder_datasource)

        maaktabellen(con_legger.cursor())
        con_legger.commit()
        resultaat = doe_profinprof(con_legger.cursor(), con_legger.cursor())
        con_legger.commit()
        self.feedbacktext.setText(resultaat)
        self.feedbacktext.setText("De fit % zijn berekend.")

    def execute_snap_points(self):
        con_legger = load_spatialite(self.polder_datasource)

        snap_points(con_legger.cursor())

        self.feedbacktext.setText("De punten zijn gesnapt.")

    def execute_pre_fill(self):
        self.feedbacktext.setText("Waarschuwing: nog niet geimplementeerd")

    def run_all(self):
        self.execute_snap_points()
        time.sleep(1)
        self.execute_redirect_flows()
        time.sleep(1)
        self.execute_step2()
        time.sleep(1)
        self.execute_step3()
        self.feedbacktext.setText("Alle taken uitgevoerd.")

    def post_process(self):
        calc_gradient(self.iface, self.polder_datasource)
        self.feedbacktext.setText("Totaal gradient berekend")

    def setup_ui(self):
        self.verticalLayout = QtWidgets.QVBoxLayout(self)
        self.verticalLayout.setObjectName("verticalLayout")

        self.information_row = QtWidgets.QHBoxLayout()
        self.information_row.setObjectName("Information row")
        self.bottom_row = QtWidgets.QHBoxLayout()
        self.bottom_row.setObjectName("Bottom row")
        self.feedback_row = QtWidgets.QHBoxLayout()
        self.feedback_row.setObjectName("Feedback row")
        self.exit_row = QtWidgets.QHBoxLayout()
        self.exit_row.setObjectName("Exit row")

        # Selected file name and location in INFORMATION ROW groupbox
        self.polder_filename = QtWidgets.QLineEdit(self)
        self.polder_filename.setText(self.polder_datasource)
        self.polder_filename.setObjectName("polder legger filename")

        # Assembling INFORMATION ROW groubox
        self.box_info = QtWidgets.QVBoxLayout()
        self.box_info.addWidget(self.polder_filename)  # intro text toevoegen aan box.

        self.groupBox_info = QtWidgets.QGroupBox(self)
        self.groupBox_info.setTitle("Bestanden gekozen:")
        self.groupBox_info.setLayout(self.box_info)  # box toevoegen aan groupbox
        self.information_row.addWidget(self.groupBox_info)

        # Assembling step 1 - snap_points
        self.snap_points_button = QtWidgets.QPushButton(self)
        self.snap_points_button.setObjectName("Snap points")
        self.snap_points_button.clicked.connect(self.execute_snap_points)
        self.groupBox_snap_points = QtWidgets.QGroupBox(self)
        self.groupBox_snap_points.setTitle("Stap 1: snap eindpunten")
        self.box_snap_points = QtWidgets.QHBoxLayout()
        self.box_snap_points.addWidget(self.snap_points_button)
        self.groupBox_snap_points.setLayout(self.box_snap_points)  # box toevoegen aan groupbox

        # Assembling step 2 - redirect_flows
        self.step_redirect_flow_button = QtWidgets.QPushButton(self)
        self.step_redirect_flow_button.setObjectName("redirect_flow")
        self.step_redirect_flow_button.clicked.connect(self.execute_redirect_flows)

        self.groupBox_step_redirect_flows = QtWidgets.QGroupBox(self)
        self.groupBox_step_redirect_flows.setTitle("stap 2: kies eindpunten en daarna herverdeel 3di debiet")
        self.box_step_redirect_flows = QtWidgets.QVBoxLayout()
        self.box_step_redirect_flows.addWidget(self.step_redirect_flow_button)
        self.groupBox_step_redirect_flows.setLayout(self.box_step_redirect_flows)  # box toevoegen aan groupbox

        # surge selection:
        self.form_row = QtWidgets.QGridLayout(self)
        self.surge_combo_box = QComboBox(self)
        self.surge_combo_inlaat_box = QComboBox(self)

        # Assembling step 2 row
        self.step2_button = QtWidgets.QPushButton(self)
        self.step2_button.setObjectName("stap2")
        self.step2_button.clicked.connect(self.execute_step2)
        self.step2_explanation_button = QtWidgets.QPushButton(self)
        self.step2_explanation_button.setObjectName("uitleg_stap2")
        self.step2_explanation_button.clicked.connect(self.explain_step2)

        self.groupBox_step2 = QtWidgets.QGroupBox(self)
        self.groupBox_step2.setTitle("Stap 3: bereken profielvarianten")
        self.box_step2 = QtWidgets.QVBoxLayout()
        self.box_step2.addLayout(self.form_row)
        lbl = QtWidgets.QLabel(self)
        lbl.setText("Max verhang afvoer [cm/km]")
        self.form_row.addWidget(lbl, 0, 0)
        self.form_row.addWidget(self.surge_combo_box, 0, 1)
        lbl = QtWidgets.QLabel(self)
        lbl.setText("Max verhang inlaat [cm/km]")
        self.form_row.addWidget(lbl, 1, 0)
        self.form_row.addWidget(self.surge_combo_inlaat_box, 1, 1)

        self.box_step2.addWidget(self.step2_button)
        self.box_step2.addWidget(self.step2_explanation_button)
        self.groupBox_step2.setLayout(self.box_step2)  # box toevoegen aan groupbox

        # Assembling step 3 row
        self.step3_button = QtWidgets.QPushButton(self)
        self.step3_button.setObjectName("Stap 3")
        self.step3_button.clicked.connect(self.execute_step3)
        self.groupBox_step3 = QtWidgets.QGroupBox(self)
        self.groupBox_step3.setTitle("Stap 4: bepaal score per variant")
        self.box_step3 = QtWidgets.QHBoxLayout()
        self.box_step3.addWidget(self.step3_button)
        self.groupBox_step3.setLayout(self.box_step3)  # box toevoegen aan groupbox
        self.bottom_row.addWidget(self.groupBox_step3)

        # Assembling step 5 row
        self.pre_fill_button = QtWidgets.QPushButton(self)
        self.pre_fill_button.setObjectName("pre fill profiles")
        self.pre_fill_button.clicked.connect(self.execute_pre_fill)
        self.groupBox_pre_fill = QtWidgets.QGroupBox(self)
        self.groupBox_pre_fill.setTitle("Stap 5: invullen waar evident	")
        self.box_pre_fill = QtWidgets.QHBoxLayout()
        self.box_pre_fill.addWidget(self.pre_fill_button)
        self.groupBox_pre_fill.setLayout(self.box_pre_fill)  # box toevoegen aan groupbox
        self.bottom_row.addWidget(self.groupBox_pre_fill)

        # Assembling run all
        self.run_all_button = QtWidgets.QPushButton(self)
        self.run_all_button.setObjectName("pre fill profiles")
        self.run_all_button.clicked.connect(self.run_all)

        self.groupBox_run_all = QtWidgets.QGroupBox(self)
        self.groupBox_run_all.setTitle("Run alle")
        self.box_run_all = QtWidgets.QHBoxLayout()

        self.box_run_all.addWidget(self.run_all_button)
        self.groupBox_run_all.setLayout(self.box_run_all)  # box toevoegen aan groupbox

                # Assembling run all
        self.run_post_process_button = QtWidgets.QPushButton(self)
        self.run_post_process_button.clicked.connect(self.post_process)
        self.groupBox_post_process = QtWidgets.QGroupBox(self)
        self.groupBox_post_process.setTitle("Naverwerking")
        self.box_run_post_process = QtWidgets.QHBoxLayout()
        self.box_run_post_process.addWidget(self.run_post_process_button)
        self.groupBox_post_process.setLayout(self.box_run_post_process)  # box toevoegen aan groupbox
        
        # old 3di
        # timestep selection for UPPER ROW groupbox:
        self.timestep_combo_box = QComboBox(self)

        self.step3di_button = QtWidgets.QPushButton(self)
        self.step3di_button.setObjectName("step3di_button")
        self.step3di_button.clicked.connect(self.execute_step_old_3di)

        self.step3diold_explanation_button = QtWidgets.QPushButton(self)
        self.step3diold_explanation_button.setObjectName("uitleg_stap1")
        self.step3diold_explanation_button.clicked.connect(self.explain_step_old_3di)

        self.groupBox_step3diold = QtWidgets.QGroupBox(self)
        self.groupBox_step3diold.setTitle("oude functies: lees 3di resultaten")
        self.box_step3diold = QtWidgets.QVBoxLayout()

        self.model_filename = QtWidgets.QLineEdit(self)
        self.model_filename.setText(self.path_model_db)
        self.model_filename.setObjectName("model filename")
        self.result_filename = QtWidgets.QLineEdit(self)
        self.result_filename.setText(self.path_result_nc)
        self.result_filename.setObjectName("result filename")
        self.connection_filename = QtWidgets.QLineEdit(self)
        self.connection_filename.setText(self.path_result_db)
        self.connection_filename.setObjectName("connection filename")
        self.box_step3diold.addWidget(self.model_filename)
        self.box_step3diold.addWidget(self.result_filename)
        self.box_step3diold.addWidget(self.connection_filename)

        self.box_step3diold.addWidget(self.timestep_combo_box)
        self.box_step3diold.addWidget(self.step3di_button)
        self.box_step3diold.addWidget(self.step3diold_explanation_button)
        self.groupBox_step3diold.setLayout(self.box_step3diold)  # box toevoegen aan groupbox

        # Assembling feedback row
        self.feedbacktext = QtWidgets.QTextEdit(self)
        self.feedbackmessage = "Nog geen berekening uitgevoerd"
        self.feedbacktext.setText(self.feedbackmessage)
        self.feedbacktext.setObjectName("feedback")

        self.feedback_row.addWidget(self.feedbacktext)

        # Assembling exit row
        self.cancel_button = QtWidgets.QPushButton(self)
        self.cancel_button.setObjectName("Cancel")
        self.cancel_button.clicked.connect(self.close)
        self.exit_row.addWidget(self.cancel_button)

        self.save_button = QtWidgets.QPushButton(self)
        self.save_button.setObjectName("Close")
        self.save_button.clicked.connect(self.save_spatialite)
        self.exit_row.addWidget(self.save_button)

        # Lay-out in elkaar zetten.
        self.verticalLayout.addLayout(self.information_row)
        self.verticalLayout.addWidget(self.groupBox_snap_points)
        self.verticalLayout.addWidget(self.groupBox_step_redirect_flows)
        self.verticalLayout.addWidget(self.groupBox_step2)
        self.verticalLayout.addLayout(self.bottom_row)
        self.verticalLayout.addWidget(self.groupBox_run_all)
        self.verticalLayout.addWidget(self.groupBox_post_process)
        self.verticalLayout.addWidget(self.groupBox_step3diold)
        self.verticalLayout.addLayout(self.feedback_row)
        self.verticalLayout.addLayout(self.exit_row)

        self.retranslateUi(self)
        QtCore.QMetaObject.connectSlotsByName(self)

    def retranslateUi(self, Dialog):

        Dialog.setWindowTitle("Bereken de profielvarianten van de polder")
        self.save_button.setText("Opslaan en sluiten")
        self.step3diold_explanation_button.setText("Uitleg stap inlezen 3di netCDF")
        self.step3di_button.setText("Verbindt resultaten van netCDF aan de hydro-objecten")
        self.step_redirect_flow_button.setText("Kies eerst eindpunten, dan herverdeel debieten")
        self.step2_explanation_button.setText("Uitleg stap 3")
        self.step2_button.setText("Bereken alle mogelijke leggerprofielen")
        self.step3_button.setText("Bereken de fit van de berekende profielen")
        self.snap_points_button.setText("Snap eindpunten van lijnen")

        self.pre_fill_button.setText("Vul profielen in waar duidelijk")
        self.run_all_button.setText("Run alle taken achter elkaar")
        self.run_post_process_button.setText("Verhang op basis van gekozen legger")

        self.cancel_button.setText("Cancel")
