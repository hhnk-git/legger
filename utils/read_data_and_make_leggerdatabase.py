# coding: utf-8
# todo:
#  X DuikerSifonHevel
#  X objectid vullen --> verwijderd
#  X verwijderen imp tabellen
#  X oplossing verzinnen voor geo_alchemy fix
#  - grondsoort? --> voorlopig niet
#  X geometry colom vastzetten (op 'geometry')
#  - logging van ogr2ogr terugsluizen naar logging
#  - melding of proces gelukt is en verwijzen naar logging indien mislukt
from sqlalchemy.sql import text
import datetime
import logging
import os
import os.path
import subprocess
import sys

from legger.sql_models.legger_database import LeggerDatabase

log = logging.getLogger(__name__)


def nonfloat(value, default_value=None):
    if value is None:
        return default_value
    try:
        return float(value)
    except ValueError:
        return default_value


def nonint(value, default_value=None):
    if value is None:
        return default_value
    try:
        return int(value)
    except ValueError:
        return default_value


def nonwkt(value, default_value=None):
    if value is None:
        return default_value
    try:
        return 'SRID=28992;' + value.wkt
    except ValueError:
        return default_value


class CreateLeggerSpatialite(object):
    """
        Select "dump" or "export" .gdb files that come from DAMO
        This should be similar to the export that is send to the N&S datachecker and modelbuilder for this model revision,
        except discharge in 'afvoer' and 'anvoer' have been added as well as waterleverlevels.

        The output of this step is a legger_{polder}_{datetime}.sqlite file with all the necessary tables and data.
        Step 1: make empty legger database
        Step 2: Read DAMO (geopandas)
        Step 3: make dataframes from data according to right format
        Step 4: write dataframes to legger database (sqlalchemy)
        """

    def __init__(self, filepath_DAMO, database_path):
        self.filepath_DAMO = filepath_DAMO
        self.database_path = database_path

        self.ogr_exe = os.path.abspath(os.path.join(sys.executable, os.pardir, os.pardir, 'bin', 'ogr2ogr.exe'))

        self.tables = ['DuikerSifonHevel', 'Waterdeel', 'HydroObject',
                       'GW_PRO', 'GW_PRW', 'GW_PBP', 'IWS_GEO_BESCHR_PROFIELPUNTEN', 'Debieten_3Di_HR',
                       'Peilgebieden_na_datacheck']
        # tabellen oude methode peilgebieden: 'PeilafwijkingGebied', 'PeilgebiedPraktijk',

        self.db = LeggerDatabase(
            {
                'db_file': self.database_path,
                'db_path': self.database_path  # N&S inconsistent gebruik van  :-O
            },
            'spatialite'
        )

    def full_import_ogr2ogr(self):

        # Volgende activiteiten:
        # stap 1: aanmaken lege sqlite met de juiste tabellen en de juiste kolommen in tabellen lukt niet in mijn testomgeving.
        self.create_spatialite()

        # Stap 2: Laad data uit DAMO in spatialite
        self.dump_gdb_to_spatialite()

        # Stap 3: vul tabellen met queries
        self.vul_queries()

        # Stap 4: verwijder onnodige tabellen
        self.delete_imported_tables()

        # Stap 5: add default settings to tabellen
        self.add_default_settings()

    def create_spatialite(self, delete_existing=True):
        # Step 1: make empty legger database

        if os.path.exists(self.database_path):
            if delete_existing:
                os.remove(self.database_path)
            else:
                raise Exception('spatialite {0} already exist'.format(self.database_path))

        # Make new database
        self.db.create_db()

    def dump_gdb_to_spatialite(self):
        process_errors = []

        nr_tables = len(self.tables)
        for i, table in enumerate(self.tables):

            log.info("--- copy {0}/{1} table {2} ---".format(i + 1, nr_tables, table))

            # "-overwrite"
            cmd = '"{ogr_exe}" -a_srs EPSG:28992 -f SQLite -dsco SPATIALITE=YES -append ' \
                  '-lco GEOMETRY_NAME=geometry -nln {dest_table}' \
                  ' "{spatialite_path}" "{gdb_path}" {source_table}'.format(
                ogr_exe=self.ogr_exe,
                gdb_path=self.filepath_DAMO,
                source_table=table,  # ' '.join(tables),
                dest_table="imp_{0}".format(table),
                spatialite_path=self.database_path
            )
            log.info(cmd)
            print(cmd)

            ret = subprocess.call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if ret != 0:
                msg = "ogr2ogr return code was '{ret}' unequal to 0 for table '{table}'".format(ret=ret, table=table)
                process_errors.append(msg)
                log.error(msg)

    def vul_queries(self):

        # vullen profielpunten
        session = self.db.get_session()
        session.execute(text("""
        INSERT INTO profielpunten  (pbp_id, prw_id, pbpident, osmomsch, iws_volgnr, iws_hoogte, afstand, pro_pro_id, geometry)
        SELECT pbp_id, prw_id, pbpident, osmomsch, iws_volgnr, iws_hoogte, iws_afstand, pro_pro_id, CastToXY(ipp.geometry)
        FROM imp_gw_pbp pbp, imp_iws_geo_beschr_profielpunten ipp, imp_gw_prw prw 
        WHERE pbp.pbp_id = ipp.pbp_pbp_id AND pbp.prw_prw_id = prw.prw_id
        """))

        # vullen profielen
        session.execute(text("""
        INSERT INTO profielen  (proident, bron_profiel, pro_id, hydro_id)
        SELECT proident, pro.osmomsch, pro_id, ho.hydroobject_id as hydroobject_id
        FROM imp_gw_pro pro
        LEFT JOIN imp_hydroobject ho ON st_intersects(pro.geometry, ho.geometry)  
        where betrouwbaar = 1
        """))

        # vullen kenmerken
        session.execute(text("""
        INSERT INTO kenmerken(
            id,
            diepte,
            bodemhoogte,
            breedte,
            lengte,
            taludvoorkeur,
            steilste_talud,
            grondsoort,
            hydro_id)
        SELECT 
            hydroobject_id as id,
            diepte as diepte, 
            bodemhoogtenap as bodemhoogte,
            breedte as waterbreedte,
            ST_Length(geometry) as lengte,
            taludvoorkeur as talud_voorkeur,
            min(ws_talud_links, ws_talud_rechts) as steilste_talud,
            CASE WHEN grondsoort IS NULL THEN 'Onbekend' ELSE grondsoort END as grondsoort,
            hydroobject_id as hydro_id
          FROM imp_hydroobject
        """))

        session.execute(text("""
        INSERT INTO hydroobject  (id, code, categorieoppwaterlichaam, streefpeil, zomerpeil, debiet_inlaat, debiet_fme, richting_fme, geometry)
        SELECT 
            hydroobject_id as id,
            code,
            categorieoppwaterlichaam,
            winterpeil as streefpeil,
            zomerpeil,
            debiet_aanvoer,
            debiet_afvoer_prof,
            richting,
            geometry
        FROM imp_hydroobject
        """))

        session.execute(text("""
                 SELECT CreateSpatialIndex('hydroobject', 'geometry');
                 """))

        # vullen waterdeel =
        session.execute(text("""
        INSERT INTO waterdeel  (id, shape_length, shape_area, geometry)
        SELECT ogc_fid as id, shape_length, shape_area, geometry
        FROM imp_waterdeel
        """))

        # duikersifonhevel
        session.execute(text("""
         INSERT INTO duikersifonhevel(
             id, 
             code, 
             categorie, 
             lengte, 
             hoogteopening, 
             breedteopening,
             hoogtebinnenonderkantbene,
             hoogtebinnenonderkantbov,
             vormkoker,
             debiet,
             geometry)
         SELECT 
             ogc_fid, 
             code, 
             ws_categorie, 
             lengte, 
             hoogteopening, 
             breedteopening,
             hoogtebinnenonderkantbene,
             hoogtebinnenonderkantbov,
             vormkoker,
             "0",
             geometry
         FROM imp_duikersifonhevel
         """))

        # debiet 3di
        session.execute(text("""
         CREATE TABLE debiet_3di AS 
         SELECT
             fid as id,
             CASE WHEN richting > 0 THEN q_abs ELSE -1 * q_abs END as debiet,
             geometry
         FROM imp_Debieten_3Di_HR;
         """))

        session.execute(text("""
                 SELECT RecoverGeometryColumn( 'debiet_3di' , 'geometry' , 28992 , 'MULTILINESTRING' );
                 """))

        session.execute(text("""
                 SELECT CreateSpatialIndex('debiet_3di', 'geometry');
                 """))

        session.execute(text("""
WITH
    cnt as (SELECT 1 x union select x+1 from cnt where x<24)
	, pnt as (
			SELECT h.id, cnt.x / 25.0 as fraction, Line_Interpolate_Point(h.geometry, cnt.x / 25.0) as geom, ST_LENGTH(h.geometry) as length
            FROM hydroobject h, cnt
							--WHERE h.id = 165183  -- FOR DEBUGGING
	), pnt_buf as (
			SELECT p.*, ST_Expand(p.geom, MIN(1, length/ 11 )) as geom_buffer from pnt p)
	,link as (
			SELECT 
                p.id as h_id, 
                d.id as d_id, 
                p.fraction, 1 - st_distance(p.geom, d.geometry) as score, 
                abs(d.debiet) as flow 
             FROM pnt_buf p, debiet_3di d 
             WHERE st_intersects(p.geom_buffer, d.geometry) 
                AND d.ROWID IN (SELECT ROWID 
                                FROM SpatialIndex
                                WHERE f_table_name = 'debiet_3di' AND search_frame = p.geom_buffer)),
    score as (SELECT h_id, d_id, sum(score) as score, max(flow) as flow, count(*) as cnt
              FROM link 
              GROUP BY h_id, d_id 
              ORDER BY h_id, 3 DESC, 4 DESC),
    linked as (SELECT distinct * FROM score WHERE cnt >= 2 GROUP BY h_id),
    matched as (SELECT  
                    h.id as hydro_id, 
                    CASE WHEN Line_Locate_Point(h.geometry, st_startpoint(d.geometry)) <= Line_Locate_Point(h.geometry, st_endpoint(d.geometry)) THEN d.debiet ELSE -1 * d.debiet END as debiet_3di,
                    round (l.score / 45.0 * 100.0, 2) as score
                FROM linked l, hydroobject h, debiet_3di d  
                WHERE l.h_id = h.id and l.d_id = d.id)

    UPDATE hydroobject
    SET 
        debiet_3di = (
			SELECT m.debiet_3di FROM matched m
				--, hydroobject -- FOR DEBUGGING
				WHERE m.hydro_id = id
					),
        score = (SELECT m.score FROM matched m WHERE m.hydro_id = id)
         """))

        session.execute(text("""
    UPDATE hydroobject
    SET debiet = debiet_3di
                 """))

        session.execute(text("""
            WITH 
                max_diepte as (Select pr.hydro_id as hydro_id, min(pp.iws_hoogte) as bodemhoogte, ho.streefpeil - min(pp.iws_hoogte)  as diepte, 'meting' as bron
                from profielpunten pp, profielen pr, hydroobject ho
                where pp.pro_pro_id = pr.pro_id AND ho.id = pr.hydro_id AND pp.osmomsch='Z1'
                group by pr.hydro_id)

            UPDATE kenmerken
            SET (bodemhoogte, diepte, bron_diepte) = (SELECT bodemhoogte, diepte, bron FROM max_diepte WHERE kenmerken.hydro_id = max_diepte.hydro_id)
            WHERE kenmerken.hydro_id in (SELECT hydro_id FROM max_diepte)    
            """))

        session.commit()

    def delete_imported_tables(self):
        session = self.db.get_session()
        for table in self.tables:
            session.execute(text("DROP TABLE IF EXISTS imp_{0} ;".format(table.lower())))
            session.execute(text("DROP TABLE IF EXISTS idx_imp_{0}_geometry;".format(table.lower())))
            session.execute(text("DROP TABLE IF EXISTS idx_imp_{0}_geometry_node;".format(table.lower())))
            session.execute(text("DROP TABLE IF EXISTS idx_imp_{0}_geometry_parent;".format(table.lower())))
            session.execute(text("DROP TABLE IF EXISTS idx_imp_{0}_geometry_rowid;".format(table.lower())))
            session.execute(text("DELETE FROM geometry_columns WHERE f_table_name = 'imp_{0}';".format(table.lower())))

        session.commit()
        # make space available by reducing filesize
        session.execute(text("VACUUM"))
        session.commit()

    def add_default_settings(self):
        session = self.db.get_session()

        session.execute(text(
            """
                    INSERT INTO begroeiingsvariant(id, naam, is_default, friction_manning, friction_begroeiing, begroeiingsdeel) 
                    VALUES 
                        (3, 'vol', 1, 34, 65, 0.9),
                        (2, 'half', 0, 34, 30, 0.5),
                        (1, 'kwart', 0, 34, 30, 0.25)
                    """))

        session.execute(text(
            "INSERT INTO categorie(categorie, naam, variant_diepte_min, variant_diepte_max, default_talud) VALUES "
            "(1, 'primair', 0.2, 3, 1.5),"
            "(2, 'secundair', 0.15, 2.5, 1.5),"
            "(3, 'tertaire', 0.15, 1, 1.5),"
            "(4, 'overig', 0.15, 1, 1.5)"))
        session.commit()


def main():
    test_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'tests', 'data'))

    database_path = os.path.join(
        test_data_dir,
        "test_{0}.sqlite".format(str(datetime.datetime.today().strftime('%Y%m%d')))
    )

    filepath_DAMO = os.path.join(test_data_dir, 'DAMO.gdb')  # 'Hoekje_leggertool_database.gdb')

    legger_class = CreateLeggerSpatialite(
        filepath_DAMO,
        database_path
    )

    legger_class.full_import_ogr2ogr()


if __name__ == '__main__':
    main()
