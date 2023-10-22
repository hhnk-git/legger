import sqlite3


def create_legger_views(session: sqlite3.Connection):
    session.executescript(
        """
            DROP VIEW IF EXISTS hydroobjects_kenmerken;
            
            CREATE VIEW hydroobjects_kenmerken AS 
            SELECT 
                h.id, 
                h.code, 
                h.categorieoppwaterlichaam, 
                h.streefpeil,
                h.zomerpeil,
                ABS(debiet) as debiet,
                ABS(debiet_inlaat) as debiet_inlaat,
                ABS(debiet_3di) as debiet_3di,
                ABS(debiet_aangepast) as debiet_aangepast,
                ABS(h.debiet_opgelegd),
                h.eindpunt_geselecteerd,
                diepte, 
                breedte, 
                grondsoort,
                begroeiingsvariant_id, 
                min_diepte, 
                max_diepte, 
                min_breedte, 
                max_breedte,
                ST_LENGTH("GEOMETRY") as lengte,
                geselecteerd_diepte,
                geselecteerd_breedte,
                geselecteerde_variant,
                geselecteerde_begroeiingsvariant,
                geselecteerd_verhang,
                geselecteerd_verhang_inlaat,
                geselecteerd_afvoer_leidend,
                k.soort_vak,
                h.duiker_count,
                h.opmerkingen,
                h.kijkp_breedte,
                h.kijkp_diepte,
                h.kijkp_talud,
                h.kijkp_reden,
                CASE 
                  WHEN h.debiet_aangepast >= 0 THEN "GEOMETRY"
                  WHEN h.debiet_aangepast THEN ST_REVERSE("GEOMETRY")
                    ELSE "GEOMETRY" 
                END AS "GEOMETRY",
                CASE 
                  WHEN h.debiet_aangepast >= 0 THEN MakeLine(StartPoint("GEOMETRY"), EndPoint("GEOMETRY"))
                  WHEN h.debiet_aangepast THEN MakeLine(EndPoint("GEOMETRY"), StartPoint("GEOMETRY"))
                    ELSE MakeLine(StartPoint("GEOMETRY"), EndPoint("GEOMETRY"))
                END AS line,
                CASE WHEN h.debiet_aangepast >= 0 or h.debiet_aangepast is null  THEN 1
                    ELSE 3 
                END AS direction,
                CASE WHEN h.debiet_aangepast >= 0 or h.debiet_aangepast is null THEN CAST(0 AS BIT)
                    ELSE CAST(1 AS BIT)
                END AS reversed
            FROM hydroobject h 
            JOIN kenmerken k ON h.id = k.hydro_id 
            LEFT OUTER JOIN ( 
                SELECT
                hydro_id,
                min(diepte) AS min_diepte,
                max(diepte) AS max_diepte,
                min(waterbreedte) AS min_breedte,
                max(waterbreedte) AS max_breedte
                FROM varianten
                GROUP BY hydro_id) AS mm 
                ON mm.hydro_id = h.id
            LEFT OUTER JOIN (
                SELECT
                g.hydro_id,
                v.diepte as geselecteerd_diepte,
                v.waterbreedte as geselecteerd_breedte,
                v.id as geselecteerde_variant,
                v.begroeiingsvariant_id as geselecteerde_begroeiingsvariant,
                v.verhang as geselecteerd_verhang,
                v.verhang_inlaat as geselecteerd_verhang_inlaat,
                v.afvoer_leidend as geselecteerd_afvoer_leidend
                FROM geselecteerd g, varianten v
                WHERE g.variant_id = v.id) as sel
                ON sel.hydro_id = h.id;
         
        DELETE FROM views_geometry_columns WHERE view_name = 'hydroobjects_kenmerken';
        INSERT INTO views_geometry_columns(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column, read_only)
            VALUES ('hydroobjects_kenmerken', 'line', 'id', 'hydroobject', 'geometry', 1);
        INSERT INTO views_geometry_columns(view_name, view_geometry, view_rowid, f_table_name, f_geometry_column, read_only)
            VALUES ('hydroobjects_kenmerken', 'geometry', 'id', 'hydroobject', 'geometry', 1);
        
        SELECT RecoverGeometryColumn( 'hydroobjects_kenmerken' , 'geometry' , 28992 , 'LINESTRING');
        SELECT RecoverGeometryColumn( 'hydroobjects_kenmerken' , 'line' , 28992 , 'LINESTRING');
        
        SELECT InvalidateLayerStatistics('hydroobject');
        SELECT UpdateLayerStatistics('hydroobject');
        SELECT InvalidateLayerStatistics('hydroobjects_kenmerken');
        SELECT UpdateLayerStatistics('hydroobjects_kenmerken');
          
        """)

    # session.execute(
    #     """
    #         INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, f_table_name,
    #           f_geometry_column, read_only)
    #         VALUES('hydroobjects_kenmerken', 'geometry', 'id', 'hydroobject', 'geometry', 1);
    #     """)
    #
    # session.execute(
    #     """
    #         INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, f_table_name,
    #           f_geometry_column, read_only)
    #         VALUES('hydroobjects_kenmerken', 'line', 'id', 'hydroobject', 'geometry', 1);
    #     """)
    #
    # session.execute(
    #     """
    #         SELECT InvalidateLayerStatistics('hydroobject');
    #     """)
    #
    # session.execute(
    #     """
    #         SELECT UpdateLayerStatistics('hydroobject');
    #     """)
    #
    # session.execute(
    #     """
    #         SELECT UpdateLayerStatistics('hydroobjects_kenmerken');
    #     """)

    session.commit()

    ### view for getting all legger results, including additional performance indicators
    session.executescript(
        """
            DROP VIEW IF EXISTS hydroobjects_selected_legger;
        
            CREATE VIEW hydroobjects_selected_legger AS 
            SELECT 
                h.id, 
                h.code, 
                h.categorieoppwaterlichaam, 
                h.streefpeil, 
                h.zomerpeil,
                h.debiet,
                h.debiet_inlaat,
                h.debiet_3di,
                h.debiet_aangepast,
                k.diepte,
                k.breedte, 
                k.grondsoort, 
                h.kijkp_breedte,
                h.kijkp_diepte,
                h.kijkp_talud,
                h.kijkp_reden,
                h.opmerkingen,
                ST_LENGTH(h.geometry) as lengte,
                h.geometry,
                s.selected_on as geselecteerd_op,
                s.tot_verhang as totaal_verhang,
                --s.opmerkingen as selectie_opmerking,
                v.diepte as geselecteerde_diepte,
                v.waterbreedte as geselecteerd_waterbreedte,
                v.bodembreedte as geselecteerde_bodembreedte,
                v.talud as geselecteerd_talud,
                v.verhang as verhang,
                v.opmerkingen as profiel_opmerking,
                v.begroeiingsvariant_id as geselecteerde_begroeiingsvariant,
                p.t_fit as fit_score,
                p.t_afst as offset,
                p.t_overdiepte as overdiepte,
                p.t_overbreedte_l as overbreedte_links,
                p.t_overbreedte_r as overbreedte_rechts
            FROM hydroobject h
            JOIN kenmerken k ON h.id = k.hydro_id 	
            LEFT OUTER JOIN geselecteerd s ON h.id = s.hydro_id
            LEFT OUTER JOIN varianten v ON s.variant_id = v.id
            LEFT OUTER JOIN profielfiguren p ON v.id = p.profid;
         
         --DELETE FROM views_geometry_columns WHERE view_name = 'hydroobjects_selected_legger';
         SELECT RecoverGeometryColumn( 'hydroobjects_selected_legger' , 'geometry' , 28992 , 'LineString' );      
        """)

    # session.execute(
    #     """
    #         INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, f_table_name,
    #           f_geometry_column, read_only)
    #         VALUES('hydroobjects_selected_legger', 'geometry', 'id', 'hydroobject', 'geometry', 1);
    #     """)
    #
    # session.execute(
    #     """
    #         SELECT UpdateLayerStatistics('hydroobjects_selected_legger');
    #     """)

    # session.executescript(
    #     """
    #     DROP VIEW IF EXISTS begroeiingsadvies;
    #     CREATE VIEW begroeiingsadvies AS
    #       SELECT
    #         bv.naam as advies_naam,
    #         a.*
    #         FROM 
    #         (SELECT 
    #         CASE
    #             WHEN categorieoppwaterlichaam == 1 THEN min(advies_ruw, 2)
    #             ELSE advies_ruw
    #         END as advies_id,
    #         *
    #         FROM
    #         (SELECT  
    #         CASE 
    #           WHEN fit_sterk > 0.995 then 3
    #           WHEN fit_middel > 0.995 then 2
    #           WHEN fit_normaal > 0.995 then 1
    #           WHEN fit_normaal is not NULL then 
    #                 CASE
    #                 WHEN minprof_sterk = 1 then 3
    #                 WHEN minprof_middel = 1 then 2
    #                 WHEN minprof_normaal = 1 then 1
    #                 ELSE
    #                 0
    #                 END
    #           WHEN overd_sterk > 0.2 then 3
    #           WHEN minprof_sterk = 1 then 3
    #           WHEN overd_middel > 0.2 then 2
    #           WHEN minprof_middel = 1 then 2
    #           WHEN overd_normaal > 0 then 1
    #           WHEN minprof_normaal = 1 then 1
    #           WHEN aantal_normaal is NULL then -1
    #           else 0
    #           END as advies_ruw,
    #           *
    #         FROM
    #         (SELECT
    #         h.id,
    #         h.categorieoppwaterlichaam,
    #         h.geometry,
    #         min(h.begroeiingsvariant_id) as aangew_bv_id,
    #         min(bv.naam) as aangew_bv_naam,
    #         max(pf3.t_fit) AS fit_sterk,
    #         max(pf2.t_fit) AS fit_middel,
    #         max(pf1.t_fit) AS fit_normaal,
    #         max(d3.over_diepte) as overd_sterk,
    #         max(d2.over_diepte) as overd_middel,
    #         max(d1.over_diepte) as overd_normaal,
    #         m3.aantal = 1 and m3.max_bodembreedte as minprof_sterk,
    #         m2.aantal = 1 and m2.max_bodembreedte as minprof_middel,
    #         m1.aantal = 1 and m1.max_bodembreedte as minprof_normaal,
    #         m1.aantal as aantal_normaal
    #         FROM hydroobject h 
    #         LEFT OUTER JOIN (Select * from profielfiguren f, varianten v where v.id = f.profid and f.t_overdiepte >= 0.2 and v.begroeiingsvariant_id = 1) as pf1 on pf1.id_hydro = h.id
    #         LEFT OUTER JOIN (Select * from profielfiguren f, varianten v where v.id = f.profid and f.t_overdiepte >= 0.2 and v.begroeiingsvariant_id = 2) as pf2 on pf2.id_hydro = h.id
    #         LEFT OUTER JOIN (Select * from profielfiguren f, varianten v where v.id = f.profid and f.t_overdiepte >= 0 and v.begroeiingsvariant_id = 3) as pf3 on pf3.id_hydro = h.id
    #         LEFT OUTER JOIN (Select ho.id as id,  max(k.diepte - v.diepte) as over_diepte from hydroobject ho, varianten v, kenmerken k 
    #             where v.hydro_id = ho.id and k.hydro_id = ho.id and v.waterbreedte <= k.breedte  and v.begroeiingsvariant_id = 1
    #             GROUP BY ho.id
    #             ) as d1 on d1.id = h.id
    #         LEFT OUTER JOIN (Select ho.id as id,  max(k.diepte - v.diepte) as over_diepte from hydroobject ho, varianten v, kenmerken k 
    #             where v.hydro_id = ho.id and k.hydro_id = ho.id and v.waterbreedte <= k.breedte  and v.begroeiingsvariant_id = 2
    #             GROUP BY ho.id
    #             ) as d2 on d2.id = h.id
    #         LEFT OUTER JOIN (Select ho.id as id,  max(k.diepte - v.diepte) as over_diepte from hydroobject ho, varianten v, kenmerken k 
    #             where v.hydro_id = ho.id and k.hydro_id = ho.id and (v.waterbreedte <= k.breedte or v.bodembreedte = 0.5)  and v.begroeiingsvariant_id = 3
    #             GROUP BY ho.id
    #             ) as d3 on d3.id = h.id
    #         LEFT OUTER JOIN (Select hydro_id, count(hydro_id) as aantal, max(bodembreedte) as max_bodembreedte from varianten v
    #             where begroeiingsvariant_id = 1
    #             group by hydro_id
    #             ) as m1 on m1.hydro_id = h.id
    #         LEFT OUTER JOIN (Select hydro_id, count(hydro_id) as aantal, max(bodembreedte) as max_bodembreedte from varianten v
    #             where begroeiingsvariant_id = 2
    #             group by hydro_id
    #             ) as m2 on m2.hydro_id = h.id
    #         LEFT OUTER JOIN (Select hydro_id, count(hydro_id) as aantal, max(bodembreedte) as max_bodembreedte from varianten v
    #             where begroeiingsvariant_id = 3
    #             group by hydro_id
    #             ) as m3 on m3.hydro_id = h.id
    #         LEFT JOIN begroeiingsvariant bv on h.begroeiingsvariant_id = bv.id 
    #         GROUP BY h.id
    #         ORDER BY h.id))) as a
    #         LEFT OUTER JOIN begroeiingsvariant as bv ON bv.id = a.advies_id;

    #         --DELETE FROM views_geometry_columns WHERE view_name = 'begroeiingsadvies';
    #         SELECT RecoverGeometryColumn( 'begroeiingsadvies' , 'geometry' , 28992 , 'LINESTRING' );       
    #      """)

    # session.execute(
    #     """
    #         INSERT INTO views_geometry_columns (view_name, view_geometry, view_rowid, f_table_name,
    #           f_geometry_column, read_only)
    #         VALUES('begroeiingsadvies', 'geometry', 'id', 'hydroobject', 'geometry', 1);
    #     """)

    session.commit()
