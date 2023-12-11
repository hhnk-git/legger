-- backup bestaande varianten en geselecteerde varianten
CREATE TABLE geselecteerd_backup AS
SELECT * FROM geselecteerd
;

CREATE TABLE varianten_backup as
SELECT * FROM varianten
;

DELETE FROM varianten
;

DELETE FROM geselecteerd
;

-- grondsoorten aanpassen
WITH list AS (
SELECT a.id as hydro_id, a.code, a.grondsoort as gs_fout, b.grondsoort
FROM hydroobjects_kenmerken as a
JOIN HydroObject_grondsoort as b
ON a.code LIKE b.code
WHERE a.grondsoort NOT LIKE b.grondsoort)
UPDATE kenmerken SET grondsoort = list.grondsoort FROM list WHERE kenmerken.hydro_id = list.hydro_id

--> nieuwe varianten maken met leggertool stappen 3 en 4
-- nieuwe geselecteerde standaardprofielen in aparte tabel
CREATE TABLE geselecteerd_aanvulling AS
SELECT * FROM geselecteerd
;

DELETE FROM geselecteerd
;
-- vergelijken ingevulde standaardprofielen
-- overschrijf geselecteerde standaardprofielen uit de backup met standaardprofielen wanneer diepte en begroeiing gelijk zijn, maar nu niet
INSERT INTO geselecteerd (hydro_id,variant_id,selected_on,tot_verhang,hydro_verhang)
WITH terug_standaard as (
	SELECT a.hydro_id, a.variant_id, a.selected_on, b.verhang, b.diepte, b.begroeiingsvariant_id, SUBSTR(a.variant_id,instr(a.variant_id,'stand')+6,1) as klasse
	FROM geselecteerd_backup as a
	JOIN varianten_backup as b
	ON a.variant_id = b.id 
	WHERE a.variant_id LIKE 'OAF%'
	)
SELECT v.hydro_id, v.id as variant_id, datetime() as selected_on, NULL as tot_verhang, v.verhang as hydro_verhang 
FROM terug_standaard as ts
JOIN varianten as v
ON v.hydro_id = ts.hydro_id AND v.diepte = ts.diepte AND v.begroeiingsvariant_id = ts.begroeiingsvariant_id 
AND v.id LIKE 'OAF%'
AND SUBSTR(v.id,instr(v.id,'stand')+6,1) = ts.klasse
;

INSERT INTO geselecteerd (hydro_id,variant_id,selected_on,tot_verhang,hydro_verhang)
WITH terug_gekozen as (
	SELECT a.hydro_id, a.variant_id, a.selected_on, b.verhang, b.diepte, b.begroeiingsvariant_id, SUBSTR(a.variant_id,instr(a.variant_id,'_')+1,4) as hydro_diepte
	FROM geselecteerd_backup as a
	JOIN varianten_backup as b
	ON a.variant_id = b.id 
	WHERE a.variant_id NOT LIKE 'OAF%'
	)
SELECT v.hydro_id, v.id as variant_id, selected_on, NULL as tot_verhang, v.verhang as hydro_verhang--, tg.diepte , v.diepte, hydro_diepte, SUBSTR(v.id,instr(v.id,'_')+1,4)
FROM terug_gekozen as tg
JOIN varianten as v
ON v.hydro_id = tg.hydro_id AND v.diepte = tg.diepte AND v.begroeiingsvariant_id = tg.begroeiingsvariant_id 
AND v.id NOT LIKE 'OAF%'
AND tg.hydro_diepte = SUBSTR(v.id,instr(v.id,'_')+1,4)
AND v.hydro_id NOT IN (SELECT hydro_id FROM geselecteerd)
;

INSERT INTO geselecteerd (hydro_id,variant_id,selected_on,tot_verhang,hydro_verhang)
SELECT hydro_id,variant_id,selected_on,tot_verhang,hydro_verhang
FROM geselecteerd_aanvulling
WHERE hydro_id NOT IN (SELECT hydro_id FROM geselecteerd)
;