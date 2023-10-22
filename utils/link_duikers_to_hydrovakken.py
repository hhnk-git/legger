try:
    from legger.sql_models.legger_database import load_spatialite
except ImportError:
    import sys, os

    sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))

    from legger.sql_models.legger_database import load_spatialite


def link_duikers_to_hydrovakken(path_legger_db):
    # Verkrijg de lagen door hun naam. Zorg ervoor dat de namen exact overeenkomen met wat je in je QGIS-project hebt.

    # first add column of not exists

    conn = load_spatialite(path_legger_db)
    cur = conn.cursor()

    # Check if the column exists
    cur.execute("PRAGMA table_info(duikersifonhevel)")
    columns = [column[1] for column in cur.fetchall()]

    # If the column does not exist, add it
    if "hydro_id" not in columns:
        cur.execute("ALTER TABLE duikersifonhevel ADD COLUMN hydro_id INTEGER")
        conn.commit()

    # Check if the column exists
    cur.execute("PRAGMA table_info(hydroobject)")
    columns = [column[1] for column in cur.fetchall()]

    # If the column does not exist, add it
    if "duiker_count" not in columns:
        cur.execute("ALTER TABLE hydroobject ADD COLUMN duiker_count INTEGER;")
        conn.commit()

    cur.executescript("""    
    UPDATE duikersifonhevel
    SET hydro_id = subquery.hydroobject_id
    FROM (
        SELECT d.id AS duikersifonhevel_id, h.id AS hydroobject_id
        FROM hydroobject h, duikersifonhevel d
        WHERE ST_INTERSECTS(h.geometry, d.geometry) AND  PtDistWithin(h.geometry, ST_StartPoint(d.geometry), 0.2) AND PtDistWithin(h.geometry, ST_EndPoint(d.geometry), 0.2)
    ) AS subquery
    WHERE duikersifonhevel.id = subquery.duikersifonhevel_id;
    """)

    conn.commit()
    conn.execute("""
    -- Update de waarden
    UPDATE hydroobject
    SET duiker_count = (
        SELECT COUNT(dsh.id)
        FROM duikersifonhevel dsh
        WHERE dsh.hydro_id = hydroobject.id
    );
    """)

    conn.commit()


if __name__ == '__main__':
    link_duikers_to_hydrovakken('//Users/bastiaanroos/Documents/testdata/leggertool/legger_westerkogge2.sqlite')
