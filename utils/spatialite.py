# -*- coding: utf-8 -*-
import logging
import sqlite3

logger = logging.getLogger(__name__)


def set_spatial_extension(con, connection_record):
    """Load spatialite extension as described in
    https://geoalchemy-2.readthedocs.io/en/latest/spatialite_tutorial.html"""
    import sqlite3

    con.enable_load_extension(True)
    cur = con.cursor()
    libs = [
        # SpatiaLite >= 4.2 and Sqlite >= 3.7.17, should work on all platforms
        ("mod_spatialite", "sqlite3_modspatialite_init"),
        # SpatiaLite >= 4.2 and Sqlite < 3.7.17 (Travis)
        ("mod_spatialite.so", "sqlite3_modspatialite_init"),
        # SpatiaLite < 4.2 (linux)
        ("libspatialite.so", "sqlite3_extension_init"),
    ]
    found = False
    for lib, entry_point in libs:
        try:
            cur.execute("select load_extension('{}', '{}')".format(lib, entry_point))
        except sqlite3.OperationalError:
            logger.exception(
                "Loading extension %s from %s failed, trying the next", entry_point, lib
            )
            continue
        else:
            logger.info("Successfully loaded extension %s from %s.", entry_point, lib)
            found = True
            break
    # if not found:
    #     raise RuntimeError("Cannot find any suitable spatialite module")
    cur.close()
    con.enable_load_extension(False)


def load_spatialite(path: str):
    con = sqlite3.connect(path)
    set_spatial_extension(con, '')
    return con