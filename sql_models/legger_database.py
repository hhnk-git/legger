import copy
import logging
import os
import sqlite3
from osgeo import ogr
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy.pool import NullPool

from .legger import Base
from .sqlalchemy_add_columns import create_and_upgrade

log = logging.getLogger(__name__)


def load_spatialite_base(con, connection_record):
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
            log.info(
                "Loading extension %s from %s failed, trying the next", entry_point, lib
            )
            continue
        else:
            log.info("Successfully loaded extension %s from %s.", entry_point, lib)
            found = True
            break
    if not found:
        raise RuntimeError("Cannot find any suitable spatialite module")
    cur.close()
    con.enable_load_extension(False)


def load_spatialite(path: str):
    con = sqlite3.connect(path)
    load_spatialite_base(con, '')
    return con


class LeggerDatabase(object):
    """Wrapper around sqlalchemy interface with functions to create, update
        databases and get connections.
        This class is equal to ThreeDiDatabase, except fix_views
        Two functions create_db and get_metadata added because of link to Base
        (code is beside link to different 'Base;  equal to ThreediDatabase)
    """

    def __init__(self, path, echo=False):
        """
        :param connection_settings:
        """
        self.path = path

        self.echo = echo

        self._engine = None
        self._combined_base = None  # TODO: unused?
        self._base = None  # TODO: unused?
        self._base_metadata = None

    def create_db(self, overwrite=False):
        if overwrite and os.path.isfile(self.path):
            os.remove(self.path)

        drv = ogr.GetDriverByName('SQLite')
        db = drv.CreateDataSource(self.path,
                                  ["SPATIALITE=YES"])
        Base.metadata.create_all(self.engine)

    def get_metadata(self, including_existing_tables=False, engine=None):

        if including_existing_tables:
            metadata = copy.deepcopy(Base.metadata)
            if engine is None:
                engine = self.engine

            metadata.bind = engine
            metadata.reflect(extend_existing=True, bind=engine)
            return metadata
        else:
            if self._base_metadata is None:
                self._base_metadata = copy.deepcopy(Base.metadata)
            return self._base_metadata

    def fix_views(self):
        """function overwrite, function is not relevant"""
        raise NotImplementedError('fix views not relevant in this context')

    def create_and_check_fields(self):

        # engine = self.get_engine()
        create_and_upgrade(self.engine, self.get_metadata())
        # self.metadata(engine=engine, force_refresh=True)

    @property
    def engine(self):
        # TODO: can this become a cached_property? Depends on the following method.
        return self.get_engine()

    def get_engine(self, get_seperate_engine=False):

        if self._engine is None or get_seperate_engine:
            if self.path == "":
                # Special case in-memory SQLite:
                # https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#threading-pooling-behavior
                poolclass = None
            else:
                poolclass = NullPool

            engine = create_engine(
                "sqlite:///{0}".format(self.path), echo=self.echo, poolclass=poolclass
            )
            listen(engine, "connect", load_spatialite_base)
            if get_seperate_engine:
                return engine
            else:
                self._engine = engine

        return self._engine

    def get_metadata(self, including_existing_tables=True, engine=None):
        if including_existing_tables:
            metadata = copy.deepcopy(Base.metadata)
            if engine is None:
                engine = self.engine

            metadata.bind = engine
            metadata.reflect(extend_existing=True, bind=engine)
            return metadata
        else:
            if self._base_metadata is None:
                self._base_metadata = copy.deepcopy(Base.metadata)
            return self._base_metadata

    def get_session(self):
        return sessionmaker(bind=self.engine)()

    def run_vacuum(self):
        """
        call vacuum on a sqlite DB which reclaims any unused storage space from sqlite
        """
        statement = """VACUUM;"""
        with self.engine.begin() as connection:
            connection.execute(text(statement))
