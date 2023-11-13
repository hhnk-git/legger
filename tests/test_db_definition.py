import os.path
import unittest

from legger.sql_models.legger import HydroObject
from legger.sql_models.legger_database import LeggerDatabase


class TestReadTdiResults(unittest.TestCase):
    def setUp(self):
        self.legger_db = os.path.join(
            os.path.dirname(__file__),
            'data',
            'test_spatialite_with_matchprof.sqlite'
        )

    def test_with_query(self):
        db = LeggerDatabase(self.legger_db)
        session = db.get_session()

        hydro_object = session.query(HydroObject).filter_by(id=1).first()
        hydro_objects = session.query(HydroObject).filter(HydroObject.id.in_([])).all()

        self.assertIsNotNone(hydro_object)
