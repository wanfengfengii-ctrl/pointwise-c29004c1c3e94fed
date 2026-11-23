import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.publish.coordinator import publish_batch
from src.publish.database import initialize_database, open_database

DAY = '2026-04-12'
NEXT_DAY = '2026-04-13'
MISSING = object()


def event(event_id, seq, value=MISSING, *, entity='E', field='A', source='scale', day=DAY,
          payload=None, requires=()):
    if payload is None:
        payload = json.dumps({} if value is MISSING else {field: value})
    return dict(event_id=event_id, seq=seq, entity=entity, field=field, source=source,
                day=day, payload=payload, requires=[list(item) for item in requires])


class LakeCase(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(prefix='mass-test-')
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'observations.sqlite'
        self.db = self.connect()
        initialize_database(self.db)

    def connect(self, path=None):
        db = open_database(self.path if path is None else path)
        self.addCleanup(db.close)
        return db

    def put(self, request_id, *records):
        return publish_batch(self.db, request_id, records)

    def counts(self, db=None):
        db = self.db if db is None else db
        return tuple(db.execute(statement).fetchone()[0] for statement in (
            'SELECT count(*) FROM raw_events', 'SELECT count(*) FROM fact_snapshots',
            'SELECT count(*) FROM snapshot_fields', 'SELECT count(*) FROM contributions',
            'SELECT count(*) FROM daily_totals', 'SELECT count(*) FROM results',
            'SELECT count(*) FROM request_receipts', 'SELECT count(*) FROM segments',
        ))
