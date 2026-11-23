import sqlite3

from src.partitions.catalog import load_catalog, resolve_segment
from src.partitions.layout import read_segment
from src.partitions.verify import SegmentIntegrityError
from src.publish.attempts import RequestConflict, load_attempt
from src.publish.coordinator import publish_compaction
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import query_history

from tests import DAY, NEXT_DAY, LakeCase, event


class TestCompaction(LakeCase):
    def _fragments(self):
        first = self.put('old', event('written', 1, 12))
        self.put('corrected', event('clear', 4, None), event('b', 5, 5, field='B'))
        self.put('late', event('late', 3, 9))
        return first

    def test_failed_compaction_keeps_old_catalog(self):
        old = self._fragments()
        before = query_current(self.db)
        catalog = load_catalog(self.db)
        counts = self.counts()
        stable = tuple(read_segment(self.db, segment_id) for _, segment_id in catalog.active)
        self.db.execute('''CREATE TRIGGER damage_locator AFTER INSERT ON segment_members
                           WHEN NEW.raw_ref = NEW.event_key
                           BEGIN UPDATE segment_members SET raw_ref = 'missing original'
                            WHERE segment_id = NEW.segment_id AND event_key = NEW.event_key; END''')
        with self.assertRaisesRegex(SegmentIntegrityError, 'replacement evidence differs'):
            publish_compaction(self.db, 'readback', DAY, DAY, catalog.version)
        self.db.execute('DROP TRIGGER damage_locator')
        self.db.execute('CREATE TABLE commit_parent (id INTEGER PRIMARY KEY)')
        self.db.execute('''CREATE TABLE commit_guard (id INTEGER REFERENCES commit_parent(id)
                           DEFERRABLE INITIALLY DEFERRED)''')
        self.db.execute('''CREATE TRIGGER refuse_catalog_commit AFTER INSERT ON catalog_replacements
                           BEGIN INSERT INTO commit_guard VALUES (87); END''')
        with self.assertRaises(sqlite3.IntegrityError):
            publish_compaction(self.db, 'commit', DAY, DAY, catalog.version)
        reader = self.connect()
        self.assertEqual(load_catalog(reader), catalog)
        self.assertEqual(self.counts(reader), counts)
        self.assertEqual(tuple(read_segment(reader, item.segment_id) for item in stable), stable)
        self.assertEqual(query_current(reader), before)
        self.assertEqual(query_history(reader, old.result_id).daily[0].total_mg, '12')
        for request_id in ('readback', 'commit'):
            self.assertIsNone(find_request_receipt(reader, request_id))
            self.assertEqual(load_attempt(reader, request_id).status, 'failed')
        reader.execute('DROP TRIGGER refuse_catalog_commit')
        success = publish_compaction(reader, 'commit', DAY, DAY, catalog.version)
        self.assertEqual(query_current(reader), before)
        self.assertEqual(success.catalog_version, catalog.version + 1)

    def test_valid_replacement_and_source_version(self):
        old = self._fragments()
        self.put('tomorrow', event('tomorrow', 1, 3, field='C', day=NEXT_DAY))
        before = query_current(self.db)
        source = load_catalog(self.db)
        old_ids = {segment_id for day, segment_id in source.active if day == DAY}
        later = {segment_id for day, segment_id in source.active if day == NEXT_DAY}
        maintenance = publish_compaction(self.db, 'compact', DAY, DAY, source.version)
        self.assertEqual(maintenance.result_id, before.result_id)
        self.assertEqual({old_id for old_id, _ in maintenance.replacements}, old_ids)
        new_ids = {new_id for _, new_id in maintenance.replacements}
        self.assertEqual(len(new_ids), 1)
        self.assertEqual({segment_id for _, segment_id in load_catalog(self.db).active}, new_ids | later)
        for old_id in old_ids:
            self.assertTrue(read_segment(self.db, old_id).retired)
        historic = query_history(self.db, old.result_id)
        self.assertIn(historic.evidence[0].origin_segment, old_ids)
        self.assertIn(resolve_segment(self.db, historic.evidence[0].original.identity.key).active, new_ids)
        self.assertEqual(query_current(self.db), before)
        self.assertEqual(publish_compaction(self.db, 'compact', DAY, DAY, source.version), maintenance)
        with self.assertRaises(RequestConflict):
            publish_compaction(self.db, 'compact', DAY, NEXT_DAY, source.version)
        with self.assertRaisesRegex(SegmentIntegrityError, 'stale'):
            publish_compaction(self.db, 'stale', DAY, DAY, source.version)
        self.assertIsNone(find_request_receipt(self.db, 'stale'))
        second = publish_compaction(self.db, 'compact-again', DAY, DAY, maintenance.catalog_version)
        self.assertEqual({old_id for old_id, _ in second.replacements}, new_ids)
        self.assertEqual(query_history(self.db, old.result_id), historic)
        self.assertEqual(query_current(self.db), before)
