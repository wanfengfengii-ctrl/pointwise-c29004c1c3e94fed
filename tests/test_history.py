from src.intake.identity import identify_event
from src.intake.validation import IntakeError
from src.partitions.catalog import load_catalog
from src.publish.coordinator import publish_compaction
from src.publish.database import initialize_database
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import query_history
from src.query.reconcile import ReconciliationError

from tests import DAY, LakeCase, event


class TestHistory(LakeCase):
    def test_snapshot_is_immutable(self):
        old = self.put('first', event('first', 1, 12))
        before = query_history(self.db, old.result_id)
        cleared = self.put('clear', event('clear', 3, None), event('b', 4, 5, field='B'))
        after = query_history(self.db, cleared.result_id)
        self.put('late', event('late', 2, 9))
        current = query_current(self.db)
        self.assertEqual(before.facts[0].value, '12')
        self.assertEqual(before.daily[0].total_mg, '12')
        self.assertEqual(after.facts[0].state, 'clear')
        self.assertEqual(after.facts[0].event.event_id, 'clear')
        self.assertEqual(after.daily[0].total_mg, '5')
        catalog = load_catalog(self.db)
        maintenance = publish_compaction(self.db, 'compact', DAY, DAY, catalog.version)
        self.assertEqual(maintenance.result_id, current.result_id)
        self.assertEqual(query_current(self.db), current)
        self.assertEqual(query_history(self.db, old.result_id), before)
        self.assertEqual(query_history(self.db, cleared.result_id), after)
        with self.assertRaises(LookupError):
            query_history(self.db, 'result-999')

    def test_reopen_preserves_data(self):
        first = self.put('first', event('first', 1, 12))
        manual = self.put('manual', event('manual', 2, 16, source='qc'))
        old = query_history(self.db, first.result_id)
        revised = query_current(self.db)
        published_catalog = load_catalog(self.db)
        counts = self.counts()
        self.db.close()
        reopened = self.connect()
        initialize_database(reopened)
        initialize_database(reopened)
        self.assertEqual(self.counts(reopened), counts)
        self.assertEqual(query_history(reopened, first.result_id), old)
        self.assertEqual(query_current(reopened), revised)
        self.assertEqual(query_current(reopened).daily[0].total_mg, '16')
        self.assertEqual(find_request_receipt(reopened, 'manual'), manual)
        self.assertEqual(load_catalog(reopened), published_catalog)

    def test_incomplete_evidence_rejected(self):
        first = self.put('first', event('first', 1, 12))
        old = query_history(self.db, first.result_id)
        clear = event('clear', 2, None)
        recent = self.put('recent', clear, event('b', 2, 5, field='B'))
        original = self.db.execute('SELECT raw_json FROM raw_events WHERE event_key = ?',
                                   (identify_event(clear).key,)).fetchone()[0]
        self.db.execute('UPDATE raw_events SET raw_json = ? WHERE event_key = ?', ('{', identify_event(clear).key))
        with self.assertRaisesRegex(IntakeError, 'damaged'):
            query_history(self.db, recent.result_id)
        self.assertEqual(query_history(self.db, first.result_id), old)
        self.db.execute('UPDATE raw_events SET raw_json = ? WHERE event_key = ?', (original, identify_event(clear).key))
        self.db.execute('UPDATE daily_totals SET total_mg = ? WHERE version = ?', ('17', recent.version))
        with self.assertRaises(ReconciliationError):
            query_current(self.db)
        self.assertEqual(query_history(self.db, first.result_id), old)
        self.assertEqual(find_request_receipt(self.db, 'recent'), recent)
