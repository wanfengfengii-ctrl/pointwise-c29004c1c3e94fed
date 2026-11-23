import sqlite3

from src.facts.order import ReplayOrderError
from src.intake.identity import identify_event
from src.intake.validation import IntakeError
from src.publish.attempts import load_attempt
from src.publish.coordinator import publish_batch
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import query_history

from tests import LakeCase, event


class TestFailures(LakeCase):
    def test_replay_rejection_keeps_old_state(self):
        first = self.put('first', event('a', 1, 12))
        prior = query_current(self.db)
        counts = self.counts()
        predecessor = event('needed', 2, 7, field='B')
        dependent = event('dependent', 4, None, requires=[identify_event(predecessor).parts])
        for request, record, exception in (
            ('missing', dependent, ReplayOrderError),
            ('collision', event('collision', 1, 13, source='correction'), ReplayOrderError),
            ('damaged', event('bad', 6, payload='{"A":'), IntakeError),
            ('duplicate-key', event('bad-key', 6, payload='{"A":null,"A":12}'), IntakeError),
            ('non-number', event('bad-value', 6, True), IntakeError),
        ):
            with self.subTest(request=request):
                with self.assertRaises(exception):
                    self.put(request, record)
                self.assertEqual(query_current(self.db), prior)
                self.assertEqual(query_history(self.db, first.result_id), prior)
                self.assertIsNone(find_request_receipt(self.db, request))
                self.assertEqual(self.counts(), counts)
        self.put('predecessor', predecessor)
        recovered = self.put('missing', dependent)
        self.assertEqual(query_current(self.db).result_id, recovered.result_id)
        self.assertEqual(query_current(self.db).daily[0].total_mg, '7')
        self.assertEqual(query_history(self.db, first.result_id), prior)

    def test_cycle_rejects_both_events(self):
        later = event('later', 3, 11)
        earlier = event('earlier', 1, 12, requires=[identify_event(later).parts])
        with self.assertRaisesRegex(ReplayOrderError, 'conflict'):
            self.put('cycle', later, earlier)
        self.assertEqual(self.counts()[:2], (0, 0))
        self.assertIsNone(query_current(self.db))
        self.assertIsNone(find_request_receipt(self.db, 'cycle'))

    def test_publish_failure_is_atomic(self):
        accepted = self.put('old', event('old', 1, 12))
        prior = query_current(self.db)
        counts = self.counts()
        requested = [event('clear', 3, None), event('other', 4, 5, field='B')]
        self.db.execute('''CREATE TRIGGER reject_daily_insert BEFORE INSERT ON daily_totals
                           BEGIN SELECT RAISE(ABORT, 'daily table unavailable'); END''')
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'daily table unavailable'):
            self.put('new', *requested)
        reader = self.connect()
        self.assertEqual(query_current(reader), prior)
        self.assertEqual(query_history(reader, accepted.result_id), prior)
        self.assertEqual(find_request_receipt(reader, 'old'), accepted)
        self.assertIsNone(find_request_receipt(reader, 'new'))
        self.assertEqual(self.counts(reader), counts)
        self.assertIn('daily table unavailable', load_attempt(reader, 'new').reason)
        reader.execute('DROP TRIGGER reject_daily_insert')
        new = publish_batch(reader, 'new', requested)
        self.assertEqual(query_current(reader).result_id, new.result_id)
        self.assertEqual(query_current(reader).daily[0].total_mg, '5')
        self.assertEqual(query_history(reader, accepted.result_id), prior)
