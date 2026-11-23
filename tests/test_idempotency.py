from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Barrier

from src.intake.validation import IntakeError
from src.publish.attempts import RequestConflict, bind_request_parameters, load_attempt
from src.publish.coordinator import publish_batch
from src.publish.database import open_database
from src.publish.receipts import find_request_receipt
from src.publish.rollback import publication_transaction, recover_incomplete_attempts
from src.query.current import query_current

from tests import LakeCase, event


class TestIdempotency(LakeCase):
    def test_retry_and_conflict(self):
        first = event('a', 1, 12)
        second = event('b', 1, 0, field='B')
        original = self.put('submitted', first, second)
        counts = self.counts()
        reopened = self.connect()
        self.assertEqual(publish_batch(reopened, 'submitted', [second, first]), original)
        self.assertEqual(find_request_receipt(reopened, 'submitted'), original)
        with self.assertRaises(RequestConflict):
            publish_batch(reopened, 'submitted', [event('a', 1, 13), second])
        with self.assertRaises(IntakeError):
            publish_batch(reopened, 'conflicting-event', [event('a', 1, 13)])
        self.assertIsNone(find_request_receipt(reopened, 'conflicting-event'))
        self.assertEqual(load_attempt(reopened, 'submitted').status, 'complete')
        self.assertEqual(self.counts(reopened), counts)
        self.assertEqual(query_current(reopened).daily[0].total_mg, '12')

    def test_recover_pending_binding(self):
        record = event('a', 1, 12)
        with publication_transaction(self.db):
            bind_request_parameters(self.db, 'pending', 'batch', [record])
        reopened = self.connect()
        self.assertEqual(recover_incomplete_attempts(reopened), ('pending',))
        self.assertEqual(load_attempt(reopened, 'pending').status, 'failed')
        self.assertIsNone(find_request_receipt(reopened, 'pending'))
        with self.assertRaises(RequestConflict):
            publish_batch(reopened, 'pending', [event('a', 1, 11)])
        success = publish_batch(reopened, 'pending', [record])
        self.assertEqual(recover_incomplete_attempts(reopened), ())
        self.assertEqual(publish_batch(reopened, 'pending', [record]), success)

    def test_concurrent_retries_share_one_receipt(self):
        ready = Barrier(2)
        record = event('a', 1, 12)

        def send():
            with closing(open_database(self.path)) as db:
                ready.wait(timeout=5)
                return publish_batch(db, 'same', [record])

        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [pool.submit(send) for _ in range(2)]
            left, right = (call.result(timeout=15) for call in calls)
        self.assertEqual(left, right)
        self.assertEqual(self.counts()[:2], (1, 1))
        self.assertEqual(self.counts()[6], 1)
