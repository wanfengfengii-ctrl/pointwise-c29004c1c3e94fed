import json

from src.intake.identity import identify_event
from src.intake.raw_store import find_raw_event
from src.intake.validation import IntakeError
from src.facts.order import ReplayOrderError
from src.partitions.catalog import load_catalog, resolve_segment
from src.publish.attempts import RequestConflict, bind_request_parameters, load_attempt
from src.publish.coordinator import publish_compaction
from src.publish.database import initialize_database
from src.publish.receipts import find_request_receipt
from src.publish.retention import ReclaimedResultError, reclaim, result_is_reclaimed
from src.publish.rollback import publication_transaction, recover_incomplete_attempts
from src.query.current import query_current
from src.query.history import query_history

from tests import DAY, LakeCase, event


class TestRetention(LakeCase):
    def _ab_ledger(self):
        # Old originals A (a1 = 12) and B (b1 = 3) arrive in the same batch.
        a1 = event('a1', 1, 12)
        b1 = event('b1', 1, 3, field='B')
        first = self.put('first', a1, b1)
        # A is re-measured as a2 but explicitly depends on the old a1, so a1 is
        # retained; B is overwritten by b2 and nobody depends on the old b1.
        a2 = event('a2', 2, 10, requires=[identify_event(a1).parts])
        b2 = event('b2', 2, 5, field='B')
        current = self.put('second', a2, b2)
        return first, current, (a1, b1, a2, b2)

    def test_closure_keeps_dependent_a_and_removes_covered_b(self):
        first, current, (a1, b1, a2, b2) = self._ab_ledger()
        version = load_catalog(self.db).version
        receipt = reclaim(self.db, 'reclaim', {first.result_id}, version)
        self.assertEqual(receipt.kind, 'reclaim')
        self.assertIsNone(receipt.result_id)
        self.assertEqual([item.event.event_id for item in receipt.events], ['b1'])
        self.assertEqual(receipt.catalog_version, version + 1)
        flags = {row[0].split('"')[1]: row[1]
                 for row in self.db.execute('SELECT event_key, reclaimed FROM raw_events')}
        self.assertEqual(flags, {'a1': 0, 'a2': 0, 'b1': 1, 'b2': 0})
        # Identity, sequence occupancy and body summary survive; the body is gone.
        tombstone = find_raw_event(self.db, identify_event(b1))
        self.assertTrue(tombstone.reclaimed)
        self.assertEqual(tombstone.identity, identify_event(b1))
        self.assertEqual(tombstone.revision.seq, b1['seq'])
        self.assertTrue(tombstone.payload_digest)
        # Facts, details and totals of the current ledger stay intact.
        result = query_current(self.db)
        self.assertEqual([(f.field, f.value, f.event.event_id) for f in result.facts],
                         [('A', '10', 'a2'), ('B', '5', 'b2')])
        self.assertEqual([(d.day, d.total_mg, d.count) for d in result.daily], [(DAY, '15', 2)])
        # Current evidence may omit the removed historical original but keeps the
        # origin segment numbers for the surviving evidence.
        evidence_ids = {item.original.identity.event_id for item in result.evidence}
        self.assertEqual(evidence_ids, {'a1', 'a2', 'b2'})
        self.assertTrue(all(item.origin_segment > 0 for item in result.evidence))
        print('POINTWISE_SCENARIO ' + json.dumps(dict(
            case_id='V9.retention_closure', input=[a1, b1, a2, b2],
            expected={'retained': ['a1', 'a2', 'b2'], 'removed': ['b1']},
            actual={'retained': sorted(evidence_ids),
                    'removed': [item.event.event_id for item in receipt.events]},
            code_paths=['src/publish/retention.py', 'src/intake/raw_store.py', 'src/query/history.py'],
            oracle='a2 显式前置依赖 a1，旧原件 A 保留；b1 已被 b2 覆盖且无人依赖，旧原件 B 回收。'),
            ensure_ascii=False))

    def test_expired_history_reports_reclaimed(self):
        first, current, _ = self._ab_ledger()
        reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        self.assertTrue(result_is_reclaimed(self.db, first.result_id))
        with self.assertRaises(ReclaimedResultError):
            query_history(self.db, first.result_id)
        # The current result is never treated as reclaimed.
        self.assertFalse(result_is_reclaimed(self.db, current.result_id))

    def test_same_body_recollects_original_receipt(self):
        first, _, (a1, b1, _, _) = self._ab_ledger()
        reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        repeat = self.put('recollect', b1)
        self.assertEqual(repeat.result_id, first.result_id)
        self.assertEqual(len(repeat.events), 1)
        self.assertEqual(repeat.events[0].event, identify_event(b1))
        self.assertEqual(repeat.events[0].request_id, 'first')
        # The body stays reclaimed; resubmission does not restore the original.
        self.assertTrue(find_raw_event(self.db, identify_event(b1)).reclaimed)

    def test_same_identity_different_body_rejected(self):
        first, _, (_, b1, _, _) = self._ab_ledger()
        reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        altered = event('b1', 1, 4, field='B')
        with self.assertRaises(IntakeError):
            self.put('tamper', altered)
        self.assertIsNone(find_request_receipt(self.db, 'tamper'))
        # Sequence occupancy still blocks a colliding new identity.
        collision = event('intruder', 1, 4, field='B', source='other-scale')
        with self.assertRaises(ReplayOrderError):
            self.put('collide', collision)

    def test_new_event_referencing_reclaimed_body_fails_whole_batch(self):
        first, _, (_, b1, _, _) = self._ab_ledger()
        reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        follower = event('follower', 3, 1, field='C',
                         requires=[identify_event(b1).parts])
        innocent = event('innocent', 4, 8, field='D')
        before = query_current(self.db)
        with self.assertRaises(IntakeError):
            self.put('stale-reference', follower, innocent)
        # Nothing from the rejected batch is stored and the ledger is unchanged.
        self.assertIsNone(find_raw_event(self.db, identify_event(follower)))
        self.assertIsNone(find_raw_event(self.db, identify_event(innocent)))
        self.assertIsNone(find_request_receipt(self.db, 'stale-reference'))
        self.assertEqual(query_current(self.db), before)

    def test_stale_catalog_version_rejected(self):
        first, _, _ = self._ab_ledger()
        version = load_catalog(self.db).version
        reclaim(self.db, 'reclaim', {first.result_id}, version)
        with self.assertRaisesRegex(ValueError, 'stale'):
            reclaim(self.db, 'late-reclaim', set(), version)
        # Retrying the bound request with a different catalog version conflicts.
        with self.assertRaises(RequestConflict):
            reclaim(self.db, 'reclaim', {first.result_id}, version + 1)

    def test_reclaim_is_idempotent_and_recovers_after_reopen(self):
        first, _, (_, b1, _, _) = self._ab_ledger()
        version = load_catalog(self.db).version
        with publication_transaction(self.db):
            bind_request_parameters(self.db, 'interrupted', 'reclaim',
                                    [sorted({first.result_id}), version])
        self.db.close()
        reopened = self.connect()
        initialize_database(reopened)
        self.assertEqual(recover_incomplete_attempts(reopened), ('interrupted',))
        self.assertEqual(load_attempt(reopened, 'interrupted').status, 'failed')
        receipt = reclaim(reopened, 'interrupted', {first.result_id}, version)
        self.assertEqual([item.event.event_id for item in receipt.events], ['b1'])
        self.assertEqual(reclaim(reopened, 'interrupted', {first.result_id}, version), receipt)
        self.assertEqual(load_attempt(reopened, 'interrupted').status, 'complete')

    def test_unexpired_intermediate_snapshot_keeps_full_originals(self):
        first = self.put('first', event('a1', 1, 12), event('b1', 1, 3, field='B'))
        middle = self.put('middle', event('b2', 2, 5, field='B'))
        self.put('latest', event('a3', 3, 11))
        # Only the first result expires; the unexpired middle snapshot still
        # references b1, so b1 must be protected.
        receipt = reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        self.assertEqual(receipt.events, ())
        self.assertFalse(find_raw_event(self.db, identify_event(event('b1', 1, 3, field='B'))).reclaimed)
        historic = query_history(self.db, middle.result_id)
        self.assertEqual({item.original.identity.event_id for item in historic.evidence},
                         {'a1', 'b1', 'b2'})
        # Once the middle result also expires, its readback protection ends.
        # b1 is owned by result-1; with result-2 now reclaimed too, no unexpired
        # non-current snapshot protects b1 and the current closure excludes it.
        second = reclaim(self.db, 'reclaim-2', {middle.result_id},
                         load_catalog(self.db).version)
        self.assertIn('b1', [item.event.event_id for item in second.events])
        self.assertTrue(result_is_reclaimed(self.db, middle.result_id))
        with self.assertRaises(ReclaimedResultError):
            query_history(self.db, middle.result_id)
        # Current evidence still reconciles without the removed original.
        query_current(self.db)

    def test_invalid_reclaim_requests(self):
        first, current, _ = self._ab_ledger()
        version = load_catalog(self.db).version
        with self.assertRaises(LookupError):
            reclaim(self.db, 'unknown', {'result-999'}, version)
        with self.assertRaises(ValueError):
            reclaim(self.db, 'current', {current.result_id}, version)
        with self.assertRaises(ValueError):
            reclaim(self.db, 'bad-version', {first.result_id}, -1)
        with self.assertRaises(ValueError):
            reclaim(self.db, 'bad-expired', first.result_id, version)
        with self.assertRaises(ValueError):
            reclaim(self.db, 'dup', [first.result_id, first.result_id], version)

    def test_normal_behavior_unchanged_before_reclaim(self):
        first, current, _ = self._ab_ledger()
        # Historical readback with full evidence works exactly as before reclaim.
        historic = query_history(self.db, first.result_id)
        self.assertEqual([item.original.identity.event_id for item in historic.evidence],
                         ['a1', 'b1'])
        self.assertEqual(historic.daily[0].total_mg, '15')
        # Pure resubmission still returns the original receipt before reclamation.
        again = self.put('first', event('a1', 1, 12), event('b1', 1, 3, field='B'))
        self.assertEqual(again, find_request_receipt(self.db, 'first'))
        self.assertEqual(query_current(self.db).result_id, current.result_id)

    def test_origin_segment_numbers_survive_reclaim(self):
        first, _, (a1, b1, _, _) = self._ab_ledger()
        b1_identity = identify_event(b1)
        original_location = resolve_segment(self.db, b1_identity.key)
        reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        after = resolve_segment(self.db, b1_identity.key)
        self.assertEqual(after.original, original_location.original)
        # The reclaimed identity still has a catalog location (sequence occupancy).
        self.assertEqual(after.active, original_location.active)
        # Reconciliation of the current ledger still passes end to end.
        self.assertIsNotNone(query_current(self.db).result_id)

    def test_reclaim_does_not_break_later_compaction(self):
        first, _, _ = self._ab_ledger()
        reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        before = query_current(self.db)
        maintenance = self.compact(DAY, DAY)
        self.assertEqual(query_current(self.db).facts, before.facts)
        self.assertEqual(query_current(self.db).daily, before.daily)
        self.assertTrue(maintenance.catalog_version >= 1)

    def test_transitive_predecessors_are_retained(self):
        old = event('old', 1, 12)
        mid = event('mid', 2, 11, requires=[identify_event(old).parts])
        head = event('head', 3, 10, requires=[identify_event(mid).parts])
        first = self.put('first', old)
        second = self.put('second', mid)
        self.put('third', head)
        # Both historical results expire, but old is a transitive explicit
        # predecessor of the current head: the whole chain must stay.
        receipt = reclaim(self.db, 'reclaim', {first.result_id, second.result_id},
                          load_catalog(self.db).version)
        self.assertEqual(receipt.events, ())
        for record in (old, mid, head):
            self.assertFalse(find_raw_event(self.db, identify_event(record)).reclaimed)

    def test_missing_only_revisions_keep_sequence_occupancy(self):
        missing = event('gap', 2)  # payload {} => missing, no fact row
        first = self.put('first', missing)
        self.put('later', event('later', 3, 5, field='B'))
        # The missing revision never wrote a value but still occupies seq 2 and
        # constrains admission, so it cannot be reclaimed.
        receipt = reclaim(self.db, 'reclaim', {first.result_id}, load_catalog(self.db).version)
        self.assertEqual(receipt.events, ())
        self.assertFalse(find_raw_event(self.db, identify_event(missing)).reclaimed)

    def compact(self, start, end):
        return publish_compaction(self.db, 'compact', start, end, load_catalog(self.db).version)
