from pathlib import Path

from src.intake.identity import identify_event
from src.publish.coordinator import publish_batch
from src.publish.database import initialize_database
from src.query.current import query_current
from src.query.history import query_history

from tests import LakeCase, event


class TestReplay(LakeCase):
    def _compare_arrival_orders(self, final_value):
        ordered_db = self.connect(Path(self.directory.name) / 'ordered.sqlite')
        initialize_database(ordered_db)
        first = event('first', 1, 12)
        second = event('second', 2, 9)
        third = event('third', 3, final_value)
        base = self.put('initial', first)
        arrived = self.put('third', third)
        late = self.put('second', second)
        for record in (first, second, third):
            publish_batch(ordered_db, record['event_id'], [record])
        by_arrival = query_current(self.db)
        ordered = query_current(ordered_db)
        self.assertEqual(by_arrival.facts, ordered.facts)
        self.assertEqual(by_arrival.daily, ordered.daily)
        self.assertEqual(by_arrival.contributions, ordered.contributions)
        self.assertEqual([(item.original, item.revision, item.effect) for item in by_arrival.evidence],
                         [(item.original, item.revision, item.effect) for item in ordered.evidence])
        self.assertEqual([item.original.identity.event_id for item in by_arrival.evidence],
                         ['first', 'second', 'third'])
        self.assertEqual(by_arrival.facts[0].event, identify_event(third))
        self.assertEqual(query_history(self.db, base.result_id).facts[0].value, '12')
        self.assertEqual(query_history(self.db, arrived.result_id).facts, by_arrival.facts)
        self.assertNotEqual(arrived.result_id, late.result_id)
        self.assertEqual(late.affected_days, ())
        return by_arrival

    def test_order_independent_replay(self):
        result = self._compare_arrival_orders(7)
        self.assertEqual(result.facts[0].value, '7')
        self.assertEqual(result.daily[0].total_mg, '7')

    def test_late_clear_replay(self):
        result = self._compare_arrival_orders(None)
        self.assertEqual(result.facts[0].state, 'clear')
        self.assertIsNone(result.facts[0].value)
        self.assertEqual(result.daily, ())
        self.assertEqual(result.evidence[-1].effect.before.event_id, 'second')
        self.assertEqual(result.evidence[-1].effect.after.event_id, 'third')

    def test_duplicate_event_under_different_requests(self):
        initial = event('first', 1, 12)
        clear = event('clear', 3, None)
        first = self.put('first', initial)
        cleared = self.put('clear', clear)
        unchanged = query_current(self.db)
        repeated = self.put('retransmit', clear, clear)
        self.assertEqual(repeated.result_id, cleared.result_id)
        self.assertEqual(repeated.events, cleared.events)
        multiple = self.put('mixed-retry', initial, clear)
        self.assertIsNone(multiple.result_id)
        self.assertEqual({item.result_id for item in multiple.events}, {first.result_id, cleared.result_id})
        self.assertEqual(self.counts()[:2], (2, 2))
        self.assertEqual(query_current(self.db), unchanged)
        mixed = self.put('old-and-new', initial, event('new', 1, 5, field='B'))
        self.assertEqual({item.request_id for item in mixed.events}, {'first', 'old-and-new'})
        self.assertEqual(self.counts()[:2], (3, 3))
        self.assertEqual(query_current(self.db).daily[0].total_mg, '5')
