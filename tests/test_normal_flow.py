import json

from src.facts.effects import effect_for_event
from src.facts.snapshot import load_snapshot
from src.intake.identity import identify_event
from src.intake.normalize import FieldIntent
from src.publish.receipts import find_request_receipt
from src.query.current import query_current
from src.query.history import query_history

from tests import DAY, NEXT_DAY, LakeCase, event


class TestNormalFlow(LakeCase):
    def test_three_state_update(self):
        first = event('first', 1, payload=' { "A": 12.00 } ')
        omitted = event('zero', 2, payload='{"B":0}')
        zero = event('zero', 2, field='B', payload='{"B":0}')
        clear = event('clear', 3, None)
        before = self.put('first', first)
        retained = self.put('zero', zero, omitted)
        retained_result = query_history(self.db, retained.result_id)
        self.assertEqual([(fact.field, fact.value, fact.event.event_id) for fact in retained_result.facts],
                         [('A', '12.00', 'first'), ('B', '0', 'zero')])
        self.assertEqual([(item.field, item.value) for item in retained_result.contributions], [('A', '12.00'), ('B', '0')])
        self.assertEqual([(item.total_mg, item.count) for item in retained_result.daily], [('12', 2)])
        published = self.put('clear', clear)
        result = query_current(self.db)
        evidence = {item.original.identity: item for item in result.evidence}
        for record, intent in ((first, FieldIntent.VALUE), (omitted, FieldIntent.MISSING),
                               (zero, FieldIntent.VALUE), (clear, FieldIntent.CLEAR)):
            item = evidence[identify_event(record)]
            self.assertEqual(item.original.payload, record['payload'])
            self.assertEqual(item.revision.intent, intent)
        self.assertNotIn('A', json.loads(evidence[identify_event(omitted)].original.payload))
        clear_body = json.loads(evidence[identify_event(clear)].original.payload)
        self.assertIn('A', clear_body)
        self.assertIsNone(clear_body['A'])
        effect = effect_for_event(load_snapshot(self.db, published.version).effects, identify_event(omitted))
        self.assertEqual(effect.before, identify_event(first))
        self.assertEqual(effect.after, identify_event(first))
        self.assertEqual(result.facts[0].state, FieldIntent.CLEAR)
        self.assertEqual(result.facts[0].event, identify_event(clear))
        self.assertEqual([(item.total_mg, item.count) for item in result.daily], [('0', 1)])
        self.assertEqual(evidence[identify_event(first)].accepted_result, before.result_id)
        self.assertEqual(find_request_receipt(self.db, 'clear'), published)

    def test_clear_removes_contribution(self):
        old = event('old', 1, 12)
        clear = event('clear', 4, None)
        other = event('other', 5, 5, field='B')
        initial = self.put('old', old)
        latest = self.put('latest', clear, other)
        result = query_history(self.db, latest.result_id)
        facts = {fact.field: (fact.state, fact.value, fact.event.event_id) for fact in result.facts}
        expected_facts = {'A': ('clear', None, 'clear'), 'B': ('value', '5', 'other')}
        self.assertEqual(facts, expected_facts)
        contribution = [(item.field, item.value, item.event.event_id, item.version) for item in result.contributions]
        self.assertEqual(contribution, [('B', '5', 'other', latest.version)])
        totals = [(item.day, item.total_mg, item.count) for item in result.daily]
        self.assertEqual(totals, [(DAY, '5', 1)])
        self.assertEqual(query_history(self.db, initial.result_id).daily[0].total_mg, '12')
        self.assertEqual(latest.affected_days, (DAY,))
        evidence = next(item for item in result.evidence if item.original.identity == identify_event(clear))
        self.assertEqual(evidence.effect.before, identify_event(old))
        self.assertEqual(evidence.effect.after, identify_event(clear))
        print('POINTWISE_SCENARIO ' + json.dumps(dict(
            case_id='V2.contribution_retraction', input=[old, clear, other],
            expected=expected_facts, actual=facts,
            downstream_expected=[(DAY, '5', 1)], downstream_actual=totals,
            code_paths=['src/intake/normalize.py', 'src/facts/apply.py', 'src/aggregate/contributions.py',
                        'src/query/history.py'],
            oracle='手算清空 A 撤掉 12 mg，B 的 5 mg 是唯一有效贡献。'), ensure_ascii=False))

    def test_exact_mass_date_move_and_scoped_query(self):
        self.put('decimal', event('e', 1, payload='{"A":0.1}'),
                 event('x', 1, entity='X', payload='{"A":0.200000000000000000000000000001}'),
                 event('never', 1, entity='N'))
        old = query_current(self.db)
        self.assertEqual([(item.day, item.total_mg) for item in old.daily],
                         [(DAY, '0.300000000000000000000000000001')])
        self.assertEqual({item.entity for item in old.facts}, {'E', 'X'})
        moved = self.put('move', event('new-day', 2, day=NEXT_DAY, payload='{"A":0.10}'))
        self.assertEqual(moved.affected_days, (DAY, NEXT_DAY))
        self.assertEqual([(item.day, item.total_mg) for item in query_current(self.db).daily],
                         [(DAY, '0.200000000000000000000000000001'), (NEXT_DAY, '0.1')])
        scoped = query_current(self.db, start=NEXT_DAY, end=NEXT_DAY, entities=['E'])
        self.assertEqual([(item.entity, item.day, item.value) for item in scoped.facts], [('E', NEXT_DAY, '0.10')])
        self.assertEqual([(item.entity, item.value) for item in scoped.contributions], [('E', '0.10')])
        self.assertEqual([(item.total_mg, item.count) for item in scoped.daily], [('0.1', 1)])
        self.assertEqual(query_current(self.db, end=DAY, entities=['E']).daily, ())
        self.assertEqual(query_history(self.db, old.result_id), old)
