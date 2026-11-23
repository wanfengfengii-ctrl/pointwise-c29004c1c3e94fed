from src.aggregate.daily import build_daily_totals
from src.facts.replay import compare_snapshot_facts, replay_events
from src.intake.normalize import FieldIntent


class ReconciliationError(ValueError):
    pass


def verify_reconciliation(snapshot, contributions, totals):
    supported = {fact.field_key: fact for fact in snapshot.facts if fact.state is FieldIntent.VALUE}
    supplied = {item.field_key: item for item in contributions}
    if len(supplied) != len(contributions) or supplied.keys() != supported.keys():
        raise ReconciliationError('contributions do not cover exactly the live value facts')
    for key, fact in supported.items():
        item = supplied[key]
        if (item.version, item.day, item.event, item.value, item.unit) != (
                snapshot.version, fact.day, fact.event, fact.value, 'mg'):
            raise ReconciliationError(f'contribution differs from the published fact for {key}')
    if tuple(totals) != build_daily_totals(snapshot.version, contributions):
        raise ReconciliationError('daily totals differ from the published contribution ledger')


def verify_query_evidence(snapshot, entries):
    keys = tuple(entry.raw.identity.key for entry in entries)
    if keys != snapshot.members or any(entry.accepted_version > snapshot.version for entry in entries):
        raise ReconciliationError('event evidence does not belong to this snapshot')
    rebuilt = replay_events((entry.revision for entry in entries), snapshot.version,
                            expected_keys=snapshot.members)
    try:
        compare_snapshot_facts(snapshot, rebuilt.snapshot)
    except ValueError as error:
        raise ReconciliationError(f'cannot reconcile snapshot evidence: {error}') from error
