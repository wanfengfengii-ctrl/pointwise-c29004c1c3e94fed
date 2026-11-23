from dataclasses import dataclass

from .apply import materialize_facts
from .order import ReplayOrderError, order_effective_events
from .snapshot import FactSnapshot


@dataclass(frozen=True)
class ReplayResult:
    snapshot: FactSnapshot
    affected_entities: tuple[str, ...]


def replay_events(revisions, version, *, expected_keys=None, changed_keys=None):
    ordered = order_effective_events(revisions)
    keys = tuple(item.identity.key for item in ordered)
    if expected_keys is not None and set(keys) != set(expected_keys):
        raise ReplayOrderError('replay inputs do not cover the required event manifest')
    changed = set(keys) if changed_keys is None else set(changed_keys)
    if not changed <= set(keys):
        raise ReplayOrderError('changed events are missing from replay inputs')
    facts, effects = materialize_facts(ordered)
    entities = {item.identity.entity for item in ordered if item.identity.key in changed}
    return ReplayResult(FactSnapshot(version, keys, facts, effects), tuple(sorted(entities)))


def compare_snapshot_facts(expected, rebuilt):
    if expected.version != rebuilt.version or expected.members != rebuilt.members:
        raise ReplayOrderError('rebuilt snapshot has different event membership or order')
    if expected.facts != rebuilt.facts:
        raise ReplayOrderError('rebuilt facts differ from the published version')
    if expected.effects != rebuilt.effects:
        raise ReplayOrderError('rebuilt effect evidence differs from the published version')
