from dataclasses import dataclass

from src.aggregate.contributions import Contribution
from src.aggregate.daily import DailyAggregate, build_daily_totals, load_contributions, load_daily_totals
from src.facts.apply import FieldFact
from src.facts.effects import FieldEffect
from src.facts.snapshot import load_snapshot
from src.intake.identity import require_name
from src.intake.normalize import FieldRevision
from src.intake.raw_store import load_raw_events
from src.intake.validation import RawEvent, validate_range
from src.partitions.catalog import resolve_segment
from src.partitions.layout import read_segment
from src.publish.database import read_transaction
from src.publish.receipts import resolve_result_receipt, result_for_version

from .reconcile import ReconciliationError, verify_query_evidence, verify_reconciliation


@dataclass(frozen=True)
class EventEvidence:
    original: RawEvent
    revision: FieldRevision
    effect: FieldEffect
    accepted_result: str
    origin_segment: int


@dataclass(frozen=True)
class QueryResult:
    result_id: str
    version: int
    facts: tuple[FieldFact, ...]
    contributions: tuple[Contribution, ...]
    daily: tuple[DailyAggregate, ...]
    evidence: tuple[EventEvidence, ...]


def _evidence(db, snapshot, entries):
    segment_cache = {}
    result_cache = {}

    def locate(segment_id):
        if segment_id not in segment_cache:
            segment = read_segment(db, segment_id)
            segment_cache[segment_id] = segment, {item.event_key: item for item in segment.members}
        return segment_cache[segment_id]

    evidence = []
    for entry, effect in zip(entries, snapshot.effects, strict=True):
        key = entry.raw.identity.key
        location = resolve_segment(db, key)
        active, live_members = locate(location.active)
        _, original_members = locate(location.original)
        live = live_members.get(key)
        original = original_members.get(key)
        if (active.retired or live is None or original is None or live != original
                or live.raw_ref != key or live.revision != entry.revision):
            raise ReconciliationError(f'incomplete original or active segment evidence for {key}')
        if entry.accepted_version not in result_cache:
            result_id, _ = result_for_version(db, entry.accepted_version)
            resolve_result_receipt(db, result_id)
            result_cache[entry.accepted_version] = result_id
        evidence.append(EventEvidence(entry.raw, entry.revision, effect,
                                      result_cache[entry.accepted_version], location.original))
    return tuple(evidence)


def query_scope(start, end, entities):
    validate_range(start, end)
    if isinstance(entities, (str, bytes)):
        raise ValueError('entities must be a collection of entity identifiers')
    selected = None if entities is None else frozenset(require_name(item, 'entity') for item in entities)

    def includes(entity, day):
        return ((selected is None or entity in selected) and (start is None or day >= start)
                and (end is None or day <= end))

    return includes


def read_result(db, result_id, includes):
    receipt = resolve_result_receipt(db, result_id)
    snapshot = load_snapshot(db, receipt.version)
    entries = load_raw_events(db, snapshot.members)
    verify_query_evidence(snapshot, entries)
    evidence = _evidence(db, snapshot, entries)
    contributions = load_contributions(db, snapshot.version)
    totals = load_daily_totals(db, snapshot.version)
    verify_reconciliation(snapshot, contributions, totals)
    selected = tuple(item for item in contributions if includes(item.entity, item.day))
    return QueryResult(result_id, snapshot.version,
                       tuple(fact for fact in snapshot.facts if includes(fact.entity, fact.day)),
                       selected, build_daily_totals(snapshot.version, selected),
                       tuple(item for item in evidence if includes(item.original.identity.entity, item.original.day)))


def query_history(db, result_id, *, start=None, end=None, entities=None):
    require_name(result_id, 'result_id')
    includes = query_scope(start, end, entities)
    with read_transaction(db):
        return read_result(db, result_id, includes)
