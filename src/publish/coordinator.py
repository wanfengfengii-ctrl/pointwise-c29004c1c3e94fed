import sqlite3

from src.aggregate.contributions import affected_contribution_dates, derive_contributions
from src.aggregate.daily import build_daily_totals, load_contributions, load_daily_totals, save_daily_totals
from src.facts.replay import compare_snapshot_facts, replay_events
from src.facts.snapshot import load_snapshot, save_snapshot
from src.intake.accept import accept_batch, unique_events
from src.intake.raw_store import load_raw_events
from src.intake.validation import validate_batch, validate_day, validate_range
from src.partitions.catalog import load_catalog, publish_partition_update, publish_replacement, read_active_segments
from src.partitions.compact import plan_compaction, stage_replacement
from src.partitions.layout import stage_partition_segments
from src.partitions.retire import retire_replaced_segments
from src.query.reconcile import verify_query_evidence, verify_reconciliation

from .attempts import begin_attempt, bind_request_parameters, complete_attempt, record_failure
from .receipts import (EventReceipt, PublishReceipt, find_request_receipt, load_current_result,
                       resolve_result_receipt, result_for_version, save_success_receipt)
from .rollback import publication_transaction


def _run_publication(db, request_id, kind, parameters, action):
    # Keep the binding after a failed publication so a retry has the same meaning.
    with publication_transaction(db):
        bind_request_parameters(db, request_id, kind, parameters)
        existing = find_request_receipt(db, request_id)
    if existing is not None:
        return existing
    try:
        with publication_transaction(db):
            existing = find_request_receipt(db, request_id)
            if existing is not None:
                return existing
            begin_attempt(db, request_id)
            receipt, make_current = action()
            save_success_receipt(db, receipt, make_current=make_current)
            complete_attempt(db, request_id)
            return receipt
    except (ValueError, LookupError, sqlite3.Error) as error:
        try:
            with publication_transaction(db):
                if find_request_receipt(db, request_id) is None:
                    record_failure(db, request_id, f'{type(error).__name__}: {error}')
        except sqlite3.Error as diagnostic_error:
            error.add_note(f'could not persist attempt diagnostics: {diagnostic_error}')
        raise


def _current_snapshot(db):
    result_id = load_current_result(db)
    if result_id is None:
        return None, None
    receipt = resolve_result_receipt(db, result_id)
    return result_id, load_snapshot(db, receipt.version)


def _published_inputs(db, current, catalog):
    segments = read_active_segments(db, catalog)
    members = {item.event_key: item for segment in segments.values() for item in segment.members}
    expected = () if current is None else current.members
    if set(members) != set(expected):
        raise ValueError('catalog membership differs from the current snapshot')
    entries = load_raw_events(db, expected)
    for entry in entries:
        key = entry.raw.identity.key
        member = members[key]
        if member.raw_ref != key or member.revision != entry.revision:
            raise ValueError(f'active segment differs from the original event {key}')
    if current is not None:
        verify_query_evidence(current, entries)
    return segments, members


def _event_receipts(db, entries):
    by_version = {}
    receipts = []
    for entry in entries:
        if entry.accepted_version not in by_version:
            by_version[entry.accepted_version] = result_for_version(db, entry.accepted_version)
        result_id, request_id = by_version[entry.accepted_version]
        receipts.append(EventReceipt(entry.raw.identity, result_id, request_id))
    return receipts


def _publish_batch(db, request_id, events):
    _, previous = _current_snapshot(db)
    catalog = load_catalog(db)
    version = 1 if previous is None else previous.version + 1
    accepted = accept_batch(db, events, version)
    repeated = _event_receipts(db, accepted.repeated)
    if not accepted.added:
        originals = {item.result_id for item in repeated}
        result_id = next(iter(originals)) if len(originals) == 1 else None
        original_version = resolve_result_receipt(db, result_id).version if result_id is not None else None
        return PublishReceipt(request_id, 'batch', result_id, original_version, tuple(repeated)), False

    _, prior_members = _published_inputs(db, previous, catalog)
    revisions = tuple(entry.revision for entry in accepted.added)
    changed = {item.identity.key for item in revisions}
    all_revisions = [member.revision for member in prior_members.values()] + list(revisions)
    replayed = replay_events(all_revisions, version,
                             expected_keys=set(prior_members) | changed, changed_keys=changed)
    snapshot = replayed.snapshot
    contributions = derive_contributions(snapshot)
    prior_contributions = () if previous is None else load_contributions(db, previous.version)
    affected_days = affected_contribution_dates(prior_contributions, contributions)
    totals = build_daily_totals(version, contributions)
    verify_reconciliation(snapshot, contributions, totals)

    staged = stage_partition_segments(db, revisions)
    save_snapshot(db, snapshot)
    save_daily_totals(db, totals, contributions)
    verify_reconciliation(snapshot, load_contributions(db, version), load_daily_totals(db, version))
    catalog_version = publish_partition_update(db, catalog.version, staged)
    result_id = f'result-{version}'
    accepted_receipts = repeated + [EventReceipt(item.identity, result_id, request_id) for item in revisions]
    receipts = tuple(sorted(accepted_receipts, key=lambda item: item.event.key))
    return PublishReceipt(request_id, 'batch', result_id, version, receipts,
                          catalog_version, affected_days=affected_days), True


def publish_batch(db, request_id, records):
    events = unique_events(validate_batch(records))
    parameters = [event.to_record() for event in events]
    return _run_publication(db, request_id, 'batch', parameters,
                            lambda: _publish_batch(db, request_id, events))


def _publish_compaction(db, request_id, start, end, source_version):
    plan = plan_compaction(db, request_id, start, end, source_version)
    result_id, current = _current_snapshot(db)
    if current is None:
        raise ValueError('compaction requires a published data snapshot')
    active, _ = _published_inputs(db, current, load_catalog(db))
    replacements = stage_replacement(db, plan)
    for replacement in replacements:
        for source_id in replacement.source_ids:
            del active[source_id]
        active[replacement.content.segment_id] = replacement.content

    revisions = [item.revision for segment in active.values() for item in segment.members]
    replayed = replay_events(revisions, current.version, expected_keys=current.members)
    compare_snapshot_facts(current, replayed.snapshot)
    contributions = load_contributions(db, current.version)
    totals = load_daily_totals(db, current.version)
    verify_reconciliation(current, contributions, totals)
    rebuilt_contributions = derive_contributions(replayed.snapshot)
    if rebuilt_contributions != contributions or build_daily_totals(current.version, rebuilt_contributions) != totals:
        raise ValueError('replacement changed the rebuilt daily ledger')
    catalog_version = publish_replacement(db, plan.source_version, replacements)
    retire_replaced_segments(db, replacements)
    edges = tuple(sorted((source_id, replacement.content.segment_id)
                         for replacement in replacements for source_id in replacement.source_ids))
    return PublishReceipt(request_id, 'compaction', result_id, current.version,
                          catalog_version=catalog_version, replacements=edges), False


def publish_compaction(db, request_id, start, end, source_version):
    validate_day(start)
    validate_day(end)
    validate_range(start, end)
    if type(source_version) is not int or source_version < 0:
        raise ValueError('source_version must be a nonnegative integer')
    return _run_publication(db, request_id, 'compaction', [start, end, source_version],
                            lambda: _publish_compaction(db, request_id, start, end, source_version))
