import json
import sqlite3
from collections import defaultdict

from src.facts.order import order_effective_events
from src.facts.snapshot import load_snapshot
from src.intake.identity import EventIdentity, require_name
from src.intake.normalize import FieldIntent
from src.intake.raw_store import entry_identity, load_raw_events, mark_events_reclaimed, record_payload_digest
from src.partitions.catalog import load_catalog, publish_reclamation
from src.publish.attempts import begin_attempt, bind_request_parameters, complete_attempt, record_failure
from src.publish.receipts import (EventReceipt, PublishReceipt, find_request_receipt, load_current_result,
                                  resolve_result_receipt, result_for_version, save_success_receipt)
from src.publish.rollback import publication_transaction


class ReclaimedResultError(LookupError):
    """Raised when a history result whose originals were reclaimed is queried."""


def initialize_retention(db):
    db.execute('''CREATE TABLE IF NOT EXISTS reclaimed_bodies (
        event_key TEXT PRIMARY KEY, request_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL, catalog_version INTEGER NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS reclaimed_results (
        result_id TEXT PRIMARY KEY, version INTEGER NOT NULL, request_id TEXT NOT NULL,
        catalog_version INTEGER NOT NULL
    )''')


def result_is_reclaimed(db, result_id):
    return db.execute('SELECT 1 FROM reclaimed_results WHERE result_id = ?', (result_id,)).fetchone() is not None


def _validate_expired(expired):
    if isinstance(expired, (str, bytes)) or not isinstance(expired, (set, frozenset, tuple, list)):
        raise ValueError('expired must be a collection of historical result identifiers')
    normalized = set()
    for result_id in expired:
        require_name(result_id, 'expired result')
        if result_id in normalized:
            raise ValueError(f'expired result listed more than once: {result_id}')
        normalized.add(result_id)
    return frozenset(normalized)


def _retention_closure(entries):
    """Current-ledger closure: for each field the last value/clear event (or,
    for never-written fields, every occupying revision), the events after it
    that still constrain admission, and the recursive explicit predecessors of
    every retained event. Earlier overridden revisions are only pulled in via
    explicit requirements."""
    ordered = order_effective_events(tuple(entry.revision for entry in entries))
    by_identity = {entry_identity(entry): entry for entry in entries}
    positions = defaultdict(list)
    for revision in ordered:
        positions[revision.identity.field_key].append(revision.identity)
    retained = set()
    for identities in positions.values():
        terminal_index = -1
        for index, identity in enumerate(identities):
            if by_identity[identity].revision.intent in (FieldIntent.VALUE, FieldIntent.CLEAR):
                terminal_index = index
        if terminal_index < 0:
            # Only missing revisions were seen; each still occupies its
            # sequence slot and constrains admission.
            retained.update(identities)
        else:
            retained.update(identities[terminal_index:])
    pending = list(retained)
    while pending:
        identity = pending.pop()
        for required in by_identity[identity].revision.requires:
            if required not in retained:
                retained.add(required)
                pending.append(required)
    unknown = retained - set(by_identity)
    if unknown:
        raise ValueError(f'retention closure references missing event {next(iter(unknown)).key}')
    return retained


def _reclaim(db, request_id, expired, expected_version):
    catalog = load_catalog(db)
    if catalog.version != expected_version:
        raise ValueError(f'catalog source version {expected_version} is stale')
    current_id = load_current_result(db)
    if current_id is None:
        raise ValueError('reclamation requires a published data snapshot')
    if current_id in expired:
        raise ValueError(f'current result {current_id} cannot be reclaimed')
    current_receipt = resolve_result_receipt(db, current_id)
    result_versions = dict(db.execute('SELECT result_id, version FROM results'))
    for result_id in expired:
        if result_id not in result_versions:
            raise LookupError(f'unknown expired result {result_id!r}')
        if result_is_reclaimed(db, result_id):
            raise ValueError(f'result was already reclaimed: {result_id}')

    snapshot = load_snapshot(db, current_receipt.version)
    entries = load_raw_events(db, snapshot.members)
    closure = _retention_closure(entries)
    already_reclaimed = {row[0] for row in db.execute('SELECT result_id FROM reclaimed_results')}
    # A body's accepting result must itself be reclaimed (in this call or an
    # earlier one) before the body may be removed.
    reclaiming_versions = {result_versions[result_id]
                           for result_id in expired | already_reclaimed}
    # Unexpired historical (non-current) results keep the complete original set
    # their fixed snapshots need for readback. The current result may omit the
    # removed bodies, and expired results no longer require full readback, so
    # both are excluded from this protection set.
    if reclaiming_versions:
        placeholders = ','.join('?' for _ in reclaiming_versions)
        protected_rows = tuple(db.execute(
            f'''SELECT members_json FROM fact_snapshots
                WHERE version <> ? AND version NOT IN ({placeholders})''',
            (current_receipt.version, *sorted(reclaiming_versions))))
    else:
        protected_rows = tuple(db.execute(
            'SELECT members_json FROM fact_snapshots WHERE version <> ?',
            (current_receipt.version,)))
    protected = set()
    for (members_json,) in protected_rows:
        for key in json.loads(members_json):
            protected.add(EventIdentity.from_key(key))
    # A body is removed when the result that accepted it is reclaimed (now or
    # previously), the current ledger closure does not retain it, and no
    # unexpired historical snapshot still needs it for readback.
    removable = [entry for entry in entries
                 if (not entry.reclaimed and entry.accepted_version in reclaiming_versions
                     and entry_identity(entry) not in closure
                     and entry_identity(entry) not in protected)]
    if len(removable) != len({entry_identity(entry) for entry in removable}):
        raise ValueError('duplicate reclamation targets')

    by_version = {}
    for entry in removable:
        if entry.accepted_version not in by_version:
            by_version[entry.accepted_version] = result_for_version(db, entry.accepted_version)
    receipts = tuple(sorted((EventReceipt(entry_identity(entry), *by_version[entry.accepted_version])
                             for entry in removable), key=lambda item: item.event.key))
    # Capture the body summary before the originals are replaced by tombstones.
    body_rows = [(entry_identity(entry).key, request_id,
                  record_payload_digest(entry.raw), expected_version + 1) for entry in removable]
    mark_events_reclaimed(db, removable)
    catalog_version = publish_reclamation(db, expected_version)
    db.executemany('INSERT INTO reclaimed_bodies VALUES (?, ?, ?, ?)', body_rows)
    db.executemany('INSERT INTO reclaimed_results VALUES (?, ?, ?, ?)', [
        (result_id, result_versions[result_id], request_id, catalog_version)
        for result_id in sorted(expired)
    ])
    return PublishReceipt(request_id, 'reclaim', None, None, receipts, catalog_version), False


def _run_reclaim(db, request_id, expired, expected_version):
    with publication_transaction(db):
        bind_request_parameters(db, request_id, 'reclaim',
                                [sorted(expired), expected_version])
        existing = find_request_receipt(db, request_id)
    if existing is not None:
        return existing
    try:
        with publication_transaction(db):
            if find_request_receipt(db, request_id) is not None:
                return find_request_receipt(db, request_id)
            begin_attempt(db, request_id)
            receipt, make_current = _reclaim(db, request_id, expired, expected_version)
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


def reclaim(db, request, expired, version):
    """Reclaim original weighing bodies for confirmed-expired historical
    results, keeping every body the current ledger's retention closure and the
    unexpired historical results still need for readback."""
    require_name(request, 'request')
    normalized = _validate_expired(expired)
    if type(version) is not int or version < 0:
        raise ValueError('version must be a nonnegative integer')
    return _run_reclaim(db, request, normalized, version)
