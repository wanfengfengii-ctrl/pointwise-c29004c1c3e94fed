import json
from dataclasses import dataclass

from .normalize import FieldRevision, encode_field_revision, normalize_event, restore_field_revision
from .validation import IntakeError, RawEvent, validate_raw_event


@dataclass(frozen=True)
class RawEntry:
    raw: RawEvent
    revision: FieldRevision
    accepted_version: int


def initialize_raw_store(db):
    db.execute('''CREATE TABLE IF NOT EXISTS raw_events (
        event_key TEXT PRIMARY KEY, raw_json TEXT NOT NULL, revision_json TEXT NOT NULL,
        accepted_version INTEGER NOT NULL CHECK (accepted_version > 0)
    )''')


def store_raw_events(db, entries):
    db.executemany('INSERT INTO raw_events VALUES (?, ?, ?, ?)', [
        (entry.raw.identity.key, json.dumps(entry.raw.to_record(), sort_keys=True),
         encode_field_revision(entry.revision), entry.accepted_version)
        for entry in entries
    ])


def _restore(row):
    key, raw_json, revision_json, version = row
    try:
        raw = validate_raw_event(json.loads(raw_json))
        revision = restore_field_revision(revision_json)
        if raw.identity.key != key or normalize_event(raw) != revision:
            raise IntakeError('stored original and revision do not agree')
    except (ValueError, TypeError, RecursionError) as error:
        raise IntakeError(f'event evidence is damaged for {key}: {error}') from error
    return RawEntry(raw, revision, version)


def find_raw_event(db, identity):
    row = db.execute('SELECT * FROM raw_events WHERE event_key = ?', (identity.key,)).fetchone()
    return None if row is None else _restore(row)


def load_raw_events(db, keys=None):
    if keys is None:
        return tuple(_restore(row) for row in db.execute('SELECT * FROM raw_events ORDER BY event_key'))
    entries = []
    for key in keys:
        row = db.execute('SELECT * FROM raw_events WHERE event_key = ?', (key,)).fetchone()
        if row is None:
            raise IntakeError(f'missing original event {key}')
        entries.append(_restore(row))
    return tuple(entries)
