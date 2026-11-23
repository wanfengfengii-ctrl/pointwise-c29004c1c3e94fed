import hashlib
import json
from dataclasses import dataclass

from .normalize import FieldRevision, encode_field_revision, normalize_event, restore_field_revision
from .validation import IntakeError, RawEvent, validate_raw_event

TOMBSTONE_RAW_JSON = json.dumps({'reclaimed': True}, sort_keys=True, separators=(',', ':'))


@dataclass(frozen=True)
class RawEntry:
    raw: RawEvent
    revision: FieldRevision
    accepted_version: int

    @property
    def identity(self):
        return self.raw.identity

    @property
    def key(self):
        return self.raw.identity.key

    @property
    def reclaimed(self):
        return False


@dataclass(frozen=True)
class ReclaimedEntry:
    identity: object
    revision: FieldRevision
    accepted_version: int
    payload_digest: str

    @property
    def key(self):
        return self.identity.key

    @property
    def reclaimed(self):
        return True


def entry_identity(entry):
    return entry.identity


def record_payload_digest(raw):
    canonical = json.dumps(raw.to_record(), sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def initialize_raw_store(db):
    db.execute('''CREATE TABLE IF NOT EXISTS raw_events (
        event_key TEXT PRIMARY KEY, raw_json TEXT NOT NULL, revision_json TEXT NOT NULL,
        accepted_version INTEGER NOT NULL CHECK (accepted_version > 0),
        reclaimed INTEGER NOT NULL DEFAULT 0 CHECK (reclaimed IN (0, 1)),
        payload_digest TEXT
    )''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(raw_events)')}
    if 'reclaimed' not in columns:
        db.execute('ALTER TABLE raw_events ADD COLUMN reclaimed INTEGER NOT NULL DEFAULT 0')
    if 'payload_digest' not in columns:
        db.execute('ALTER TABLE raw_events ADD COLUMN payload_digest TEXT')


def store_raw_events(db, entries):
    db.executemany(
        'INSERT INTO raw_events (event_key, raw_json, revision_json, accepted_version, reclaimed, payload_digest)'
        ' VALUES (?, ?, ?, ?, 0, NULL)', [
            (entry.raw.identity.key, json.dumps(entry.raw.to_record(), sort_keys=True),
             encode_field_revision(entry.revision), entry.accepted_version)
            for entry in entries
        ])


def _restore(row):
    key, raw_json, revision_json, version, reclaimed_flag, digest = row
    revision = restore_field_revision(revision_json)
    if reclaimed_flag:
        if revision.identity.key != key or not digest or raw_json != TOMBSTONE_RAW_JSON:
            raise IntakeError(f'reclaimed evidence is damaged for {key}')
        return ReclaimedEntry(revision.identity, revision, version, digest)
    try:
        raw = validate_raw_event(json.loads(raw_json))
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
        rows = tuple(db.execute('SELECT * FROM raw_events ORDER BY event_key'))
    else:
        rows = tuple(db.execute('SELECT * FROM raw_events WHERE event_key = ?', (key,)).fetchone()
                     for key in keys)
        if any(row is None for row in rows):
            missing = next(key for key, row in zip(keys, rows) if row is None)
            raise IntakeError(f'missing original event {missing}')
    return tuple(_restore(row) for row in rows)


def reclaimed_event_keys(db):
    return frozenset(row[0] for row in db.execute(
        'SELECT event_key FROM raw_events WHERE reclaimed = 1'))


def mark_events_reclaimed(db, entries):
    rows = []
    for entry in entries:
        if entry.reclaimed:
            raise IntakeError(f'event was already reclaimed: {entry.key}')
        rows.append((TOMBSTONE_RAW_JSON, record_payload_digest(entry.raw), entry.key))
    cursor = db.executemany('''UPDATE raw_events
                               SET raw_json = ?, payload_digest = ?, reclaimed = 1
                               WHERE event_key = ? AND reclaimed = 0''', rows)
    if cursor.rowcount != len(rows):
        raise IntakeError('reclamation target changed before the bodies were removed')
