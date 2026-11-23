from dataclasses import dataclass

from .layout import read_segment


@dataclass(frozen=True)
class SegmentCatalog:
    version: int
    active: tuple[tuple[str, int], ...]
    replacements: tuple[tuple[int, int, int], ...]


@dataclass(frozen=True)
class EventLocation:
    original: int
    active: int


def initialize_catalog(db):
    db.execute('''CREATE TABLE IF NOT EXISTS catalog_state (
        slot INTEGER PRIMARY KEY CHECK (slot = 1), version INTEGER NOT NULL
    )''')
    db.execute('INSERT OR IGNORE INTO catalog_state VALUES (1, 0)')
    db.execute('''CREATE TABLE IF NOT EXISTS catalog_active (
        segment_id INTEGER PRIMARY KEY REFERENCES segments(segment_id), day TEXT NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS catalog_replacements (
        old_segment INTEGER PRIMARY KEY REFERENCES segments(segment_id),
        new_segment INTEGER NOT NULL REFERENCES segments(segment_id), version INTEGER NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS event_locations (
        event_key TEXT PRIMARY KEY, original_segment INTEGER NOT NULL REFERENCES segments(segment_id),
        active_segment INTEGER NOT NULL REFERENCES segments(segment_id)
    )''')


def load_catalog(db):
    version = db.execute('SELECT version FROM catalog_state WHERE slot = 1').fetchone()[0]
    active = tuple(tuple(row) for row in db.execute('SELECT day, segment_id FROM catalog_active ORDER BY day, segment_id'))
    edges = tuple(tuple(row) for row in db.execute('''
        SELECT old_segment, new_segment, version FROM catalog_replacements ORDER BY version, old_segment'''))
    return SegmentCatalog(version, active, edges)


def resolve_segment(db, event_key):
    row = db.execute('''SELECT loc.original_segment, loc.active_segment
                        FROM event_locations AS loc
                        JOIN catalog_active AS active ON active.segment_id = loc.active_segment
                        WHERE loc.event_key = ?''', (event_key,)).fetchone()
    if row is None:
        raise LookupError(f'no current segment location for {event_key}')
    return EventLocation(*row)


def read_active_segments(db, catalog):
    owners = {}
    segments = {}
    for day, segment_id in catalog.active:
        segment = read_segment(db, segment_id)
        if segment.day != day or segment.retired:
            raise ValueError(f'catalog references an unusable active segment {segment_id}')
        segments[segment_id] = segment
        for member in segment.members:
            if member.event_key in owners:
                raise ValueError(f'duplicate active segment member {member.event_key}')
            owners[member.event_key] = segment_id
    locations = dict(db.execute('SELECT event_key, active_segment FROM event_locations'))
    if owners != locations:
        raise ValueError('active segment contents and event locations disagree')
    return segments


def _advance(db, expected):
    cursor = db.execute('UPDATE catalog_state SET version = version + 1 WHERE slot = 1 AND version = ?', (expected,))
    if cursor.rowcount != 1:
        raise ValueError('catalog source version changed')
    return expected + 1


def publish_partition_update(db, expected, segments):
    if not segments:
        raise ValueError('partition update has no segments')
    for segment in segments:
        db.execute('INSERT INTO catalog_active VALUES (?, ?)', (segment.segment_id, segment.day))
        db.executemany('INSERT INTO event_locations VALUES (?, ?, ?)', [
            (member.event_key, segment.segment_id, segment.segment_id) for member in segment.members
        ])
    return _advance(db, expected)


def publish_replacement(db, expected, replacements):
    version = _advance(db, expected)
    for replacement in replacements:
        new = replacement.content
        db.execute('INSERT INTO catalog_active VALUES (?, ?)', (new.segment_id, new.day))
        for old_id in replacement.source_ids:
            cursor = db.execute('DELETE FROM catalog_active WHERE day = ? AND segment_id = ?', (new.day, old_id))
            if cursor.rowcount != 1:
                raise ValueError(f'segment {old_id} is no longer a source')
            db.execute('UPDATE event_locations SET active_segment = ? WHERE active_segment = ?', (new.segment_id, old_id))
            db.execute('INSERT INTO catalog_replacements VALUES (?, ?, ?)', (old_id, new.segment_id, version))
    return version
