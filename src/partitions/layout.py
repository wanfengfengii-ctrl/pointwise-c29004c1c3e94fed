from collections import defaultdict
from dataclasses import dataclass

from src.intake.normalize import FieldRevision, encode_field_revision, restore_field_revision
from src.intake.validation import validate_day


@dataclass(frozen=True)
class SegmentMember:
    event_key: str
    raw_ref: str
    revision: FieldRevision


@dataclass(frozen=True)
class SegmentContent:
    segment_id: int
    day: str
    retired: bool
    members: tuple[SegmentMember, ...]


def initialize_segments(db):
    db.execute('''CREATE TABLE IF NOT EXISTS segments (
        segment_id INTEGER PRIMARY KEY, day TEXT NOT NULL,
        retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1))
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS segment_members (
        segment_id INTEGER NOT NULL REFERENCES segments(segment_id),
        event_key TEXT NOT NULL, raw_ref TEXT NOT NULL, revision_json TEXT NOT NULL,
        PRIMARY KEY (segment_id, event_key)
    )''')


def write_segment(db, day, members):
    validate_day(day)
    members = tuple(members)
    if not members:
        raise ValueError('cannot write an empty segment')
    if any(item.revision.day != day or item.revision.identity.key != item.event_key for item in members):
        raise ValueError('cannot write a segment with misplaced members')
    segment_id = db.execute('INSERT INTO segments (day) VALUES (?)', (day,)).lastrowid
    db.executemany('INSERT INTO segment_members VALUES (?, ?, ?, ?)', [
        (segment_id, item.event_key, item.raw_ref, encode_field_revision(item.revision)) for item in members
    ])
    return segment_id


def read_segment(db, segment_id):
    row = db.execute('SELECT day, retired FROM segments WHERE segment_id = ?', (segment_id,)).fetchone()
    if row is None:
        raise LookupError(f'missing segment {segment_id}')
    day, retired = row
    validate_day(day)
    members = tuple(SegmentMember(key, raw_ref, restore_field_revision(data))
                    for key, raw_ref, data in db.execute('''
                        SELECT event_key, raw_ref, revision_json FROM segment_members
                        WHERE segment_id = ? ORDER BY event_key''', (segment_id,)))
    if not members or any(item.revision.identity.key != item.event_key or item.revision.day != day
                          for item in members):
        raise ValueError(f'invalid member evidence in segment {segment_id}')
    return SegmentContent(segment_id, day, bool(retired), members)


def stage_partition_segments(db, revisions):
    by_day = defaultdict(list)
    for revision in revisions:
        key = revision.identity.key
        by_day[revision.day].append(SegmentMember(key, key, revision))
    staged = []
    for day, members in sorted(by_day.items()):
        ordered = tuple(sorted(members, key=lambda item: item.event_key))
        candidate = read_segment(db, write_segment(db, day, ordered))
        if candidate.day != day or candidate.retired or candidate.members != ordered:
            raise ValueError(f'partition append readback mismatch on {day}')
        staged.append(candidate)
    return tuple(staged)
