from .catalog import resolve_segment
from .layout import read_segment
from .verify import SegmentIntegrityError


def can_retire_segment(db, source_id, candidate):
    source = read_segment(db, source_id)
    if source.retired or source.day != candidate.day:
        return False
    candidate_keys = {member.event_key for member in candidate.members}
    original_segments = {source_id: {member.event_key for member in source.members}}
    for member in source.members:
        location = resolve_segment(db, member.event_key)
        if location.original not in original_segments:
            original = read_segment(db, location.original)
            original_segments[location.original] = {item.event_key for item in original.members}
        if (location.active != candidate.segment_id or member.event_key not in candidate_keys
                or member.event_key not in original_segments[location.original]):
            return False
    return True


def retire_replaced_segments(db, replacements):
    for replacement in replacements:
        for source_id in replacement.source_ids:
            if not can_retire_segment(db, source_id, replacement.content):
                raise SegmentIntegrityError(f'historical source cannot be retired: {source_id}')
            cursor = db.execute('UPDATE segments SET retired = 1 WHERE segment_id = ? AND retired = 0', (source_id,))
            if cursor.rowcount != 1:
                raise SegmentIntegrityError(f'source changed before retirement: {source_id}')
