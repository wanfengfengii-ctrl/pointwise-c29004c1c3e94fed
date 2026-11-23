from collections import defaultdict
from dataclasses import dataclass

from src.intake.identity import require_name
from src.intake.validation import validate_day, validate_range

from .catalog import load_catalog
from .layout import SegmentContent, read_segment, write_segment
from .verify import SegmentIntegrityError, validate_replacement


@dataclass(frozen=True)
class CompactionPlan:
    request_id: str
    start: str
    end: str
    source_version: int
    groups: tuple[tuple[str, tuple[int, ...]], ...]


@dataclass(frozen=True)
class Replacement:
    source_ids: tuple[int, ...]
    content: SegmentContent


def plan_compaction(db, request_id, start, end, source_version):
    require_name(request_id, 'request_id')
    validate_day(start)
    validate_day(end)
    validate_range(start, end)
    if type(source_version) is not int or source_version < 0:
        raise ValueError('source_version must be a nonnegative integer')
    catalog = load_catalog(db)
    if catalog.version != source_version:
        raise SegmentIntegrityError(f'catalog source version {source_version} is stale')
    groups = defaultdict(list)
    for day, segment_id in catalog.active:
        if start <= day <= end:
            groups[day].append(segment_id)
    if not groups:
        raise SegmentIntegrityError('no active segments in compaction range')
    return CompactionPlan(request_id, start, end, source_version,
                          tuple((day, tuple(ids)) for day, ids in sorted(groups.items())))


def stage_replacement(db, plan):
    replacements = []
    for day, source_ids in plan.groups:
        sources = tuple(read_segment(db, source_id) for source_id in source_ids)
        members = [member for source in sources for member in source.members]
        if len({member.event_key for member in members}) != len(members):
            raise SegmentIntegrityError('source segments contain duplicate event identities')
        candidate_id = write_segment(db, day, members)
        candidate = read_segment(db, candidate_id)
        validate_replacement(sources, candidate)
        replacements.append(Replacement(source_ids, candidate))
    return tuple(replacements)
