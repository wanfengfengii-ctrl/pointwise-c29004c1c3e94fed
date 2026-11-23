class SegmentIntegrityError(ValueError):
    pass


def _members(segments):
    members = {}
    for segment in segments:
        for member in segment.members:
            if member.event_key in members:
                raise SegmentIntegrityError(f'duplicate member {member.event_key} in replacement group')
            members[member.event_key] = member
    return members


def validate_replacement(sources, candidate):
    if not sources or candidate.retired or any(
            segment.day != candidate.day or segment.retired or segment.segment_id == candidate.segment_id
            for segment in sources):
        raise SegmentIntegrityError('invalid source or replacement segment')
    expected = _members(sources)
    actual = _members((candidate,))
    if expected.keys() != actual.keys():
        raise SegmentIntegrityError('replacement member identities differ from source')
    for key in sorted(expected):
        if expected[key] != actual[key]:
            raise SegmentIntegrityError(f'replacement evidence differs for {key}')
