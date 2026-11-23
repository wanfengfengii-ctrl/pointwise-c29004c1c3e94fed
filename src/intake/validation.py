import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, DecimalException

from .identity import EventIdentity, identify_event


class IntakeError(ValueError):
    pass


def validate_day(value):
    if isinstance(value, str):
        try:
            if date.fromisoformat(value).isoformat() == value:
                return value
        except ValueError:
            pass
    raise ValueError('day must be a calendar date in YYYY-MM-DD form')


def validate_range(start, end):
    if start is not None:
        validate_day(start)
    if end is not None:
        validate_day(end)
    if start is not None and end is not None and start > end:
        raise ValueError('date range is reversed')
    return start, end


def validate_sequence(value):
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError('seq must be a nonnegative SQLite integer')
    return value


def validate_mass(value):
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError('a supplied mass must be a finite JSON number')
    parts = value.as_tuple()
    if len(parts.digits) > 100 or abs(parts.exponent) > 100:
        raise ValueError('mass exceeds the supported decimal precision')
    return value


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate payload key {key!r}')
        result[key] = value
    return result


def _invalid_constant(token):
    raise ValueError(f'invalid JSON number {token}')


def decode_payload(payload):
    if not isinstance(payload, str):
        raise IntakeError('payload must contain the original JSON text')
    try:
        payload.encode('utf-8')
        obj = json.loads(payload, parse_float=Decimal, parse_int=Decimal,
                         parse_constant=_invalid_constant, object_pairs_hook=_unique_object)
    except (ValueError, DecimalException, RecursionError) as error:
        raise IntakeError(f'invalid original payload: {error}') from error
    if not isinstance(obj, dict):
        raise IntakeError('payload must be a JSON object')
    return obj


@dataclass(frozen=True)
class RawEvent:
    identity: EventIdentity
    seq: int
    day: str
    payload: str
    requires: tuple[EventIdentity, ...]

    def to_record(self):
        return dict(event_id=self.identity.event_id, entity=self.identity.entity,
                    field=self.identity.field, source=self.identity.source,
                    seq=self.seq, day=self.day, payload=self.payload,
                    requires=[list(item.parts) for item in self.requires])


def validate_raw_event(record):
    required = {'event_id', 'entity', 'field', 'source', 'seq', 'day', 'payload'}
    if not isinstance(record, dict) or not required <= record.keys() or record.keys() - required - {'requires'}:
        raise IntakeError('invalid event envelope fields')
    try:
        identity = identify_event(record)
        seq = validate_sequence(record['seq'])
        day = validate_day(record['day'])
        payload = record['payload']
        obj = decode_payload(payload)
        if identity.field in obj and obj[identity.field] is not None:
            validate_mass(obj[identity.field])
        refs = record.get('requires', [])
        if not isinstance(refs, list):
            raise ValueError('requires must be an array of event identities')
        requires = tuple(sorted(EventIdentity.from_parts(ref) for ref in refs))
        if len(set(requires)) != len(requires) or identity in requires:
            raise ValueError('duplicate or self-referential predecessor')
        return RawEvent(identity, seq, day, payload, requires)
    except IntakeError:
        raise
    except ValueError as error:
        raise IntakeError(str(error)) from error


def validate_batch(records):
    if not isinstance(records, (list, tuple)) or not records:
        raise IntakeError('batch must be a nonempty array of events')
    return tuple(validate_raw_event(record) for record in records)
