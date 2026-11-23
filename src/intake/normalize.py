import json
from dataclasses import dataclass
from decimal import Decimal, DecimalException
from enum import StrEnum

from .identity import EventIdentity
from .validation import IntakeError, decode_payload, validate_day, validate_mass, validate_sequence


class FieldIntent(StrEnum):
    MISSING = 'missing'
    CLEAR = 'clear'
    VALUE = 'value'


@dataclass(frozen=True)
class FieldRevision:
    identity: EventIdentity
    seq: int
    day: str
    intent: FieldIntent
    value: str | None
    requires: tuple[EventIdentity, ...]


def normalize_event(raw):
    body = decode_payload(raw.payload)
    field = raw.identity.field
    value = None
    if field not in body:
        intent = FieldIntent.MISSING
    elif body[field] is None:
        intent = FieldIntent.CLEAR
    else:
        intent = FieldIntent.VALUE
        value = format(validate_mass(body[field]), 'f')
    return FieldRevision(raw.identity, raw.seq, raw.day, intent, value, raw.requires)


def encode_field_revision(revision):
    return json.dumps([revision.identity.parts, revision.seq, revision.day, revision.intent,
                       revision.value, [item.parts for item in revision.requires]], separators=(',', ':'))


def restore_field_revision(encoded):
    try:
        parts, seq, day, state, value, refs = json.loads(encoded)
        identity = EventIdentity.from_parts(parts)
        intent = FieldIntent(state)
        if intent is FieldIntent.VALUE:
            if not isinstance(value, str):
                raise ValueError('value revision lacks decimal text')
            mass = Decimal(value)
            if not mass.is_finite() or format(mass, 'f') != value:
                raise ValueError('invalid stored decimal text')
            # Fixed-point formatting may expand a positive input exponent.
            if len(value) > 202 or abs(mass.as_tuple().exponent) > 100:
                raise ValueError('stored decimal text exceeds supported precision')
        elif value is not None:
            raise ValueError('non-value revision carries a mass')
        if not isinstance(refs, list):
            raise ValueError('predecessor list is invalid')
        requires = tuple(EventIdentity.from_parts(ref) for ref in refs)
        if tuple(sorted(set(requires))) != requires or identity in requires:
            raise ValueError('invalid normalized predecessors')
        result = FieldRevision(identity, validate_sequence(seq), validate_day(day), intent, value, requires)
        if encode_field_revision(result) != encoded:
            raise ValueError('noncanonical normalized revision')
        return result
    except (ValueError, TypeError, DecimalException, RecursionError) as error:
        raise IntakeError(f'invalid normalized revision: {error}') from error
