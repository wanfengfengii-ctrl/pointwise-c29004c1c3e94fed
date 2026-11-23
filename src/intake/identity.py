import json
from dataclasses import dataclass


def require_name(value, label):
    if not isinstance(value, str) or not value.strip() or '\x00' in value:
        raise ValueError(f'{label} must be a nonempty string without NUL')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as error:
        raise ValueError(f'{label} must be valid UTF-8') from error
    return value


@dataclass(frozen=True, order=True)
class EventIdentity:
    event_id: str
    entity: str
    field: str
    source: str

    @property
    def parts(self):
        return self.event_id, self.entity, self.field, self.source

    @property
    def field_key(self):
        return self.entity, self.field

    @property
    def key(self):
        return json.dumps(self.parts, separators=(',', ':'))

    @classmethod
    def from_parts(cls, parts):
        if not isinstance(parts, (tuple, list)) or len(parts) != 4:
            raise ValueError('event identity requires four parts')
        return cls(*(require_name(part, label) for label, part in zip(
            ('event_id', 'entity', 'field', 'source'), parts)))

    @classmethod
    def from_key(cls, key):
        identity = cls.from_parts(json.loads(key))
        if identity.key != key:
            raise ValueError('noncanonical event key')
        return identity


def identify_event(record):
    return EventIdentity.from_parts([record[name] for name in (
        'event_id', 'entity', 'field', 'source')])


def same_event_content(left, right):
    return left.to_record() == right.to_record()
