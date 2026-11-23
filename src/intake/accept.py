from dataclasses import dataclass

from .identity import same_event_content
from .normalize import normalize_event
from .raw_store import RawEntry, find_raw_event, store_raw_events
from .validation import IntakeError


def unique_events(events):
    unique = {}
    for event in events:
        prior = unique.setdefault(event.identity, event)
        if not same_event_content(prior, event):
            raise IntakeError(f'conflicting content for event {event.identity.key}')
    return tuple(sorted(unique.values(), key=lambda event: event.identity.key))


@dataclass(frozen=True)
class Acceptance:
    added: tuple[RawEntry, ...]
    repeated: tuple[RawEntry, ...]


def accept_batch(db, events, version):
    added = []
    repeated = []
    for event in unique_events(events):
        existing = find_raw_event(db, event.identity)
        if existing is None:
            added.append(RawEntry(event, normalize_event(event), version))
        elif same_event_content(existing.raw, event):
            repeated.append(existing)
        else:
            raise IntakeError(f'event identity already has different content: {event.identity.key}')
    store_raw_events(db, added)
    return Acceptance(tuple(added), tuple(repeated))
