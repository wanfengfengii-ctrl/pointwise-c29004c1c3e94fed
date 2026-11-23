from dataclasses import dataclass

from .identity import same_event_content
from .normalize import normalize_event
from .raw_store import (RawEntry, find_raw_event, record_payload_digest,
                        reclaimed_event_keys, store_raw_events)
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
    repeated: tuple  # RawEntry for live events, ReclaimedEntry for reclaimed bodies


def accept_batch(db, events, version):
    added = []
    repeated = []
    reclaimed_keys = reclaimed_event_keys(db)
    for event in unique_events(events):
        existing = find_raw_event(db, event.identity)
        if existing is None:
            missing = [required for required in event.requires if required.key in reclaimed_keys]
            if missing:
                raise IntakeError(
                    f'new event {event.identity.key} references reclaimed body {missing[0].key}')
            added.append(RawEntry(event, normalize_event(event), version))
        elif existing.reclaimed:
            # Identity, sequence occupancy and body summary survive reclamation:
            # the identical body may re-collect the original acceptance receipt.
            if record_payload_digest(event) != existing.payload_digest or normalize_event(event) != existing.revision:
                raise IntakeError(
                    f'event identity already has different content: {event.identity.key}')
            repeated.append(existing)
        elif same_event_content(existing.raw, event):
            repeated.append(existing)
        else:
            raise IntakeError(f'event identity already has different content: {event.identity.key}')
    store_raw_events(db, added)
    return Acceptance(tuple(added), tuple(repeated))
