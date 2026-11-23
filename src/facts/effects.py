from dataclasses import dataclass

from src.intake.identity import EventIdentity
from src.intake.normalize import FieldIntent


@dataclass(frozen=True)
class FieldEffect:
    event: EventIdentity
    ordinal: int
    intent: FieldIntent
    before: EventIdentity | None
    after: EventIdentity | None


def record_effect(revision, ordinal, before, after):
    return FieldEffect(revision.identity, ordinal, revision.intent,
                       before.event if before is not None else None,
                       after.event if after is not None else None)


def effect_for_event(effects, event):
    for effect in effects:
        if effect.event == event:
            return effect
    raise LookupError(f'no effect for event {event.key}')
