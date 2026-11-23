from dataclasses import dataclass

from src.intake.identity import EventIdentity
from src.intake.normalize import FieldIntent

from .effects import record_effect


@dataclass(frozen=True)
class FieldFact:
    entity: str
    field: str
    state: FieldIntent
    value: str | None
    day: str
    event: EventIdentity
    seq: int

    @property
    def field_key(self):
        return self.entity, self.field


def apply_field_revision(prior, revision):
    if revision.intent is FieldIntent.MISSING:
        return prior
    if revision.intent is FieldIntent.CLEAR:
        value = None
    elif revision.intent is FieldIntent.VALUE and revision.value is not None:
        value = revision.value
    else:
        raise ValueError(f'unusable revision for {revision.identity.key}')
    return FieldFact(revision.identity.entity, revision.identity.field, revision.intent,
                     value, revision.day, revision.identity, revision.seq)


def materialize_facts(ordered):
    facts = {}
    effects = []
    for ordinal, revision in enumerate(ordered, 1):
        field_key = revision.identity.field_key
        prior = facts.get(field_key)
        after = apply_field_revision(prior, revision)
        if after is not None:
            facts[field_key] = after
        effects.append(record_effect(revision, ordinal, prior, after))
    return tuple(facts[key] for key in sorted(facts)), tuple(effects)
