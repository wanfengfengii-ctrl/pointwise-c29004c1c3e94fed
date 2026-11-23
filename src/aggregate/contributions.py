from dataclasses import dataclass
from decimal import Decimal, DecimalException

from src.intake.identity import EventIdentity
from src.intake.normalize import FieldIntent
from src.intake.validation import validate_day


@dataclass(frozen=True)
class Contribution:
    version: int
    entity: str
    field: str
    day: str
    event: EventIdentity
    value: str
    unit: str = 'mg'

    @property
    def field_key(self):
        return self.entity, self.field


def derive_contributions(snapshot):
    contributions = []
    for fact in snapshot.facts:
        if fact.state is not FieldIntent.VALUE:
            continue
        try:
            if fact.value is None or not Decimal(fact.value).is_finite():
                raise ValueError('fact has no finite mass')
            validate_day(fact.day)
        except (ValueError, DecimalException) as error:
            raise ValueError(f'cannot derive contribution for {fact.field_key}: {error}') from error
        contributions.append(Contribution(snapshot.version, fact.entity, fact.field,
                                          fact.day, fact.event, fact.value))
    return tuple(contributions)


def affected_contribution_dates(previous, current):
    def by_field(contributions):
        return {item.field_key: (item.day, item.event, item.value, item.unit) for item in contributions}

    old = by_field(previous)
    new = by_field(current)
    days = set()
    for field in old.keys() | new.keys():
        before = old.get(field)
        after = new.get(field)
        if before != after:
            if before is not None:
                days.add(before[0])
            if after is not None:
                days.add(after[0])
    return tuple(sorted(days))
