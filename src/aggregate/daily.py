from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, DecimalException
from fractions import Fraction

from src.intake.identity import EventIdentity
from src.intake.validation import validate_day

from .contributions import Contribution


@dataclass(frozen=True)
class DailyAggregate:
    version: int
    day: str
    total_mg: str
    count: int
    unit: str = 'mg'


def initialize_daily_store(db):
    db.execute('''CREATE TABLE IF NOT EXISTS contributions (
        version INTEGER NOT NULL REFERENCES fact_snapshots(version), entity TEXT NOT NULL,
        field TEXT NOT NULL, day TEXT NOT NULL, event_key TEXT NOT NULL, value TEXT NOT NULL,
        unit TEXT NOT NULL CHECK (unit = 'mg'), PRIMARY KEY (version, entity, field)
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS daily_totals (
        version INTEGER NOT NULL REFERENCES fact_snapshots(version), day TEXT NOT NULL,
        total_mg TEXT NOT NULL, count INTEGER NOT NULL CHECK (count > 0),
        unit TEXT NOT NULL CHECK (unit = 'mg'), PRIMARY KEY (version, day)
    )''')


def _finite_decimal(amount):
    sign = '-' if amount < 0 else ''
    whole, remainder = divmod(abs(amount.numerator), amount.denominator)
    digits = []
    while remainder:
        digit, remainder = divmod(10 * remainder, amount.denominator)
        digits.append(str(digit))
    decimals = '.' + ''.join(digits) if digits else ''
    return f'{sign}{whole}{decimals}'


def build_daily_totals(version, contributions):
    amounts = defaultdict(Fraction)
    counts = defaultdict(int)
    for item in contributions:
        if item.version != version or item.unit != 'mg':
            raise ValueError('contribution belongs to another version or unit')
        validate_day(item.day)
        try:
            amounts[item.day] += Fraction(Decimal(item.value))
        except (ValueError, OverflowError, DecimalException) as error:
            raise ValueError(f'invalid mass in contribution {item.field_key}') from error
        counts[item.day] += 1
    return tuple(DailyAggregate(version, day, _finite_decimal(amounts[day]), counts[day])
                 for day in sorted(amounts))


def save_daily_totals(db, totals, contributions):
    db.executemany('INSERT INTO contributions VALUES (?, ?, ?, ?, ?, ?, ?)', [
        (item.version, item.entity, item.field, item.day, item.event.key, item.value, item.unit)
        for item in contributions
    ])
    db.executemany('INSERT INTO daily_totals VALUES (?, ?, ?, ?, ?)', [
        (item.version, item.day, item.total_mg, item.count, item.unit) for item in totals
    ])


def load_daily_totals(db, version):
    return tuple(DailyAggregate(*row) for row in db.execute('''
        SELECT version, day, total_mg, count, unit FROM daily_totals
        WHERE version = ? ORDER BY day''', (version,)))


def load_contributions(db, version):
    return tuple(Contribution(version, entity, field, day, EventIdentity.from_key(event_key), value, unit)
                 for entity, field, day, event_key, value, unit in db.execute('''
                     SELECT entity, field, day, event_key, value, unit FROM contributions
                     WHERE version = ? ORDER BY entity, field''', (version,)))
