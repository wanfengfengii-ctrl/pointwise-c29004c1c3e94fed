import json
from dataclasses import dataclass

from src.intake.identity import EventIdentity
from src.intake.normalize import FieldIntent

from .apply import FieldFact
from .effects import FieldEffect


@dataclass(frozen=True)
class FactSnapshot:
    version: int
    members: tuple[str, ...]
    facts: tuple[FieldFact, ...]
    effects: tuple[FieldEffect, ...]


def initialize_snapshots(db):
    db.execute('''CREATE TABLE IF NOT EXISTS fact_snapshots (
        version INTEGER PRIMARY KEY, members_json TEXT NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS snapshot_fields (
        version INTEGER NOT NULL REFERENCES fact_snapshots(version),
        entity TEXT NOT NULL, field TEXT NOT NULL, state TEXT NOT NULL,
        value TEXT, day TEXT NOT NULL, event_key TEXT NOT NULL, seq INTEGER NOT NULL,
        PRIMARY KEY (version, entity, field)
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS snapshot_effects (
        version INTEGER NOT NULL REFERENCES fact_snapshots(version),
        ordinal INTEGER NOT NULL, event_key TEXT NOT NULL, intent TEXT NOT NULL,
        before_key TEXT, after_key TEXT,
        PRIMARY KEY (version, ordinal), UNIQUE (version, event_key)
    )''')


def _key(identity):
    return identity.key if identity is not None else None


def _identity(key):
    return EventIdentity.from_key(key) if key is not None else None


def save_snapshot(db, snapshot):
    db.execute('INSERT INTO fact_snapshots VALUES (?, ?)',
               (snapshot.version, json.dumps(snapshot.members)))
    db.executemany('INSERT INTO snapshot_fields VALUES (?, ?, ?, ?, ?, ?, ?, ?)', [
        (snapshot.version, fact.entity, fact.field, fact.state, fact.value,
         fact.day, fact.event.key, fact.seq) for fact in snapshot.facts
    ])
    db.executemany('INSERT INTO snapshot_effects VALUES (?, ?, ?, ?, ?, ?)', [
        (snapshot.version, effect.ordinal, effect.event.key, effect.intent,
         _key(effect.before), _key(effect.after)) for effect in snapshot.effects
    ])


def load_snapshot(db, version):
    row = db.execute('SELECT members_json FROM fact_snapshots WHERE version = ?', (version,)).fetchone()
    if row is None:
        raise LookupError(f'unknown fact version {version}')
    members = json.loads(row[0])
    if not isinstance(members, list) or any(not isinstance(key, str) for key in members):
        raise ValueError(f'invalid event manifest in version {version}')
    if len(set(members)) != len(members):
        raise ValueError(f'duplicate snapshot members in version {version}')
    facts = tuple(FieldFact(entity, field, FieldIntent(state), value, day, _identity(key), seq)
                  for entity, field, state, value, day, key, seq in db.execute('''
                      SELECT entity, field, state, value, day, event_key, seq
                      FROM snapshot_fields WHERE version = ? ORDER BY entity, field''', (version,)))
    effects = tuple(FieldEffect(_identity(key), ordinal, FieldIntent(intent), _identity(before), _identity(after))
                    for ordinal, key, intent, before, after in db.execute('''
                        SELECT ordinal, event_key, intent, before_key, after_key
                        FROM snapshot_effects WHERE version = ? ORDER BY ordinal''', (version,)))
    if (tuple(effect.event.key for effect in effects) != tuple(members)
            or any(effect.ordinal != index for index, effect in enumerate(effects, 1))):
        raise ValueError(f'incomplete effect evidence in version {version}')
    return FactSnapshot(version, tuple(members), facts, effects)
