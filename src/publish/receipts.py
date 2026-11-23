import json
from dataclasses import asdict, dataclass

from src.intake.identity import EventIdentity


@dataclass(frozen=True)
class EventReceipt:
    event: EventIdentity
    result_id: str
    request_id: str


@dataclass(frozen=True)
class PublishReceipt:
    request_id: str
    kind: str
    result_id: str | None
    version: int | None
    events: tuple[EventReceipt, ...] = ()
    catalog_version: int | None = None
    replacements: tuple[tuple[int, int], ...] = ()
    affected_days: tuple[str, ...] = ()


def initialize_receipts(db):
    db.execute('''CREATE TABLE IF NOT EXISTS request_receipts (
        request_id TEXT PRIMARY KEY REFERENCES publish_attempts(request_id), body TEXT NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS results (
        result_id TEXT PRIMARY KEY, version INTEGER NOT NULL UNIQUE REFERENCES fact_snapshots(version),
        request_id TEXT NOT NULL UNIQUE REFERENCES request_receipts(request_id)
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS current_result (
        slot INTEGER PRIMARY KEY CHECK (slot = 1), result_id TEXT NOT NULL REFERENCES results(result_id)
    )''')


def find_request_receipt(db, request_id):
    row = db.execute('SELECT body FROM request_receipts WHERE request_id = ?', (request_id,)).fetchone()
    if row is None:
        return None
    data = json.loads(row[0])
    if data['request_id'] != request_id:
        raise ValueError(f'invalid receipt identity for request {request_id!r}')
    data['events'] = tuple(EventReceipt(EventIdentity(**item['event']), item['result_id'], item['request_id'])
                           for item in data['events'])
    data['replacements'] = tuple(tuple(item) for item in data['replacements'])
    data['affected_days'] = tuple(data['affected_days'])
    return PublishReceipt(**data)


def save_success_receipt(db, receipt, *, make_current=False):
    db.execute('INSERT INTO request_receipts VALUES (?, ?)',
               (receipt.request_id, json.dumps(asdict(receipt), sort_keys=True)))
    if make_current:
        db.execute('INSERT INTO results VALUES (?, ?, ?)', (receipt.result_id, receipt.version, receipt.request_id))
        db.execute('''INSERT INTO current_result VALUES (1, ?)
                      ON CONFLICT(slot) DO UPDATE SET result_id = excluded.result_id''', (receipt.result_id,))


def resolve_result_receipt(db, result_id):
    row = db.execute('SELECT version, request_id FROM results WHERE result_id = ?', (result_id,)).fetchone()
    if row is None:
        raise LookupError(f'unknown result {result_id!r}')
    receipt = find_request_receipt(db, row[1])
    if receipt is None or (receipt.kind, receipt.result_id, receipt.version) != ('batch', result_id, row[0]):
        raise ValueError(f'inconsistent receipt for result {result_id!r}')
    return receipt


def result_for_version(db, version):
    row = db.execute('SELECT result_id, request_id FROM results WHERE version = ?', (version,)).fetchone()
    if row is None:
        raise LookupError(f'no published result for version {version}')
    return tuple(row)


def load_current_result(db):
    row = db.execute('SELECT result_id FROM current_result WHERE slot = 1').fetchone()
    if row is None:
        if db.execute('SELECT 1 FROM results LIMIT 1').fetchone() is not None:
            raise ValueError('published results exist without a current result')
        return None
    return row[0]
