import json
from dataclasses import dataclass

from src.intake.identity import require_name


class RequestConflict(ValueError):
    pass


@dataclass(frozen=True)
class PublishAttempt:
    request_id: str
    kind: str
    parameters: str
    status: str
    reason: str | None


def initialize_attempts(db):
    db.execute('''CREATE TABLE IF NOT EXISTS publish_attempts (
        request_id TEXT PRIMARY KEY, kind TEXT NOT NULL, parameters TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'failed', 'complete')), reason TEXT
    )''')


def load_attempt(db, request_id):
    row = db.execute('SELECT * FROM publish_attempts WHERE request_id = ?', (request_id,)).fetchone()
    return None if row is None else PublishAttempt(*row)


def bind_request_parameters(db, request_id, kind, parameters):
    require_name(request_id, 'request_id')
    encoded = json.dumps(parameters, sort_keys=True, separators=(',', ':'), allow_nan=False)
    db.execute('INSERT OR IGNORE INTO publish_attempts VALUES (?, ?, ?, ?, NULL)',
               (request_id, kind, encoded, 'pending'))
    attempt = load_attempt(db, request_id)
    if (attempt.kind, attempt.parameters) != (kind, encoded):
        raise RequestConflict(f'request {request_id!r} is bound to different parameters')
    return attempt


def begin_attempt(db, request_id):
    cursor = db.execute('''UPDATE publish_attempts SET status = 'pending', reason = NULL
                           WHERE request_id = ? AND status <> 'complete' ''', (request_id,))
    if cursor.rowcount != 1:
        raise ValueError(f'cannot begin attempt {request_id!r}')


def record_failure(db, request_id, reason):
    db.execute('''UPDATE publish_attempts SET status = 'failed', reason = ?
                  WHERE request_id = ? AND status <> 'complete' ''', (reason, request_id))


def complete_attempt(db, request_id):
    cursor = db.execute('''UPDATE publish_attempts SET status = 'complete', reason = NULL
                           WHERE request_id = ?''', (request_id,))
    if cursor.rowcount != 1:
        raise ValueError(f'no attempt to complete for {request_id!r}')
