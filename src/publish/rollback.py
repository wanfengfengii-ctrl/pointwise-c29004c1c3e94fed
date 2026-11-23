from contextlib import contextmanager

from .attempts import complete_attempt, record_failure
from .receipts import find_request_receipt


@contextmanager
def publication_transaction(db):
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise


def recover_incomplete_attempts(db):
    with publication_transaction(db):
        pending = tuple(row[0] for row in db.execute(
            "SELECT request_id FROM publish_attempts WHERE status = 'pending' ORDER BY request_id"))
        for request_id in pending:
            if find_request_receipt(db, request_id) is None:
                record_failure(db, request_id, 'interrupted before publication committed')
            else:
                complete_attempt(db, request_id)
    return pending
