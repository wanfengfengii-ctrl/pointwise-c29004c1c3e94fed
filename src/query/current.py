from src.publish.database import read_transaction
from src.publish.receipts import load_current_result

from .history import query_scope, read_result


def query_current(db, *, start=None, end=None, entities=None):
    includes = query_scope(start, end, entities)
    with read_transaction(db):
        result_id = load_current_result(db)
        if result_id is None:
            return None
        return read_result(db, result_id, includes)
