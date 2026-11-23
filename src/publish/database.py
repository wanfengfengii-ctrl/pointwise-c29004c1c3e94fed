import sqlite3
from contextlib import contextmanager

from src.aggregate.daily import initialize_daily_store
from src.facts.snapshot import initialize_snapshots
from src.intake.raw_store import initialize_raw_store
from src.partitions.catalog import initialize_catalog
from src.partitions.layout import initialize_segments

from .attempts import initialize_attempts
from .receipts import initialize_receipts
from .rollback import publication_transaction


def open_database(path):
    db = sqlite3.connect(path, isolation_level=None, timeout=5)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except sqlite3.Error:
        db.close()
        raise
    return db


def initialize_database(db):
    with publication_transaction(db):
        initialize_raw_store(db)
        initialize_snapshots(db)
        initialize_segments(db)
        initialize_catalog(db)
        initialize_daily_store(db)
        initialize_attempts(db)
        initialize_receipts(db)


@contextmanager
def read_transaction(db):
    db.execute('BEGIN')
    with db:
        yield
