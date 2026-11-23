import argparse
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import asdict, is_dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from src.partitions.catalog import load_catalog
from src.query.current import query_current
from src.query.history import query_history

from .coordinator import publish_batch, publish_compaction
from .database import initialize_database, open_database, read_transaction
from .receipts import find_request_receipt
from .rollback import recover_incomplete_attempts


def _json_default(value):
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f'cannot encode {type(value).__name__}')


def _dump(value):
    return json.dumps(value, default=_json_default, sort_keys=True, ensure_ascii=False)


def _load(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _view(result):
    return {
        'facts': {fact.field: [fact.state, fact.value, fact.event.event_id] for fact in result.facts},
        'contributions': [[item.field, item.value, item.event.event_id] for item in result.contributions],
        'daily': [[item.day, item.total_mg, item.count] for item in result.daily],
    }


def _demo(path):
    sample = _load(path)
    day = sample['day']
    batches = sample['batches']
    with TemporaryDirectory(prefix='mass-lake-') as directory:
        db_path = Path(directory) / 'observations.sqlite'
        with closing(open_database(db_path)) as db:
            initialize_database(db)
            receipts = [publish_batch(db, batch['request_id'], batch['events']) for batch in batches]
            clear = query_history(db, receipts[2].result_id)
            late = query_current(db)
            if publish_batch(db, batches[2]['request_id'], batches[2]['events']) != receipts[2]:
                raise ValueError('demo retry did not return the original receipt')
            maintenance = publish_compaction(db, 'maintenance', day, day, load_catalog(db).version)
        with closing(open_database(db_path)) as reopened:
            initialize_database(reopened)
            recover_incomplete_attempts(reopened)
            before = query_history(reopened, receipts[0].result_id)
            after = query_current(reopened)
            expected = {'facts': {'A': ['clear', None, 'clear-a'], 'B': ['value', '5', 'set-b']},
                        'contributions': [['B', '5', 'set-b']], 'daily': [[day, '5', 1]]}
            if _view(clear) != expected or _view(late) != expected or after != late:
                raise ValueError('demo clear, late replay and compacted ledger do not agree')
            if _view(before)['daily'] != [[day, '12', 1]]:
                raise ValueError('demo could not recover the original 12 mg ledger')
            scenario = dict(case_id='V8.compaction_equivalence', input=batches, expected=expected,
                            actual=_view(late), downstream_expected=expected, downstream_actual=_view(after),
                            code_paths=['src/publish/coordinator.py', 'src/partitions/compact.py',
                                        'src/facts/replay.py', 'src/query/history.py'],
                            oracle='A 的序号 4 清空盖过序号 3 的补值，B 的 5 mg 是唯一贡献。')
            print('POINTWISE_SCENARIO ' + _dump(scenario))
            return dict(old=before, cleared=clear, current=after, maintenance=maintenance, original_receipts=receipts)


def _parser():
    parser = argparse.ArgumentParser(description='Experimental weighing ledger')
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('init', 'catalog', 'current', 'history', 'receipt', 'submit', 'compact'):
        commands.add_parser(name).add_argument('db')
    commands.choices['history'].add_argument('result_id')
    for name in ('current', 'history'):
        command = commands.choices[name]
        command.add_argument('--start')
        command.add_argument('--end')
        command.add_argument('--entity', action='append', dest='entities')
    for name in ('receipt', 'submit', 'compact'):
        commands.choices[name].add_argument('request_id')
    commands.choices['submit'].add_argument('batch')
    compact = commands.choices['compact']
    compact.add_argument('start')
    compact.add_argument('end')
    compact.add_argument('source_version', type=int)
    commands.add_parser('demo').add_argument('sample')
    return parser


def _dispatch(db, args):
    if args.command == 'init':
        initialize_database(db)
        recovered = recover_incomplete_attempts(db)
        with read_transaction(db):
            return dict(current=load_current_result(db), catalog=load_catalog(db), recovered=recovered)
    if args.command == 'catalog':
        with read_transaction(db):
            return load_catalog(db)
    if args.command == 'receipt':
        result = find_request_receipt(db, args.request_id)
        if result is None:
            raise LookupError(f'no success receipt for {args.request_id!r}')
        return result
    if args.command == 'submit':
        return publish_batch(db, args.request_id, _load(args.batch))
    if args.command == 'compact':
        return publish_compaction(db, args.request_id, args.start, args.end, args.source_version)
    scope = dict(start=args.start, end=args.end, entities=args.entities)
    if args.command == 'history':
        return query_history(db, args.result_id, **scope)
    return query_current(db, **scope)


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        if args.command == 'demo':
            result = _demo(args.sample)
        else:
            if args.command != 'init' and not Path(args.db).is_file():
                raise FileNotFoundError('initialize the database before using this command')
            with closing(open_database(args.db)) as db:
                result = _dispatch(db, args)
        print(_dump(result))
        return 0
    except (ValueError, LookupError, OSError, sqlite3.Error) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
