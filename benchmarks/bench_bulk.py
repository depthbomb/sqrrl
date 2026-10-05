from gc import collect
from json import dumps
from os import environ
from asyncio import run
from pathlib import Path
from types import ModuleType
from statistics import median
from time import perf_counter
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from contextlib import asynccontextmanager
from sys import modules, path as module_path
from platform import platform, python_version

async def main():
    parser = ArgumentParser(description='Bulk insert workloads with returned-row and rollback checks')
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--samples', type=int, default=9)
    parser.add_argument('--postgres', action='store_true')
    args = parser.parse_args()
    if args.samples < 1:
        parser.error('samples must be positive')
    if args.postgres and not environ.get('SQRRL_TEST_POSTGRES'):
        parser.error('Set SQRRL_TEST_POSTGRES for PostgreSQL benchmarks')
    module_path.insert(0, str(args.source_root.resolve()))

    from sqrrl import Database, ValidationError
    from sqrrl.generate import render
    from sqrrl.schema import Schema, Table, integer, text

    schema = Schema(tuple(
        Table(f'items_{width}', f'Item{width}', (
            integer('id').primary_key(), text('name'),
            *(integer(f'value_{i}').default('7') for i in range(width)),
        ))
        for width in (1, 16)
    ))
    models = ModuleType('bulk_benchmark_models')
    modules[models.__name__] = models
    exec(compile(render(schema), '<benchmark models>', 'exec'), models.__dict__)
    result = {
        'python': python_version(), 'platform': platform(),
        'source_root': str(args.source_root.resolve()), 'samples': args.samples, 'workloads': {},
    }

    @asynccontextmanager
    async def database():
        if args.postgres:
            from sqrrl.pg_migrate import _scratch
            async with _scratch(environ['SQRRL_TEST_POSTGRES']) as db:
                result['postgresql'] = tuple(db.connection.raw.get_server_version())
                yield db
        else:
            from sqlite3 import sqlite_version
            result['sqlite'] = sqlite_version
            with TemporaryDirectory(prefix='sqrrl-bulk-bench-') as directory:
                async with await Database.create(Path(directory) / 'bench.db') as db:
                    yield db

    try:
        async with database() as db:
            for table in schema.tables:
                await db.connection.execute_fetchall(table.create_sql(dialect=db.dialect))
            client = models.Client(db)
            for width in (1, 16):
                repository = getattr(client, f'items_{width}')
                create = getattr(models, f'Item{width}Create')
                for defaults in (True, False):
                    values = {} if defaults else {f'value_{i}': i for i in range(width)}
                    inputs = [create(name=f'row{i}', **values) for i in range(5000)]
                    expected = tuple(7 if defaults else i for i in range(width))
                    timings = []
                    for sample in range(args.samples + 1):
                        clear = f'TRUNCATE items_{width} RESTART IDENTITY' if args.postgres else f'DELETE FROM items_{width}'
                        await db.connection.execute_fetchall(clear)
                        collect()
                        start = perf_counter()
                        rows = await repository.create_many(inputs)
                        elapsed = (perf_counter() - start) * 1000
                        assert len(rows) == 5000 and len({row.id for row in rows}) == 5000
                        assert {row.name for row in rows} == {item.name for item in inputs}
                        assert all(tuple(getattr(row, f'value_{i}') for i in range(width)) == expected for row in rows)
                        assert await repository.query().count() == 5000
                        if sample:
                            timings.append(elapsed)
                    name = f'bulk_5000_{width}_fields_' + ('defaults' if defaults else 'explicit')
                    result['workloads'][name] = {'median_ms': median(timings), 'samples_ms': timings}
                    print(f'{name}: {median(timings):.3f} ms', flush=True)

                try:
                    await repository.create_many([*inputs, create(name=object())])
                except ValidationError:
                    pass
                else:
                    raise AssertionError('Invalid trailing row should roll back all chunks')
                assert await repository.query().count() == 5000
    finally:
        modules.pop(models.__name__, None)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(dumps(result, indent=2) + '\n', encoding='utf-8')

if __name__ == '__main__':
    run(main())
