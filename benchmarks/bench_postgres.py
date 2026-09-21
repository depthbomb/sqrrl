from os import environ
from asyncio import run
from json import dumps
from sys import modules
from types import ModuleType
from statistics import median
from time import perf_counter
from argparse import ArgumentParser
from sqrrl.generate import render
from sqrrl.pg_migrate import _scratch
from sqrrl.schema import Schema, Table, integer, text


async def main():
    parser = ArgumentParser(description='PostgreSQL workloads with typed models and correctness checks')
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--rows', type=int, default=5000)
    args = parser.parse_args()
    if args.samples < 1 or args.rows < 1:
        parser.error('samples and rows must be positive')
    dsn = environ.get('SQRRL_TEST_POSTGRES')
    if not dsn:
        parser.error('Set SQRRL_TEST_POSTGRES to a PostgreSQL URL with CREATEDB permission')
    schema = Schema(
        (Table('records', 'Record', (integer('id').primary_key(), text('title'), integer('priority').default('0'))),)
    )
    models = ModuleType('sqrrl_benchmark_models')
    modules[models.__name__] = models
    exec(compile(render(schema), '<benchmark models>', 'exec'), models.__dict__)
    try:
        async with _scratch(dsn) as database:
            await database.connection.raw.execute(schema.normalize().tables[0].create_sql(dialect='postgresql'))
            client = models.Client(database)
            values = [models.RecordCreate(title=f'record {i}') for i in range(args.rows)]
            timings = {'bulk_insert': [], 'typed_read': [], 'native_read': []}
            for sample in range(args.samples + 1):
                await database.connection.raw.execute('TRUNCATE records RESTART IDENTITY')
                start = perf_counter()
                rows = await client.records.create_many(values)
                inserted = perf_counter() - start
                assert len(rows) == args.rows and rows[-1].priority == 0
                start = perf_counter()
                rows = await client.records.query().order_by(models.RecordColumns.id.asc()).all()
                selected = perf_counter() - start
                assert len(rows) == args.rows and rows[0].title == 'record 0'
                start = perf_counter()
                raw = await database.connection.raw.fetch('SELECT id, title, priority FROM records ORDER BY id')
                native = perf_counter() - start
                assert len(raw) == args.rows and raw[0]['title'] == rows[0].title
                if sample:
                    for name, elapsed in zip(timings, (inserted, selected, native), strict=True):
                        timings[name].append(elapsed)
            print(
                dumps(
                    {
                        'rows': args.rows,
                        'samples': args.samples,
                        'postgresql': database.connection.raw.get_server_version().major,
                        'workloads': {
                            name: {
                                'median_ms': round(median(values) * 1000, 3),
                                'rows_per_second': round(args.rows / median(values)),
                            }
                            for name, values in timings.items()
                        },
                    },
                    indent=2,
                )
            )
    finally:
        modules.pop(models.__name__, None)


if __name__ == '__main__':
    run(main())
