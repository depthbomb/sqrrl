from json import dumps
from os import environ
from asyncio import run
from pathlib import Path
from types import ModuleType
from statistics import median
from time import perf_counter
from argparse import ArgumentParser
from sys import modules, path as module_path


async def main():
    parser = ArgumentParser(description='Comparable PostgreSQL runtime and catalog workloads')
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--samples', type=int, default=7)
    args = parser.parse_args()
    if args.samples < 1:
        parser.error('samples must be positive')
    dsn = environ.get('SQRRL_TEST_POSTGRES')
    if not dsn:
        parser.error('Set SQRRL_TEST_POSTGRES to a test server URL with CREATEDB permission')
    module_path.insert(0, str(args.source_root.resolve()))

    from sqrrl.generate import render
    from sqrrl.pg_migrate import _scratch
    from sqrrl.migrate import apply, diff, status
    from sqrrl.schema import Index, Schema, Table, integer, text

    schema = Schema(
        tuple(
            Table(
                f'items_{i}',
                f'Item{i}',
                (integer('id').primary_key(), text('name'), integer('value').default('0')),
                indexes=(Index(f'items_{i}_name', ('name',)),),
            )
            for i in range(30)
        )
    )
    models = ModuleType('sqrrl_pass_models')
    modules[models.__name__] = models
    exec(compile(render(schema), '<benchmark models>', 'exec'), models.__dict__)
    history = (await diff((), schema, 'initial', dsn=dsn),)
    result = {'source_root': str(args.source_root.resolve()), 'samples': args.samples, 'workloads': {}}

    async def measure(name, action):
        values = []
        for sample in range(args.samples + 1):
            start = perf_counter()
            await action()
            elapsed = (perf_counter() - start) * 1000
            if sample:
                values.append(elapsed)
        result['workloads'][name] = {'median_ms': median(values), 'samples_ms': values}
        print(f'{name}: {median(values):.3f} ms', flush=True)

    try:
        async with _scratch(dsn) as database:
            await apply(database, history)
            client = models.Client(database)
            item = await client.items_0.create(name='target')
            result['postgresql'] = tuple(database.connection.raw.get_server_version())

            async def inspect():
                statuses = await status(database, history)
                assert len(statuses) == 1 and statuses[0].applied

            async def mutate():
                async with client.transaction():
                    for value in range(500):
                        assert await client.items_0.update_where(models.Item0Columns.id.eq(item.id), value=value) == 1
                assert (await client.items_0.get(item.id)).value == 499

            async def cursor_rows():
                cursor = await database.connection.execute('SELECT generate_series(1, 50000) AS value')
                try:
                    total = count = 0
                    while row := await cursor.fetchone():
                        total += row['value']
                        count += 1
                    assert count == 50000 and total == 50000 * 50001 // 2
                finally:
                    await cursor.close()

            async def bulk():
                await database.connection.raw.execute('TRUNCATE items_1 RESTART IDENTITY')
                rows = await client.items_1.create_many(models.Item1Create(name=f'row{i}') for i in range(5000))
                assert len(rows) == 5000 and len({row.id for row in rows}) == 5000

            await measure('status_30_tables', inspect)
            await measure('update_where_500', mutate)
            await measure('cursor_fetchone_50000', cursor_rows)
            await measure('bulk_insert_5000', bulk)
    finally:
        modules.pop(models.__name__, None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(dumps(result, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    run(main())
