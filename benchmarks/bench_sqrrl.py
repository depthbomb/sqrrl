from gc import collect
from json import dumps
from asyncio import run
from pathlib import Path
from shutil import copyfile
from types import ModuleType
from statistics import median
from time import perf_counter
from dataclasses import replace
from inspect import isawaitable
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from sys import modules, path as module_path
from platform import platform, python_version

async def invoke(action):
    result = action()
    if isawaitable(result):
        await result

async def measure(action, samples, prepare=lambda: None, verify=lambda: None):
    timings = []
    for index in range(samples + 1):
        await invoke(prepare)
        collect()
        start = perf_counter()
        await invoke(action)
        elapsed = (perf_counter() - start) * 1000
        await invoke(verify)
        if index:
            timings.append(elapsed)

    return {"median_ms": median(timings), "samples_ms": timings}

async def main():
    parser = ArgumentParser(description="Repeatable SQLite workloads with correctness checks")
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--samples", type=int, default=7)
    arguments = parser.parse_args()
    module_path.insert(0, str(arguments.source_root.resolve()))

    from sqlite3 import sqlite_version
    from sqrrl import Database
    from sqrrl.generate import render
    from sqrrl.migrate import apply, custom, diff
    from sqrrl.schema import Index, Schema, Table, integer, text

    large = Schema(
            tuple(
                    Table(
                            f"records_{index}",
                            f"Record{index}",
                            (integer("id").primary_key(), *(text(f"field_{number}") for number in range(12))),
                    )
                    for index in range(150)
            )
    )
    schema = Schema(
            (
                Table(
                        "records",
                        "Record",
                        (integer("id").primary_key(), text("title"), text("body"), integer("priority").default("0")),
                ),
            )
    )
    generated = ModuleType("benchmark_models")
    modules[generated.__name__] = generated
    exec(compile(render(schema), "benchmark_models.py", "exec"), generated.__dict__)
    initial = await diff((), schema, "initial")
    indexed = Schema((replace(schema.tables[0], indexes=(Index("records_priority", ("priority",)),)),))
    addition = await diff((initial,), indexed, "priority_index")
    result = {
        "python": python_version(),
        "sqlite": sqlite_version,
        "platform": platform(),
        "source_root": str(arguments.source_root.resolve()),
        "samples": arguments.samples,
        "workloads": {},
    }
    workloads = result["workloads"]
    workloads["normalize_150_tables"] = await measure(large.normalize, arguments.samples)
    workloads["normalize_150_tables_fresh"] = await measure(lambda: Schema(large.tables).normalize(), arguments.samples)
    workloads["generate_150_tables"] = await measure(lambda: render(large), arguments.samples)
    backfill = "UPDATE records SET body = '" + ";value" * 4096 + "';"
    workloads["custom_sql_quoted_semicolons"] = await measure(
            lambda: custom((initial,), "text_backfill", backfill), arguments.samples
    )
    with TemporaryDirectory(prefix="sqrrl-bench-") as temporary:
        root = Path(temporary)
        async with await Database.create(root / "runtime.db", wal=True) as database:
            (await apply(database, (initial,)))
            client = generated.Client(database)

            async def clear():
                (await (await database.connection.execute("DELETE FROM records")).close())

            async def insert():
                async with client.transaction() as transaction:
                    for index in range(2000):
                        (await transaction.records.create(title=f"record {index}", body="x" * 256))

            async def verify_insert():
                assert (await client.records.query().count()) == 2000
                assert (await client.records.get(2000)).title == "record 1999"

            workloads["insert_2000_transaction"] = await measure(insert, arguments.samples, clear, verify_insert)

            async def get():
                for index in range(1, 5001):
                    assert (await client.records.get((index % 2000) + 1)).priority == 0

            workloads["get_5000"] = await measure(get, arguments.samples)

            async def update():
                async with client.transaction() as transaction:
                    for index in range(1, 2001):
                        (await transaction.records.update(index, priority=1))

            async def verify_update():
                assert (await (await database.connection.execute("SELECT sum(priority) FROM records")).fetchone())[
                           0
                       ] == 2000

            workloads["update_2000_transaction"] = await measure(update, arguments.samples, verify=verify_update)
            workloads["read_2000_rows"] = await measure(lambda: client.records.query().all(), arguments.samples)
            assert (await (await database.connection.execute("PRAGMA integrity_check")).fetchone())[0] == "ok"

        source = root / "index_source.db"
        async with await Database.create(source) as database:
            (await apply(database, (initial,)))
            async with database.transaction():
                (
                    await (
                        await database.connection.executemany(
                                "INSERT INTO records(title, body, priority) VALUES (?, ?, ?)",
                                ((f"record {i}", "x" * 1024, i % 100) for i in range(20000)),
                        )
                    ).close()
                )

        target = root / "index_target.db"
        holder = []

        async def prepare_index():
            copyfile(source, target)
            holder.append((await Database.open(target)))

        async def apply_index():
            (await apply(holder[0], (initial, addition)))

        async def verify_index():
            database = holder.pop()
            try:
                row = await (
                    await database.connection.execute("SELECT count(*), sum(length(body)) FROM records")
                ).fetchone()
                assert tuple(row) == (20000, 20000 * 1024)
                assert (await (await database.connection.execute("PRAGMA integrity_check")).fetchone())[0] == "ok"
            finally:
                (await database.close())

        workloads["index_migration_20000_wide_rows"] = await measure(
                apply_index, arguments.samples, prepare_index, verify_index
        )

    content = dumps(result, indent=2) + "\n"
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(content, encoding="utf-8")

    for name, measurement in workloads.items():
        print(f"{name}: {measurement['median_ms']:.3f} ms")

if __name__ == "__main__":
    run(main())
