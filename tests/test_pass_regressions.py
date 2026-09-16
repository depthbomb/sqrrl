from pytest import raises
from dataclasses import replace
from test_async import pause_sql
from sqrrl import Database, MigrationError
from sqrrl.migrate import apply, baseline, custom, diff
from sqrrl.schema import Index, Schema, Table, integer, text
from asyncio import CancelledError, create_task, sleep, timeout
from sqlite3 import DatabaseError, SQLITE_DENY, SQLITE_OK, SQLITE_SAVEPOINT

async def test_zero_limit_is_respected_by_first_and_exists(tmp_path, schema, models):
    async with await Database.create(tmp_path / "limit.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        client = models.Client(database)
        await client.users.create(name="Ada")
        query = client.users.query().limit(0)
        assert await query.all() == []
        assert await query.first() is None
        assert await query.exists() is False
        assert await query.count() == 1

async def test_cancellation_during_savepoint_release_keeps_outer_transaction(tmp_path, schema, models):
    async with await Database.create(tmp_path / "release.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        client = models.Client(database)
        connection = database.connection

        async def work():
            async with client.transaction():
                with raises(CancelledError):
                    await client.users.create(name="saved")

                assert connection.in_transaction
                assert (await client.users.query().only()).name == "saved"

        async with pause_sql(connection, "RELEASE") as (entered, release):
            task = create_task(work())
            try:
                async with timeout(2):
                    await entered.wait()

                task.cancel()
                await sleep(0)
            finally:
                release.set()

            await task

        assert await client.users.query().count() == 1

async def test_moving_index_from_rebuilt_table_to_earlier_table(tmp_path):
    first_table = Table("alpha", "Alpha", (integer("id").primary_key(), text("title")))
    last_table = Table("zeta", "Zeta", first_table.fields, indexes=(Index("titles", ("title",)),))
    original = Schema((first_table, last_table))
    first = await diff((), original, "initial")
    desired = Schema(
            (
                replace(first_table, indexes=last_table.indexes),
                replace(last_table, fields=last_table.fields + (text("extra").nullable(),), indexes=()),
            )
    )
    second = await diff((first,), desired, "move_index")
    async with await Database.create(tmp_path / "index.db") as database:
        await apply(database, (first,))
        async with database.connection.execute("INSERT INTO zeta(title) VALUES ('preserved')"):
            pass

        await apply(database, (first, second))
        async with database.connection.execute("SELECT title, extra FROM zeta") as cursor:
            assert tuple(await cursor.fetchone()) == ("preserved", None)

async def test_baseline_rejects_boolean_version(tmp_path, schema):
    first = await diff((), schema, "initial")
    async with await Database.create(tmp_path / "baseline.db") as database:
        await apply(database, (first,))
        with raises(MigrationError, match="version"):
            await baseline(database, (first,), True)

async def test_failed_savepoint_creation_keeps_outer_transaction(tmp_path, schema, models):
    async with await Database.create(tmp_path / "begin.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        client = models.Client(database)
        async with client.transaction():
            user = await client.users.create(name="outer")
            connection = database.connection
            await connection.set_authorizer(
                    lambda action, *args: SQLITE_DENY if action == SQLITE_SAVEPOINT else SQLITE_OK
            )
            try:
                with raises(DatabaseError):
                    async with client.transaction():
                        raise AssertionError("Denied savepoint should not run its body")
            finally:
                await connection.set_authorizer(None)

            assert await client.users.query().all() == [user]

        assert await client.users.query().all() == [user]

async def test_migration_checks_errors_after_first_result_row(tmp_path):
    schema = Schema((Table("notes", "Note", (integer("id").primary_key(), text("title"))),))
    first = await diff((), schema, "initial")
    second = await custom(
            (first,),
            "late_failure",
            "UPDATE notes SET title = 'changed'; SELECT CASE WHEN id = 1 THEN 1 ELSE abs(-9223372036854775808) END FROM notes;",
    )
    async with await Database.create(tmp_path / "late.db") as database:
        await apply(database, (first,))
        async with database.connection.execute("INSERT INTO notes(title) VALUES ('one'), ('two')"):
            pass

        with raises(MigrationError, match="overflow"):
            await apply(database, (first, second))

        async with database.connection.execute("SELECT title FROM notes ORDER BY id") as cursor:
            assert [row[0] for row in await cursor.fetchall()] == ["one", "two"]

        async with database.connection.execute("SELECT count(*) FROM sqrrl_migrations") as cursor:
            assert (await cursor.fetchone())[0] == 1
