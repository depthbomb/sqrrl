from pytest import mark, raises
from aiosqlite import Connection
from sqrrl import Database, SqrrlError
from contextlib import asynccontextmanager
from threading import Event as ThreadEvent
from sqlite3 import connect as sqlite_connect
from sqrrl.migrate import apply, diff, status
from asyncio import CancelledError, Event, create_task, gather, get_running_loop, sleep, timeout

@asynccontextmanager
async def pause_sql(connection, prefix):
    loop = get_running_loop()
    entered = Event()
    release = ThreadEvent()

    def trace(statement):
        if statement.startswith(prefix):
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(5):
                loop.call_soon_threadsafe(entered.set)

    await connection.set_trace_callback(trace)
    try:
        yield entered, release
    finally:
        release.set()
        await connection.set_trace_callback(None)

async def test_tasks_do_not_join_another_tasks_transaction(tmp_path, schema, models):
    history = (await diff((), schema, "initial"),)
    async with await Database.create(tmp_path / "tasks.db") as database:
        await apply(database, history)
        client = models.Client(database)
        entered = Event()
        release = Event()

        async def rollback():
            with raises(ValueError):
                async with client.transaction():
                    await client.users.create(name="rolled back")
                    entered.set()
                    await release.wait()
                    raise ValueError("rollback")

        async def insert():
            await entered.wait()

            return await client.users.create(name="committed")

        async with timeout(5):
            owner = create_task(rollback())
            await entered.wait()
            writer = create_task(insert())
            reader = create_task(client.users.query().all())
            await sleep(0)
            assert not writer.done() and not reader.done()
            with raises(SqrrlError, match="another task"):
                database.connection

            release.set()
            _, user, rows = await gather(owner, writer, reader)

        assert rows == [user]
        assert await client.users.query().all() == [user]

async def test_cancelled_lock_waiter_does_not_release_owners_lock(tmp_path, schema, models):
    async with await Database.create(tmp_path / "waiter.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        client = models.Client(database)
        async with client.transaction():
            waiter = create_task(client.users.create(name="cancelled"))
            await sleep(0)
            waiter.cancel()
            with raises(CancelledError):
                await waiter

            assert database.connection.in_transaction
            await client.users.create(name="owner")

        assert (await client.users.query().only()).name == "owner"

@mark.parametrize("prefix", ["BEGIN", "INSERT INTO", "COMMIT"])
async def test_cancellation_during_mutation_finishes_sql_and_cleanup(tmp_path, schema, models, prefix):
    async with await Database.create(tmp_path / "cancel.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        connection = database.connection
        client = models.Client(database)
        async with pause_sql(connection, prefix) as (entered, release):
            task = create_task(client.users.create(name="cancelled"))
            try:
                async with timeout(2):
                    await entered.wait()

                task.cancel()
                await sleep(0)
                assert not task.done()
                task.cancel()
            finally:
                release.set()

            with raises(CancelledError):
                await task

        assert not connection.in_transaction
        # A commit already running in SQLite may finish before cancellation arrives.
        assert await client.users.query().count() == (1 if prefix == "COMMIT" else 0)
        await client.users.create(name="usable")

async def test_repeated_cancellation_waits_for_rollback(tmp_path, schema, models):
    async with await Database.create(tmp_path / "rollback.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        connection = database.connection
        client = models.Client(database)
        ready = Event()

        async def transaction():
            async with client.transaction():
                await client.users.create(name="cancelled")
                ready.set()
                await Event().wait()

        async with pause_sql(connection, "ROLLBACK") as (entered, release):
            task = create_task(transaction())
            try:
                async with timeout(2):
                    await ready.wait()
                    task.cancel()
                    await entered.wait()

                task.cancel()
                await sleep(0)
                assert not task.done()
            finally:
                release.set()

            with raises(CancelledError):
                await task

        assert await client.users.query().count() == 0
        assert not connection.in_transaction

async def test_cancelled_nested_transaction_preserves_outer_scope(tmp_path, schema, models):
    async with await Database.create(tmp_path / "nested.db") as database:
        await apply(database, (await diff((), schema, "initial"),))
        client = models.Client(database)
        async with client.transaction():
            user = await client.users.create(name="outer")
            with raises(CancelledError):
                async with client.transaction():
                    await client.users.create(name="inner")
                    raise CancelledError

            assert await client.users.query().all() == [user]

        assert await client.users.query().all() == [user]

@mark.parametrize("prefix", ["PRAGMA foreign_keys = OFF", "BEGIN IMMEDIATE", 'CREATE TABLE main."'])
async def test_cancelled_migration_restores_schema_and_connection(tmp_path, schema, prefix):
    history = (await diff((), schema, "initial"),)
    async with await Database.create(tmp_path / "migration.db") as database:
        connection = database.connection
        async with pause_sql(connection, prefix) as (entered, release):
            task = create_task(apply(database, history))
            try:
                async with timeout(2):
                    await entered.wait()

                task.cancel()
                await sleep(0)
                task.cancel()
            finally:
                release.set()

            with raises(CancelledError):
                await task

        assert not connection.in_transaction
        async with connection.execute("PRAGMA foreign_keys") as cursor:
            assert (await cursor.fetchone())[0] == 1

        async with connection.execute("SELECT count(*) FROM sqlite_schema") as cursor:
            assert (await cursor.fetchone())[0] == 0

        await apply(database, history)
        assert all(item.applied for item in await status(database, history))

async def test_sqlite_lock_wait_does_not_block_event_loop(tmp_path, schema, models):
    path = tmp_path / "busy.db"
    async with await Database.create(path) as first, await Database.open(path) as second:
        await apply(first, (await diff((), schema, "initial"),))
        first_client = models.Client(first)
        second_client = models.Client(second)
        entered = Event()
        loop = get_running_loop()

        def trace(statement):
            if statement.startswith("INSERT"):
                loop.call_soon_threadsafe(entered.set)

        await second.connection.set_trace_callback(trace)
        async with first.transaction():
            await first_client.users.create(name="first")
            task = create_task(second_client.users.create(name="second"))
            async with timeout(2):
                await entered.wait()
                await sleep(0.01)

            assert not task.done()

        async with timeout(2):
            await task

        assert await first_client.users.query().count() == 2

async def test_cancelled_open_closes_connection_after_worker_finishes(tmp_path, monkeypatch):
    loop = get_running_loop()
    entered = Event()
    release = ThreadEvent()
    connections = []

    def connect(*args, **kwargs):
        def connector():
            connection = sqlite_connect(":memory:", isolation_level=None)
            loop.call_soon_threadsafe(entered.set)
            release.wait(5)

            return connection

        connection = Connection(connector, 64)
        connections.append(connection)

        return connection

    monkeypatch.setattr("sqrrl.runtime.connect", connect)
    task = create_task(Database.create(tmp_path / "opening.db"))
    try:
        async with timeout(2):
            await entered.wait()

        task.cancel()
        await sleep(0)
        assert not task.done()
    finally:
        release.set()

    with raises(CancelledError):
        await task

    with raises(ValueError, match="no active connection"):
        await connections[0].execute("SELECT 1")

def test_sync_connection_is_rejected():
    connection = sqlite_connect(":memory:")
    try:
        with raises(TypeError, match="aiosqlite"):
            Database(connection)
    finally:
        connection.close()
