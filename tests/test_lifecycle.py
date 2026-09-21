from sys import modules
from asyncio import Event, gather
from types import SimpleNamespace
from unittest.mock import AsyncMock
from sqrrl import Database, SqrrlError
from pytest import mark, raises, importorskip
from aiosqlite import connect as sqlite_connect
from sqrrl.postgres import connect as pg_connect


@mark.parametrize(
    'options', [{'wal': True}, {'immediate': True}, {'timeout': 0}, {'timeout': -1}, {'timeout': float('inf')}]
)
async def test_postgres_rejects_invalid_options_before_connecting(options):
    with raises(ValueError):
        await Database.open('postgresql://invalid.invalid/test', **options)


async def test_missing_postgres_extra_has_installation_hint(monkeypatch):
    monkeypatch.setitem(modules, 'asyncpg', None)
    with raises(SqrrlError, match=r'sqrrl\[pg\]'):
        await pg_connect('postgresql://localhost/test', 1)


async def test_unsupported_postgres_version_closes_connection(monkeypatch):
    importorskip('asyncpg')
    raw = SimpleNamespace(get_server_version=lambda: SimpleNamespace(major=14), close=AsyncMock())
    monkeypatch.setattr('asyncpg.connect', AsyncMock(return_value=raw))
    with raises(SqrrlError, match='PostgreSQL 15 or newer'):
        await pg_connect('postgresql://localhost/test', 1)
    raw.close.assert_awaited_once()


async def test_unsupported_sqlite_version_does_not_create_file(tmp_path, monkeypatch):
    monkeypatch.setattr('sqrrl.runtime.sqlite_version_info', (3, 36, 0))
    with raises(SqrrlError, match='3.37.0'):
        await Database.create(tmp_path / 'old.db')
    assert not (tmp_path / 'old.db').exists()


async def test_wal_refusal_closes_connection(tmp_path, monkeypatch):
    connection = sqlite_connect(':memory:', isolation_level=None)
    monkeypatch.setattr('sqrrl.runtime.connect', lambda *args, **kwargs: connection)
    with raises(SqrrlError, match='could not enable WAL'):
        await Database.create(tmp_path / 'wal.db', wal=True)
    with raises(ValueError, match='closed|no active'):
        await connection.execute('SELECT 1')


@mark.parametrize('readers', [0, 1])
async def test_close_in_active_scope_is_rejected_and_closed_database_cannot_be_reused(tmp_path, readers):
    database = await Database.create(tmp_path / 'lifecycle.db', readers=readers)
    async with database:
        async with database.transaction():
            with raises(SqrrlError, match='Leave the transaction'):
                await database.close()
        async with database.read_connection():
            with raises(SqrrlError, match='Leave the'):
                await database.close()
    await database.close()
    with raises(SqrrlError, match='closing or closed'):
        async with database.transaction():
            pass
    with raises(SqrrlError, match='closing or closed'):
        async with database.read_connection():
            pass


async def test_concurrent_close_closes_connections_once(tmp_path, monkeypatch):
    database = await Database.create(tmp_path / 'closing.db')
    original = database.connection.close
    entered = Event()
    calls = 0

    async def close():
        nonlocal calls
        calls += 1
        entered.set()
        await original()

    async def second_close():
        await entered.wait()
        await database.close()

    monkeypatch.setattr(database.connection, 'close', close)
    await gather(database.close(), second_close())
    assert calls == 1


async def test_failed_close_still_closes_other_connections(tmp_path, monkeypatch):
    database = await Database.create(tmp_path / 'cleanup.db', readers=2)
    connections = database._readers + [database.connection]
    original = connections[0].close

    async def close():
        await original()
        raise OSError('reader cleanup failed')

    monkeypatch.setattr(connections[0], 'close', close)
    with raises(OSError, match='reader cleanup failed'):
        await database.close()
    for connection in connections:
        with raises(ValueError, match='closed|no active'):
            await connection.execute('SELECT 1')
    await database.close()
