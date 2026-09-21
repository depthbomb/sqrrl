from json import dumps
from pathlib import Path
from dataclasses import replace
from sqrrl.migrate import _checksum
from pytest import mark, raises, fixture
from sqrrl import Database, MigrationError
from sqrrl.schema import text, Table, Schema, integer
from sqrrl.migrate import diff, load, adopt, apply, write, custom, status, baseline


@fixture
async def initial():
    return await diff((), Schema((Table('items', 'Item', (integer('id').primary_key(), text('name'))),)), 'initial')


@mark.parametrize(
    'changes, message',
    [
        ({'dialect': 'unknown'}, 'dialect'),
        ({'structural': 1}, 'adoption'),
        ({'preserve_sql': ('CREATE TABLE external_history (id INTEGER)',)}, 'adoption'),
        ({'structural': True, 'preserve_sql': ('DELETE FROM items',)}, 'Preserved'),
        ({'structural': True, 'preserve_sql': (42,)}, 'Preserved'),
        ({'schema': None}, 'schema'),
        ({'name': 'renamed'}, 'invalid migration metadata'),
    ],
)
async def test_invalid_migration_write_creates_no_files(tmp_path, initial, changes, message):
    with raises(MigrationError, match=message):
        write(tmp_path / 'history', replace(initial, **changes))
    assert not (tmp_path / 'history').exists()


@mark.parametrize(
    'changes, message',
    [
        ({'dialect': 'postgresql'}, 'dialect must remain'),
        ({'structural': True}, 'metadata must remain'),
        ({'version': 3}, 'version'),
    ],
)
async def test_inconsistent_history_is_rejected_before_replay(initial, changes, message):
    second = await custom((initial,), 'noop', 'SELECT 1')
    with raises(MigrationError, match=message):
        await diff((initial, replace(second, **changes)), initial.schema, 'unchanged')


@mark.parametrize('content', ['{', '{}', '{"schema": {"tables": 1}}'])
def test_invalid_history_files_report_the_filename(tmp_path, content):
    (tmp_path / '000001_initial.json').write_text(content)
    with raises(MigrationError, match='Cannot load 000001_initial.json'):
        load(tmp_path)


async def test_missing_sql_file_is_reported(tmp_path, initial):
    (tmp_path / '000001_initial.json').write_text(dumps(initial.to_dict()))
    with raises(MigrationError, match='Cannot load'):
        load(tmp_path)


async def test_partial_migration_write_is_cleaned_up(tmp_path, initial, monkeypatch):
    original = Path.open

    def open_file(path, *args, **kwargs):
        if path.suffix == '.json':
            raise PermissionError('metadata is unwritable')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', open_file)
    with raises(PermissionError, match='unwritable'):
        write(tmp_path, initial)
    assert list(tmp_path.iterdir()) == []


async def test_ledger_tampering_and_incomplete_history_are_rejected(tmp_path, initial):
    async with await Database.create(tmp_path / 'history.db') as database:
        await apply(database, (initial,))
        with raises(MigrationError, match='newer'):
            await status(database, ())
        async with database.connection.execute("UPDATE sqrrl_migrations SET name = 'changed'"):
            pass
        with raises(MigrationError, match='differs'):
            await apply(database, (initial,))
        async with database.connection.execute('ALTER TABLE sqrrl_migrations ADD COLUMN extra TEXT'):
            pass
        with raises(MigrationError, match='ledger schema'):
            await status(database, (initial,))


async def test_baseline_and_adoption_reject_conflicting_history(tmp_path, initial):
    second = await custom((initial,), 'noop', 'SELECT 1')
    async with await Database.create(tmp_path / 'existing.db') as database:
        await apply(database, (initial,))
        for version in (0, True, 3):
            with raises(MigrationError, match='outside'):
                await baseline(database, (initial, second), version)
        with raises(MigrationError, match='conflicts'):
            await baseline(database, (initial, second), 2)
        with raises(MigrationError, match='without Sqrrl history'):
            await adopt(database, initial.schema)
        with raises(MigrationError, match='reviewed CREATE'):
            await adopt(database, initial.schema, preserve_sql=('DELETE FROM items',))
        assert all(item.applied for item in await status(database, (initial,)))


@mark.parametrize(
    'sql, message', [('-- only a comment', 'empty'), ('/* unclosed', 'Unterminated'), ('SELECT 1', 'names must start')]
)
async def test_invalid_custom_migrations(initial, sql, message):
    with raises(MigrationError, match=message):
        await custom((initial,), 'Invalid', sql)


async def test_invalid_dialect_and_missing_postgres_dsn(initial):
    with raises(MigrationError, match='Unknown dialect'):
        await diff((), initial.schema, 'initial', dialect='unknown')
    with raises(MigrationError, match='PostgreSQL DSN'):
        await diff((), initial.schema, 'initial', dialect='postgresql')
    with raises(MigrationError, match='PostgreSQL DSN'):
        await custom((), 'initial', 'SELECT 1', dsn='app.db')


async def test_allow_drop_removes_table_and_preserves_remaining_data(tmp_path, initial):
    expanded = replace(
        initial.schema, tables=initial.schema.tables + (Table('obsolete', 'Obsolete', (integer('id').primary_key(),)),)
    )
    first = await diff((), expanded, 'initial')
    with raises(MigrationError, match='removes tables'):
        await diff((first,), initial.schema, 'remove')
    second = await diff((first,), initial.schema, 'remove', allow_drop=True)
    async with await Database.create(tmp_path / 'drop.db') as database:
        await apply(database, (first,))
        async with database.connection.execute("INSERT INTO items(name) VALUES ('kept')"):
            pass
        await apply(database, (first, second))
        async with database.connection.execute('SELECT name FROM items') as cursor:
            assert (await cursor.fetchone())[0] == 'kept'
        assert all(item.applied for item in await status(database, (first, second)))


async def test_incorrect_declared_fingerprint_rolls_back_schema_and_ledger(tmp_path, initial):
    # A valid checksum alone must never bypass the checked replay's schema comparison.
    inconsistent = replace(initial, after='0' * 64)
    inconsistent = replace(inconsistent, checksum=_checksum(inconsistent))
    async with await Database.create(tmp_path / 'checked.db') as database:
        with raises(MigrationError, match='expected schema'):
            await apply(database, (inconsistent,))
        assert not any(item.applied for item in await status(database, (initial,)))
        async with database.connection.execute('SELECT name FROM sqlite_schema') as cursor:
            assert await cursor.fetchall() == []
