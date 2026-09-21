from os import environ
from uuid import uuid4
from sys import modules
from dataclasses import replace
from sqrrl.generate import write
from datetime import UTC, datetime
from sqrrl.migrate import _checksum
from urllib.parse import urlsplit, urlunsplit
from pytest import mark, skip, raises, fixture
from sqrrl.postgres import connect, compile_sql
from sqrrl.schema import datetime as datetime_field
from importlib.util import module_from_spec, spec_from_file_location
from asyncio import Event, sleep, gather, create_task, CancelledError
from sqrrl import Conflict, Database, Increment, SqrrlError, MigrationError, ValidationError
from sqrrl.schema import blob, json, text, Check, Index, Table, Schema, boolean, integer, Relationship
from sqrrl.migrate import diff, load, adopt, apply, check, custom, status, baseline, write as write_migration


@fixture
async def pg_dsn():
    dsn = environ.get('SQRRL_TEST_POSTGRES')
    if not dsn:
        skip('Set SQRRL_TEST_POSTGRES to run PostgreSQL integration tests')
    from asyncpg import connect

    admin = await connect(dsn)
    name = 'sqrrl_test_' + uuid4().hex
    await admin.execute(f'CREATE DATABASE "{name}"')
    parsed = urlsplit(dsn)
    try:
        yield urlunsplit(parsed._replace(path='/' + name))
    finally:
        try:
            await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        finally:
            await admin.close()


@fixture
async def pg_history(pg_dsn, schema):
    return (await diff((), schema, 'initial', dsn=pg_dsn),)


@fixture
async def pg(pg_dsn, pg_history):
    async with await Database.open(pg_dsn, readers=2) as database:
        await apply(database, pg_history)
        yield database


def import_models(tmp_path, schema):
    name = 'generated_pg_' + uuid4().hex
    spec = spec_from_file_location(name, write(tmp_path / f'{name}.py', schema))
    module = module_from_spec(spec)
    modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        modules.pop(name)
    return module


def test_parameter_compilation_preserves_sql_data():
    sql = """SELECT '? main.', "?", $$? main.$$ /* ? /* ? */ main. */ FROM main."items" WHERE "id" = ? -- ?
AND "text" = ?"""
    translated = compile_sql(sql)
    assert translated == sql.replace('FROM main.', 'FROM public.').replace('"id" = ?', '"id" = $1').replace(
        '"text" = ?', '"text" = $2'
    )


async def test_crud_defaults_queries_and_composite_keys(pg, models):
    client = models.Client(pg)
    ada = await client.users.create(name='Ada')
    assert ada.active is True and ada.email == 'default@example.com'
    assert await client.users.get(ada.id) == ada
    assert (await client.users.update(ada.id, email=None)).email is None
    await client.documents.create(id='binary', body=b'\x00\xff', score=1.5)
    assert (await client.documents.get('binary')).body == b'\x00\xff'
    await client.settings.create(user_id=ada.id, key='theme', value='dark')
    assert (await client.settings.get(models.SettingKey(user_id=ada.id, key='theme'))).value == 'dark'
    await client.users.create(name='Grace', active=False)
    assert await client.users.query().where(models.UserColumns.id.in_()).count() == 0
    assert await client.users.query().where(models.UserColumns.active.eq(False)).count() == 1
    assert (await client.users.query().order_by(models.UserColumns.id.asc()).offset(1).only()).name == 'Grace'
    assert await client.users.query().where(models.UserColumns.name.contains('da')).exists()
    assert await client.users.update_where(models.UserColumns.name.eq('Grace'), active=True) == 1
    assert await client.users.delete_where(models.UserColumns.name.eq('Grace')) == 1
    await client.users.delete(ada.id)
    assert await client.settings.query().count() == 0


async def test_bulk_upsert_rollback_and_parameter_batches(pg, models):
    client = models.Client(pg)
    users = await client.users.create_many([models.UserCreate(name=f'user{i}') for i in range(1200)])
    assert len(users) == 1200 and len({user.id for user in users}) == 1200
    user = users[0]
    updated = await client.users.create_many(
        [models.UserCreate(id=user.id, name='updated')],
        conflict=Conflict((models.UserColumns.id,), (models.UserColumns.name,)),
    )
    assert updated[0].name == 'updated'
    skipped = await client.users.create_many(
        [models.UserCreate(id=user.id, name='ignored')], conflict=Conflict((models.UserColumns.id,), ())
    )
    assert skipped == []
    await client.documents.create(id='x', body=b'x', score=1.0)
    assert (await client.documents.update('x', score=Increment(2.5))).score == 3.5
    assert await client.documents.update_where(_all_rows=True, score=Increment(1.0)) == 1
    assert await client.documents.delete_where(all_rows=True) == 1
    with raises(ValidationError):
        await client.users.create_many([models.UserCreate(name='valid'), models.UserCreate(name=123)])
    assert await client.users.query().count() == 1200


async def test_transactions_concurrency_cancellation_and_readers(pg, models):
    client = models.Client(pg)
    async with client.transaction():
        first = await client.users.create(name='outer')
        with raises(ValueError):
            async with client.transaction():
                await client.users.create(name='inner')
                raise ValueError('rollback nested')
    assert await client.users.query().all() == [first]
    entered = Event()

    async def cancelled():
        async with client.transaction():
            await client.users.create(name='cancelled')
            entered.set()
            await sleep(30)

    task = create_task(cancelled())
    await entered.wait()
    task.cancel()
    with raises(CancelledError):
        await task
    assert await client.users.query().count() == 1
    values = await gather(*(client.users.create(name=f'parallel{i}') for i in range(20)))
    assert len(values) == 20
    async with pg.read_connection():
        before = await client.users.query().count()
        await create_task(client.users.create(name='new snapshot'))
        assert await client.users.query().count() == before
    assert await client.users.query().count() == before + 1


async def test_types_relationships_and_literal_parameters(pg_dsn, tmp_path):
    schema = Schema(
        (
            Table(
                'parents',
                'Parent',
                (integer('id').primary_key(), text('name')),
                relationships=(Relationship('children', 'children', ('id',), ('parent_id',), many=True),),
            ),
            Table(
                'children',
                'Child',
                (
                    integer('id').primary_key(),
                    integer('parent_id').references('parents', 'id'),
                    json('payload'),
                    datetime_field('created'),
                    boolean('active').default('1'),
                    blob('body').default("X'00ff'"),
                ),
                relationships=(Relationship('parent', 'parents', ('parent_id',), ('id',)),),
            ),
        )
    )
    models = import_models(tmp_path, schema)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (await diff((), schema, 'initial', dsn=pg_dsn),))
        client = models.Client(database)
        parent = await client.parents.create(name='? main. %_!')
        instant = datetime.now(UTC)
        child = await client.children.create(parent_id=parent.id, payload={'answer': [1, True, None]}, created=instant)
        assert child.payload == {'answer': [1, True, None]} and child.created == instant
        assert child.body == b'\x00\xff'
        loaded = (
            await client.parents.query().load(models.ParentRelations.children.then(models.ChildRelations.parent)).only()
        )
        assert loaded.children[0].parent == parent
        assert (
            await client.parents.query()
            .where(models.ParentRelations.children.exists(models.ChildColumns.active.eq(True)))
            .count()
            == 1
        )
        assert await client.parents.query().where(models.ParentColumns.name.contains('%_!')).count() == 1


async def test_checked_migrations_rename_backfill_drop_and_drift(pg_dsn, tmp_path):
    original = Schema((Table('items', 'Item', (integer('id').primary_key(), text('name').identity('label'))),))
    initial = await diff((), original, 'initial', dsn=pg_dsn)
    write_migration(tmp_path, initial)
    history = load(tmp_path)
    assert history == (initial,) and initial.dialect == 'postgresql'
    desired = Schema(
        (
            Table(
                'things',
                'Item',
                (integer('id').primary_key(), text('title').identity('label'), integer('score').default('5')),
                key='items',
                indexes=(Index('title_idx', ('title',)),),
            ),
        )
    )
    second = await diff(history, desired, 'rename', dsn=pg_dsn)
    backfill = await custom(history + (second,), 'backfill', 'UPDATE things SET score = 9', dsn=pg_dsn)
    history += (second, backfill)
    await check(history, desired, dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        assert not any(item.applied for item in await status(database, history))
        await apply(database, (initial,))
        await database.connection.raw.execute("INSERT INTO items(name) VALUES ('kept')")
        await apply(database, history)
        assert await database.connection.raw.fetchval('SELECT title FROM things') == 'kept'
        assert await database.connection.raw.fetchval('SELECT score FROM things') == 9
        assert all(item.applied for item in await status(database, history))
        await apply(database, history)
        with raises(MigrationError, match='removes'):
            await diff(history, Schema(()), 'drop', dsn=pg_dsn)
        dropped = await diff(history, Schema(()), 'drop', dsn=pg_dsn, allow_drop=True)
        await database.connection.raw.execute('ALTER TABLE things ADD COLUMN drift TEXT')
        with raises(MigrationError, match='drift'):
            await status(database, history)
        with raises(MigrationError, match='drift'):
            await apply(database, history + (dropped,))


async def test_adoption_baseline_and_atomic_migration_failure(pg_dsn, schema):
    initial = await diff((), schema, 'initial', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        for statement in initial.statements:
            await database.connection.raw.execute(statement)
        adopted = await adopt(database, schema)
        await baseline(database, (adopted,), 1)
        await baseline(database, (adopted,), 1)
        assert (await status(database, (adopted,)))[0].applied
        await database.connection.raw.execute("INSERT INTO users(name) VALUES ('duplicate'), ('another')")
        changed = Schema(
            tuple(
                replace(table, checks=(Check('only_duplicate', "name = 'duplicate'"),))
                if table.name == 'users'
                else table
                for table in schema.tables
            )
        )
        migration = await diff((adopted,), changed, 'restrict', dsn=pg_dsn)
        with raises(MigrationError):
            await apply(database, (adopted, migration))
        assert len(await status(database, (adopted,))) == 1
        assert await database.connection.raw.fetchval('SELECT count(*) FROM users') == 2


async def test_adapter_execute_returns_rows_and_command_counts(pg_dsn):
    async with await Database.open(pg_dsn) as database:
        connection = database.connection
        cursor = await connection.execute('CREATE TABLE samples (id BIGINT PRIMARY KEY, value TEXT)')
        assert cursor.rowcount == -1 and await cursor.fetchall() == []
        await cursor.close()
        cursor = await connection.execute('INSERT INTO samples VALUES (?, ?), (?, ?) RETURNING id', (1, 'one', 2, 'two'))
        assert cursor.rowcount == 2
        assert (await cursor.fetchone())['id'] == 1
        assert [row['id'] for row in await cursor.fetchall()] == [2]
        assert await cursor.fetchone() is None
        cursor = await connection.execute('UPDATE samples SET value = ? WHERE id = ?', ('changed', 2))
        assert cursor.rowcount == 1 and await cursor.fetchall() == []
        cursor = await connection.execute('SELECT value FROM samples WHERE id = ?', (2,))
        assert cursor.rowcount == 1 and (await cursor.fetchone())[0] == 'changed'


async def test_postgres_connection_failures_close_started_readers(pg_dsn, monkeypatch):
    from asyncpg import PostgresError

    async def denied(*args, **kwargs):
        raise PostgresError('authentication rejected')

    with monkeypatch.context() as patch:
        patch.setattr('asyncpg.connect', denied)
        with raises(SqrrlError, match='Cannot connect.*authentication rejected'):
            await Database.open(pg_dsn)

    opened = []

    async def connect_reader(dsn, timeout):
        if len(opened) == 2:
            raise OSError('reader connection failed')
        connection = await connect(dsn, timeout)
        opened.append(connection)
        return connection

    monkeypatch.setattr('sqrrl.runtime.pg_connect', connect_reader)
    with raises(OSError, match='reader connection failed'):
        await Database.open(pg_dsn, readers=2)
    assert len(opened) == 2 and all(connection.raw.is_closed() for connection in opened)


async def test_replay_connection_failure_removes_scratch_database(pg_dsn, monkeypatch):

    administrators = []
    failed_databases = []

    async def interrupted_connect(dsn, timeout, *, database=None):
        if database is not None:
            failed_databases.append(database)
            raise OSError('replay connection failed')
        connection = await connect(dsn, timeout)
        administrators.append(connection)
        return connection

    async with await Database.open(pg_dsn) as database:
        monkeypatch.setattr('sqrrl.postgres.connect', interrupted_connect)
        schema = Schema((Table('items', 'Item', (integer('id').primary_key(),)),))
        with raises(OSError, match='replay connection failed'):
            await diff((), schema, 'initial', dsn=pg_dsn)
        assert failed_databases
        for name in failed_databases:
            assert not await database.connection.raw.fetchval('SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)', name)
    assert administrators and all(connection.raw.is_closed() for connection in administrators)


async def test_column_defaults_nullability_and_removal_preserve_data(pg_dsn):
    table = Table('items', 'Item', (integer('id').primary_key(), text('name'), text('old').nullable()))
    initial = await diff((), Schema((table,)), 'initial', dsn=pg_dsn)
    changed = replace(table, fields=(table.fields[0], text('name').nullable().default("'default'")))
    with raises(MigrationError, match='removes fields'):
        await diff((initial,), Schema((changed,)), 'change', dsn=pg_dsn)
    second = await diff((initial,), Schema((changed,)), 'change', allow_drop=True, dsn=pg_dsn)
    restored = replace(changed, fields=(table.fields[0], text('name')))
    third = await diff((initial, second), Schema((restored,)), 'restore', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (initial,))
        await database.connection.raw.execute("INSERT INTO items(name, old) VALUES ('kept', 'discarded')")
        await apply(database, (initial, second))
        await database.connection.raw.execute('INSERT INTO items DEFAULT VALUES')
        await database.connection.raw.execute('INSERT INTO items(name) VALUES (NULL)')
        with raises(MigrationError):
            await apply(database, (initial, second, third))
        assert [item.applied for item in await status(database, (initial, second, third))] == [True, True, False]
        await database.connection.raw.execute('DELETE FROM items WHERE name IS NULL')
        await apply(database, (initial, second, third))
        assert [row[0] for row in await database.connection.raw.fetch('SELECT name FROM items ORDER BY id')] == ['kept', 'default']
        assert all(item.applied for item in await status(database, (initial, second, third)))
    for desired, message in (
        (replace(table, fields=table.fields + (text('required'),)), 'default or a staged backfill'),
        (replace(table, fields=(integer('new_id').primary_key(), *table.fields[1:])), 'primary key changes'),
    ):
        with raises(MigrationError, match=message):
            await diff((initial,), Schema((desired,)), 'invalid', dsn=pg_dsn)


async def test_unmanaged_postgres_objects_are_rejected(pg, pg_history):
    for create, drop, message in (
        ("CREATE FUNCTION extra() RETURNS INT LANGUAGE SQL AS 'SELECT 1'", 'DROP FUNCTION extra()', 'functions'),
        ('CREATE VIEW extra AS SELECT 1 AS value', 'DROP VIEW extra', 'schema object'),
        ('CREATE SEQUENCE extra', 'DROP SEQUENCE extra', 'sequences'),
    ):
        await pg.connection.raw.execute(create)
        with raises(MigrationError, match=message):
            await status(pg, pg_history)
        await pg.connection.raw.execute(drop)
    assert all(item.applied for item in await status(pg, pg_history))


async def test_postgres_ledger_tampering_is_rejected(pg, pg_history):
    with raises(MigrationError, match='newer'):
        await status(pg, ())
    for alteration, restore, message in (
        ("UPDATE sqrrl_migrations SET name = 'tampered'", "UPDATE sqrrl_migrations SET name = 'initial'", 'differs'),
        ('ALTER TABLE sqrrl_migrations SET UNLOGGED', 'ALTER TABLE sqrrl_migrations SET LOGGED', 'ledger schema'),
        ('CREATE INDEX extra ON sqrrl_migrations(name)', 'DROP INDEX extra', 'ledger schema'),
    ):
        await pg.connection.raw.execute(alteration)
        with raises(MigrationError, match=message):
            await apply(pg, pg_history)
        await pg.connection.raw.execute(restore)
    assert all(item.applied for item in await status(pg, pg_history))


async def test_postgres_baseline_and_adoption_require_matching_untracked_schema(pg_dsn, pg_history, schema):
    second = await custom(pg_history, 'noop', 'SELECT 1', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        for version in (0, True, 3):
            with raises(MigrationError, match='outside'):
                await baseline(database, pg_history, version)
        with raises(MigrationError, match='exact schema match'):
            await baseline(database, pg_history, 1)
        with raises(MigrationError, match='declared structure'):
            await adopt(database, schema)
        with raises(MigrationError, match='reviewed CREATE'):
            await adopt(database, schema, preserve_sql=('DELETE FROM users',))
        await apply(database, pg_history)
        with raises(MigrationError, match='conflicts'):
            await baseline(database, pg_history + (second,), 2)
        with raises(MigrationError, match='without Sqrrl history'):
            await adopt(database, schema)
        async with database.transaction():
            with raises(MigrationError, match='outside a transaction'):
                await apply(database, pg_history)
        assert all(item.applied for item in await status(database, pg_history))


async def test_postgres_incorrect_fingerprint_rolls_back_schema_and_ledger(pg_dsn, pg_history):
    inconsistent = replace(pg_history[0], after='0' * 64)
    inconsistent = replace(inconsistent, checksum=_checksum(inconsistent))
    async with await Database.open(pg_dsn) as database:
        with raises(MigrationError, match='expected schema'):
            await apply(database, (inconsistent,))
        assert not any(item.applied for item in await status(database, pg_history))
        assert await database.connection.raw.fetchval("SELECT to_regclass('public.sqrrl_migrations')") is None


async def test_adoption_of_handwritten_constraints_and_external_history(pg_dsn):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(), text('name').unique())),))
    preserved = ('CREATE TABLE external_history (version BIGINT PRIMARY KEY);',)
    async with await Database.open(pg_dsn) as database:
        await database.connection.raw.execute(
            'CREATE TABLE items (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT NOT NULL CONSTRAINT human_unique UNIQUE);'
        )
        await database.connection.raw.execute(preserved[0])
        await database.connection.raw.execute("INSERT INTO items(name) VALUES ('retained')")
        adopted = await adopt(database, schema, preserve_sql=preserved)
        await baseline(database, (adopted,), 1)
        await check((adopted,), schema, dsn=pg_dsn)
        desired = Schema(
            (
                replace(schema.tables[0], fields=schema.tables[0].fields + (text('description').nullable(),)),
                Table('audit_notes', 'AuditNote', (integer('id').primary_key(), text('body'))),
            )
        )
        migration = await diff((adopted,), desired, 'add_description', dsn=pg_dsn)
        await apply(database, (adopted, migration))
        assert await database.connection.raw.fetchval('SELECT name FROM items') == 'retained'
        assert await database.connection.raw.fetchval("SELECT to_regclass('external_history')")


async def test_custom_scripts_are_isolated_and_ledger_is_checked(pg_dsn):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(), text('name'))),))
    initial = await diff((), schema, 'initial', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (initial,))
        await database.connection.raw.execute("INSERT INTO items(name) VALUES ('live')")
        sql = "UPDATE public.items SET name = 'must not run on live data'; INSERT INTO items(name) VALUES ($body$a; ? main.$body$);"
        backfill = await custom((initial,), 'backfill', sql, dsn=pg_dsn)
        assert await database.connection.raw.fetchval('SELECT name FROM items') == 'live'
        assert len(backfill.statements) == 2
        with raises(MigrationError):
            await custom((initial,), 'bad', 'DELETE FROM sqrrl_migrations;', dsn=pg_dsn)
        with raises(MigrationError):
            await custom((initial,), 'bad', "UPDATE items SET name = 'x'; COMMIT;", dsn=pg_dsn)
        with raises(MigrationError):
            await custom((initial,), 'bad', '-- empty', dsn=pg_dsn)
        await apply(database, (initial, backfill))
        assert await database.connection.raw.fetchval('SELECT count(*) FROM items') == 2
        await database.connection.raw.execute('ALTER TABLE sqrrl_migrations ADD COLUMN hidden TEXT')
        with raises(MigrationError, match='ledger schema'):
            await status(database, (initial, backfill))


async def test_pg_history_cannot_be_applied_to_sqlite(pg_dsn, tmp_path):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(),)),))
    migration = await diff((), schema, 'initial', dsn=pg_dsn)
    async with await Database.create(tmp_path / 'wrong.db') as database:
        with raises(MigrationError, match='SQLite migration history'):
            await apply(database, (migration,))
    sqlite_migration = await diff((), schema, 'initial')
    async with await Database.open(pg_dsn) as database:
        with raises(MigrationError, match='PostgreSQL migration history'):
            await apply(database, (sqlite_migration,))


async def test_enums_codecs_factories_and_composite_relationships(pg_dsn):
    from examples.library_schema import schema
    from examples.library_types import Binding, Label
    from examples.library_models import Client, BookRelations, ShelfRelations

    async with await Database.open(pg_dsn) as database:
        await apply(database, (await diff((), schema, 'initial', dsn=pg_dsn),))
        client = Client(database)
        shelf = await client.shelves.create(room='reading', number=2, name='Fiction')
        book = await client.books.create(isbn='one', title='Book', room='reading', shelf=2, label=Label('custom'))
        assert book.binding is Binding.PAPER and book.label == Label('custom')
        assert book.created.tzinfo is UTC and book.metadata == {}
        updated = await client.books.update(book.id, copies=Increment(2), binding=Binding.CLOTH)
        assert updated.copies == 2 and updated.edited is not None
        loaded = await client.books.query().load(BookRelations.location.then(ShelfRelations.books)).only()
        assert loaded.location.name == shelf.name
        assert loaded.location.books[0].binding is Binding.CLOTH


async def test_partial_unique_conflicts_and_wide_parameter_batches(pg, models, tmp_path):
    client = models.Client(pg)
    first = await client.users.create(name='same')
    rows = await client.users.create_many(
        [models.UserCreate(name='same', email='updated')],
        conflict=Conflict((models.UserColumns.name,), (models.UserColumns.email,), index='active_names'),
    )
    assert rows[0].id == first.id and rows[0].email == 'updated'
    fields = (integer('id').primary_key(), *(text(f'value{i}') for i in range(40)))
    schema = Schema((Table('wide', 'Wide', fields),))
    generated = import_models(tmp_path, schema)
    await pg.connection.raw.execute(schema.normalize().tables[0].create_sql(dialect='postgresql'))
    repository = generated.Client(pg).wide
    rows = await repository.create_many(
        [generated.WideCreate(**{f'value{i}': str(n) for i in range(40)}) for n in range(1000)]
    )
    assert len(rows) == 1000 and len({row.id for row in rows}) == 1000


async def test_cancellation_during_sql_cleans_transaction(pg, models):
    client = models.Client(pg)
    entered = Event()

    async def writer():
        async with client.transaction():
            await client.users.create(name='rolled back')
            entered.set()
            await pg.connection.execute_fetchall('SELECT pg_sleep(0.3)')

    task = create_task(writer())
    await entered.wait()
    await sleep(0.02)
    task.cancel()
    with raises(CancelledError):
        await task
    assert await client.users.query().count() == 0
    assert (await client.users.create(name='usable')).name == 'usable'


async def test_cli_postgres_workflow(pg_dsn, tmp_path, monkeypatch, capsys):
    from sqrrl.cli import _main

    config = tmp_path / 'sqrrl.json'
    monkeypatch.setenv('SQRRL_DATABASE_URL', pg_dsn)
    assert await _main(['init', '--config', str(config)]) == 0
    assert await _main(['generate', '--config', str(config)]) == 0
    for command in (['diff', 'initial'], ['check'], ['status'], ['up'], ['status']):
        assert await _main(['migrate', *command, '--config', str(config)]) == 0
    assert '000001 initial applied' in capsys.readouterr().out


async def test_concurrent_migration_runners(pg_dsn):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(),)),))
    history = (await diff((), schema, 'initial', dsn=pg_dsn),)
    async with await Database.open(pg_dsn) as first, await Database.open(pg_dsn) as second:
        await gather(apply(first, history), apply(second, history))
        await gather(apply(first, history), status(first, history))
        assert (await status(first, history))[0].applied
        assert await first.connection.raw.fetchval('SELECT count(*) FROM sqrrl_migrations') == 1


async def test_foreign_keys_to_unique_indexes_and_representation_guards(pg_dsn):
    from sqrrl.schema import enum

    parent = Table(
        'parents',
        'Parent',
        (integer('id').primary_key(), text('name')),
        indexes=(Index('unique_name', ('name',), unique=True),),
    )
    child = Table('children', 'Child', (integer('id').primary_key(), text('parent_name').references('parents', 'name')))
    schema = Schema((parent, child))
    first = await diff((), schema, 'initial', dsn=pg_dsn)
    renamed = Schema((replace(parent, indexes=(Index('new_unique_name', ('name',), unique=True),)), child))
    second = await diff((first,), renamed, 'rename_index', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (first,))
        await database.connection.raw.execute(
            "INSERT INTO parents(name) VALUES ('valid'); INSERT INTO children(parent_name) VALUES ('valid')"
        )
        await apply(database, (first, second))
        assert await database.connection.raw.fetchval('SELECT parent_name FROM children') == 'valid'
    # TEXT storage alone cannot establish that changing Python representation is safe.
    changed = Schema(
        (
            replace(parent, fields=(integer('id').primary_key(), enum('name', 'examples.library_types:Binding'))),
            replace(
                child,
                fields=(
                    integer('id').primary_key(),
                    enum('parent_name', 'examples.library_types:Binding').references('parents', 'name'),
                ),
            ),
        )
    )
    with raises(MigrationError, match='representation changes'):
        await diff((first,), changed, 'change_representation', dsn=pg_dsn)


async def test_portable_constant_predicates_do_not_resolve_column_names(tmp_path):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(), integer('TRUE'), integer('FALSE'))),))
    models = import_models(tmp_path, schema)
    async with await Database.create(tmp_path / 'constants.db') as database:
        await apply(database, (await diff((), schema, 'initial'),))
        client = models.Client(database)
        await client.items.create(TRUE=0, FALSE=1)
        assert await client.items.query().where(models.ItemColumns.id.in_()).count() == 0
        assert await client.items.delete_where(all_rows=True) == 1


async def test_migration_name_swaps_preserve_identities(pg_dsn):
    first = Table(
        'first',
        'First',
        (integer('id').primary_key(), text('left').identity('a'), text('right').identity('b')),
        key='first_identity',
    )
    second = Table('second', 'Second', (integer('id').primary_key(), text('value')), key='second_identity')
    schema = Schema((first, second))
    initial = await diff((), schema, 'initial', dsn=pg_dsn)
    swapped = Schema(
        (
            replace(
                first,
                name='second',
                fields=(integer('id').primary_key(), text('left').identity('b'), text('right').identity('a')),
            ),
            replace(second, name='first'),
        )
    )
    migration = await diff((initial,), swapped, 'swap_names', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (initial,))
        await database.connection.raw.execute(
            "INSERT INTO first(\"left\", \"right\") VALUES ('L', 'R'); INSERT INTO second(value) VALUES ('second')"
        )
        await apply(database, (initial, migration))
        assert tuple(await database.connection.raw.fetchrow('SELECT "left", "right" FROM second')) == ('R', 'L')
        assert await database.connection.raw.fetchval('SELECT value FROM first') == 'second'


def test_sqlite_import_and_generation_do_not_require_asyncpg(tmp_path):
    from sys import executable
    from subprocess import run

    script = """
from sys import meta_path
from importlib.abc import MetaPathFinder
class Block(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'asyncpg':
            raise ModuleNotFoundError('not installed')
meta_path.insert(0, Block())
from asyncio import run
from sqrrl import Database, SqrrlError
from sqrrl.generate import render
from examples.schema import schema
assert 'class Client' in render(schema)
async def main():
    try:
        await Database.open('postgresql://localhost/postgres')
    except SqrrlError as error:
        assert 'sqrrl[pg]' in str(error)
    else:
        raise AssertionError('missing pg extra was not reported')
run(main())
"""
    result = run([executable, '-c', script], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


async def test_caught_postgres_error_cannot_silently_roll_back(pg, models):
    from asyncpg import DivisionByZeroError

    client = models.Client(pg)
    with raises(SqrrlError, match='aborted'):
        async with client.transaction():
            await client.users.create(name='must roll back')
            with raises(DivisionByZeroError):
                await pg.connection.raw.execute('SELECT 1 / 0')
    assert await client.users.query().count() == 0
    assert (await client.users.create(name='usable')).name == 'usable'


async def test_temporary_tables_cannot_shadow_repositories_or_migrations(pg_dsn, schema, models):
    history = (await diff((), schema, 'initial', dsn=pg_dsn),)
    async with await Database.open(pg_dsn) as database:
        await apply(database, history)
        client = models.Client(database)
        user = await client.users.create(name='public user')
        await database.connection.raw.execute('CREATE TEMP TABLE users (LIKE public.users INCLUDING ALL)')
        assert await client.users.query().all() == [user]
        await database.connection.raw.execute(
            'CREATE TEMP TABLE sqrrl_migrations (LIKE public.sqrrl_migrations INCLUDING ALL)'
        )
        assert (await status(database, history))[0].applied
        await database.connection.raw.execute('CREATE SCHEMA other; SET search_path TO other')
        assert await client.users.get(user.id) == user
        assert (await status(database, history))[0].applied


async def test_disabled_foreign_key_enforcement_is_drift(pg, pg_history):
    await pg.connection.raw.execute('ALTER TABLE tasks DISABLE TRIGGER ALL')
    with raises(MigrationError):
        await status(pg, pg_history)


async def test_rewrite_rules_are_drift(pg, pg_history):
    await pg.connection.raw.execute('CREATE RULE ignore_delete AS ON DELETE TO users DO INSTEAD NOTHING')
    with raises(MigrationError):
        await status(pg, pg_history)


async def test_custom_sql_handles_postgres_comments_and_dollar_quotes(pg_dsn):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(), text('name'))),))
    initial = await diff((), schema, 'initial', dsn=pg_dsn)
    sql = '/* outer /* inner */ comment */ INSERT/* no space */ INTO items(name) VALUES ($é$a; b$é$);'
    migration = await custom((initial,), 'backfill', sql, dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (initial, migration))
        assert await database.connection.raw.fetchval('SELECT name FROM items') == 'a; b'


async def test_add_column_preserves_constraints_referenced_by_external_tables(pg_dsn):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key(), text('name'))),))
    preserved = ('CREATE TABLE external_history (id BIGINT PRIMARY KEY, item_id BIGINT REFERENCES items(id));',)
    async with await Database.open(pg_dsn) as database:
        initial = await diff((), schema, 'initial', dsn=pg_dsn)
        for sql in initial.statements + preserved:
            await database.connection.raw.execute(sql)
        adopted = await adopt(database, schema, preserve_sql=preserved)
        await baseline(database, (adopted,), 1)
        old_oid = await database.connection.raw.fetchval(
            "SELECT indexrelid FROM pg_index WHERE indrelid = 'items'::regclass AND indisprimary"
        )
        desired = Schema((replace(schema.tables[0], fields=schema.tables[0].fields + (text('notes').nullable(),)),))
        added = await diff((adopted,), desired, 'notes', dsn=pg_dsn)
        await apply(database, (adopted, added))
        assert (
            await database.connection.raw.fetchval(
                "SELECT indexrelid FROM pg_index WHERE indrelid = 'items'::regclass AND indisprimary"
            )
            == old_oid
        )


@mark.stress
async def test_large_composite_relationship_load(pg_dsn, tmp_path):
    from sqrrl.schema import ForeignKey

    schema = Schema(
        (
            Table(
                'parents',
                'Parent',
                (integer('region'), integer('number')),
                primary_key=('region', 'number'),
                relationships=(
                    Relationship('children', 'children', ('region', 'number'), ('region', 'number'), many=True),
                ),
            ),
            Table(
                'children',
                'Child',
                (integer('id').primary_key(), integer('region'), integer('number')),
                foreign_keys=(ForeignKey(('region', 'number'), 'parents', ('region', 'number')),),
            ),
        )
    )
    models = import_models(tmp_path, schema)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (await diff((), schema, 'initial', dsn=pg_dsn),))
        await database.connection.raw.copy_records_to_table('parents', records=[(i, i) for i in range(5000)])
        await database.connection.raw.copy_records_to_table('children', records=[(i, i, i) for i in range(5000)])
        rows = await models.Client(database).parents.query().load(models.ParentRelations.children).all()
        assert len(rows) == 5000 and all(len(row.children) == 1 for row in rows)


def test_constraint_renaming_is_table_scoped_and_preserves_literals():
    from sqrrl.pg_migrate import _rename_constraints

    statements = (
        """CREATE TABLE "first" (value TEXT DEFAULT 'CONSTRAINT "a" CHECK', CONSTRAINT "a" CHECK (value <> 'CONSTRAINT "b" CHECK'), CONSTRAINT "b" CHECK (value <> ''));""",
        'ALTER TABLE "second" DROP CONSTRAINT "a";',
    )
    mapping = {('first', 'a'): 'b', ('first', 'b'): 'c', ('second', 'a'): 'd'}
    assert _rename_constraints(statements, mapping) == (
        """CREATE TABLE "first" (value TEXT DEFAULT 'CONSTRAINT "a" CHECK', CONSTRAINT "b" CHECK (value <> 'CONSTRAINT "b" CHECK'), CONSTRAINT "c" CHECK (value <> ''));""",
        'ALTER TABLE "second" DROP CONSTRAINT "d";',
    )


async def test_cursor_batches_preserve_order_and_validate_size():
    from sqrrl.postgres import Cursor

    cursor = Cursor([(1,), (2,), (3,), (4,)], 4)
    with raises(ValueError):
        await cursor.fetchmany(-1)
    assert await cursor.fetchmany(0) == []
    assert await cursor.fetchone() == (1,)
    assert await cursor.fetchmany(2) == [(2,), (3,)]
    assert await cursor.fetchall() == [(4,)]
    assert await cursor.fetchone() is None
    assert await cursor.fetchmany(5) == []
    assert cursor.rowcount == 4
    await cursor.close()


async def test_adoption_preserves_duplicate_checks_and_postgres_literals(pg_dsn, tmp_path):
    schema = Schema(
        (
            Table(
                'items',
                'Item',
                (integer('id').primary_key(), integer('value')),
                checks=(Check('positive', 'value >= 0'), Check('also_positive', 'value >= 0')),
            ),
        )
    )
    preserved = ('/* outer /* nested */ comment */ CREATE TABLE external_history (value TEXT DEFAULT $é$a; b$é$);',)
    async with await Database.open(pg_dsn) as database:
        await database.connection.raw.execute(
            'CREATE TABLE items (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, value BIGINT NOT NULL CONSTRAINT a CHECK (value >= 0) CONSTRAINT b CHECK (value >= 0))'
        )
        await database.connection.raw.execute(preserved[0])
        adopted = await adopt(database, schema, preserve_sql=preserved)
        write_migration(tmp_path, adopted)
        assert load(tmp_path) == (adopted,)
        await baseline(database, (adopted,), 1)
        desired = Schema(
            (replace(schema.tables[0], checks=(Check('positive', 'value > 0'), Check('also_positive', 'value >= 0'))),)
        )
        changed = await diff((adopted,), desired, 'tighten', dsn=pg_dsn)
        await apply(database, (adopted, changed))
        await database.connection.raw.execute('INSERT INTO external_history DEFAULT VALUES')
        assert await database.connection.raw.fetchval('SELECT value FROM external_history') == 'a; b'


async def test_column_identity_swaps_rebuild_dependent_expressions(pg_dsn):
    from sqrrl.schema import ForeignKey

    parent = Table(
        'parents',
        'Parent',
        (integer('id').primary_key(), integer('left').identity('a').unique(), integer('right').identity('b').unique()),
        checks=(Check('positive', '"left" > 0'),),
        indexes=(Index('partial_left', ('left',), where='"right" > 0'),),
    )
    child = Table(
        'children',
        'Child',
        (integer('id').primary_key(), integer('parent')),
        foreign_keys=(ForeignKey(('parent',), 'parents', ('left',)),),
    )
    schema = Schema((parent, child))
    initial = await diff((), schema, 'initial', dsn=pg_dsn)
    desired = Schema(
        (
            replace(
                parent,
                fields=(
                    integer('id').primary_key(),
                    integer('left').identity('b').unique(),
                    integer('right').identity('a').unique(),
                ),
            ),
            child,
        )
    )
    swapped = await diff((initial,), desired, 'swap', dsn=pg_dsn)
    async with await Database.open(pg_dsn) as database:
        await apply(database, (initial,))
        await database.connection.raw.execute(
            'INSERT INTO parents("left", "right") VALUES (1, 2), (2, 1); INSERT INTO children(parent) VALUES (1)'
        )
        await apply(database, (initial, swapped))
        assert tuple(await database.connection.raw.fetchrow('SELECT "left", "right" FROM parents WHERE id = 1')) == (
            2,
            1,
        )
        await check((initial, swapped), desired, dsn=pg_dsn)


def test_user_check_cannot_replace_generated_primary_key():
    from sqrrl import SchemaError
    from sqrrl.pg_schema import constraints

    table = Schema((Table('items', 'Item', (integer('id').primary_key(),)),)).normalize().tables[0]
    name = next(iter(constraints(table)))
    with raises(SchemaError, match='conflicts with a generated constraint'):
        replace(table, checks=(Check(name, 'id > 0'),)).create_sql(dialect='postgresql')
