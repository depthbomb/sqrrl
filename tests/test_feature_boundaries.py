from test_cli import invoke
from dataclasses import replace
from pytest import mark, raises
from test_async import pause_sql
from test_regressions import generated
from aiosqlite import Connection, connect
from threading import Event as ThreadEvent
from sqlite3 import connect as sqlite_connect
from sqlite3 import IntegrityError, OperationalError, SQLITE_LIMIT_VARIABLE_NUMBER
from sqrrl.migrate import adopt, apply, baseline, check, diff, load, status, write
from asyncio import CancelledError, Event, create_task, gather, get_running_loop, sleep, wait_for
from sqrrl.schema import Check, ForeignKey, Index, Relationship, Schema, Table, integer, json, text
from sqrrl import Conflict, Database, Increment, MigrationError, SchemaError, SqrrlError, ValidationError


def simple_schema():
    return Schema((Table('counts', 'Count', (integer('id').primary_key(), integer('value').nullable().default('0'))),))


async def test_empty_defaults_nulls_and_extreme_parameter_limits(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    async with await Database.create(tmp_path / 'defaults.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        assert await client.counts.create_many([]) == []
        rows = await client.counts.create_many(
            [model.CountCreate(), model.CountCreate(value=None), model.CountCreate(value=7)]
        )
        assert {row.value for row in rows} == {0, None, 7}
        assert await client.counts.update_where(model.CountColumns.value.eq(None), value=Increment(1)) == 1
        assert await client.counts.query().where(model.CountColumns.value.eq(None)).count() == 1
        await db.connection._execute(db.connection._conn.setlimit, SQLITE_LIMIT_VARIABLE_NUMBER, 1)
        assert len(await client.counts.create_many([model.CountCreate(value=i) for i in range(4)])) == 4
        with raises(ValidationError, match='parameter limit'):
            await client.counts.create_many([model.CountCreate(id=100, value=1)])


async def test_bulk_integer_primary_key_defaults_match_single_create(tmp_path):
    schema = Schema((Table('items', 'Item', (integer('id').primary_key().default('99'),)),))
    model = generated(schema)
    async with await Database.create(tmp_path / 'rowid.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        assert (await client.items.create()).id == 1
        assert [row.id for row in await client.items.create_many([model.ItemCreate(), model.ItemCreate()])] == [2, 3]


async def test_partial_conflict_targets_and_update_constraint_rollback(tmp_path):
    table = Table(
        'items',
        'Item',
        (integer('id').primary_key(), text('name'), integer('active').default('1'), integer('value').default('0')),
        indexes=(Index('active_name', ('name',), unique=True, where='active = 1'),),
        checks=(Check('positive', 'value >= 0'),),
    )
    schema = Schema((table,))
    model = generated(schema)
    async with await Database.create(tmp_path / 'partial.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        await client.items.create(name='same')
        conflict = Conflict((model.ItemColumns.name,), (model.ItemColumns.value,), index='active_name')
        assert (await client.items.insert(model.ItemCreate(name='same', value=2), conflict=conflict)).value == 2
        assert (
            await client.items.insert(
                model.ItemCreate(name='same'), conflict=Conflict((model.ItemColumns.name,), index='active_name')
            )
            is None
        )
        with raises(IntegrityError):
            await client.items.create_many(
                [model.ItemCreate(name='other'), model.ItemCreate(name='same', value=-1)], conflict=conflict
            )
        assert await client.items.query().count() == 1
        assert (await client.items.query().only()).value == 2


@mark.parametrize('non_strict', [False, True])
async def test_increment_overflow_rolls_back(tmp_path, non_strict):
    schema = Schema((replace(simple_schema().tables[0], non_strict=non_strict),))
    model = generated(schema)
    async with await Database.create(tmp_path / 'overflow.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        row = await client.counts.create(value=2**63 - 1)
        with raises((IntegrityError, ValidationError)):
            await client.counts.update_where(model.CountColumns.id.eq(row.id), value=Increment(1))
        assert (await client.counts.get(row.id)).value == 2**63 - 1


async def test_conditional_writes_across_independent_connections(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    path = tmp_path / 'race.db'
    async with (
        await Database.create(path, immediate=True, wal=True) as first,
        await Database.open(path, immediate=True) as second,
    ):
        await apply(first, (await diff((), schema, 'initial'),))
        clients = [model.Client(first), model.Client(second)]
        await clients[0].counts.create(value=0)
        results = await gather(
            *(clients[i % 2].counts.update_where(model.CountColumns.value.eq(0), value=Increment(1)) for i in range(20))
        )
        assert sum(results) == 1


@mark.parametrize('prefix', ['BEGIN', 'INSERT INTO', 'COMMIT'])
async def test_bulk_cancellation_finishes_worker_and_rolls_back(tmp_path, prefix):
    schema = simple_schema()
    model = generated(schema)
    async with await Database.create(tmp_path / 'cancel.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        async with pause_sql(db.connection, prefix) as (entered, release):
            task = create_task(client.counts.create_many(model.CountCreate(value=i) for i in range(2500)))
            try:
                await wait_for(entered.wait(), 2)
                task.cancel()
                await sleep(0)
                task.cancel()
                assert not task.done()
            finally:
                release.set()
            with raises(CancelledError):
                await task
        assert await client.counts.query().count() == (2500 if prefix == 'COMMIT' else 0)
        assert not db.connection.in_transaction


async def test_reader_execution_cancellation_and_shutdown(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    db = await Database.create(tmp_path / 'readers.db', readers=1, wal=True)
    await apply(db, (await diff((), schema, 'initial'),))
    client = model.Client(db)
    reader = db._readers[0]
    try:
        async with pause_sql(reader, 'SELECT') as (entered, release):
            task = create_task(client.counts.query().all())
            try:
                await wait_for(entered.wait(), 2)
                task.cancel()
                await sleep(0)
                task.cancel()
                assert not task.done()
            finally:
                release.set()
            with raises(CancelledError):
                await task
        acquired, finish = Event(), Event()

        async def hold():
            async with db.read_connection() as connection:
                assert db.connection is connection
                with raises(OperationalError, match='readonly'):
                    await connection.execute_fetchall('INSERT INTO counts DEFAULT VALUES')
                acquired.set()
                await finish.wait()

        holder = create_task(hold())
        await acquired.wait()
        closing = create_task(db.close())
        await sleep(0)
        closing.cancel()
        await sleep(0)
        assert not closing.done()
        with raises(SqrrlError, match='closing'):
            await client.counts.query().all()
        finish.set()
        await holder
        with raises(CancelledError):
            await closing
        assert not db._read_owners
        for connection in [reader, db._connection]:
            with raises(ValueError, match='no active connection|Connection closed'):
                await connection.execute_fetchall('SELECT 1')
        await gather(db.close(), db.close())
    finally:
        await db.close()


async def test_self_relationships_and_eager_chunk_boundary(tmp_path):
    table = Table(
        'nodes',
        'Node',
        (integer('id').primary_key(), integer('parent').nullable().references('nodes', 'id')),
        relationships=(
            Relationship('children', 'nodes', ('id',), ('parent',), many=True),
            Relationship('ancestor', 'nodes', ('parent',), ('id',)),
        ),
    )
    schema = Schema((table,))
    model = generated(schema)
    async with await Database.create(tmp_path / 'self.db', readers=1) as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        await client.nodes.create_many(model.NodeCreate(id=i) for i in range(1, 11))
        await client.nodes.create(parent=1)
        await db._readers[0]._execute(db._readers[0]._conn.setlimit, SQLITE_LIMIT_VARIABLE_NUMBER, 3)
        statements = []
        await db._readers[0].set_trace_callback(statements.append)
        rows = await client.nodes.query().order_by(model.NodeColumns.id.asc()).load(model.NodeRelations.children).all()
        assert len(rows[0].children) == 1 and rows[1].children == []
        assert len([sql for sql in statements if sql.startswith('SELECT')]) == 5
        assert (
            await client.nodes.query().where(model.NodeRelations.children.exists(model.NodeColumns.id.ge(10))).count()
            == 1
        )
        assert await client.nodes.query().where(model.NodeRelations.ancestor.exists()).count() == 1


@mark.parametrize(
    'replacement',
    [
        ('NOT NULL', ''),
        ('DEFAULT 0', 'DEFAULT 1'),
        ('value >= 0', 'value > 0'),
        (' STRICT', ''),
        ('INTEGER', 'REAL'),
        ('PRIMARY KEY', 'PRIMARY KEY AUTOINCREMENT'),
        ('"value" INTEGER', '"value" INTEGER COLLATE NOCASE'),
    ],
)
async def test_adoption_rejects_constraint_default_and_storage_differences(tmp_path, replacement):
    table = Table(
        'counts',
        'Count',
        (integer('id').primary_key(), integer('value').default('0')),
        checks=(Check('positive', 'value >= 0'),),
    )
    sql = table.create_sql().replace(*replacement, 1)
    async with await Database.create(tmp_path / 'reject.db') as db:
        await db.connection.execute_fetchall(sql)
        with raises(MigrationError):
            await adopt(db, Schema((table,)))
        assert not await db.connection.execute_fetchall("SELECT 1 FROM sqlite_schema WHERE name='sqrrl_migrations'")


async def test_adoption_composite_foreign_keys_checks_indexes_and_checksum(tmp_path):
    parent = Table('parents', 'Parent', (text('code'), integer('edition')), primary_key=('code', 'edition'))
    child = Table(
        'children',
        'Child',
        (integer('id').primary_key(), text('code'), integer('edition')),
        indexes=(Index('child_code', ('code',), where='edition > 0'),),
        foreign_keys=(ForeignKey(('code', 'edition'), 'parents', ('code', 'edition'), on_delete='CASCADE'),),
    )
    schema = Schema((parent, child))
    async with await Database.create(tmp_path / 'adopt.db') as db:
        for table in schema.tables:
            await db.connection.execute_fetchall(table.create_sql().replace('CREATE TABLE', 'create table'))
            for sql in table.index_sql():
                await db.connection.execute_fetchall(sql)
        migration = await adopt(db, schema)
        write(tmp_path / 'history', migration)
        history = load(tmp_path / 'history')
        await baseline(db, history, 1)
        await check(history, schema)
        assert (await status(db, history))[0].applied
        with raises(MigrationError, match='checksum'):
            await status(db, (replace(migration, statements=('SELECT 1;',)),))


async def test_adoption_rejects_data_fk_violations_and_changed_source(tmp_path):
    parent = Table('parents', 'Parent', (integer('id').primary_key(),))
    child = Table('children', 'Child', (integer('id').primary_key(), integer('parent').references('parents', 'id')))
    schema = Schema((parent, child))
    async with await Database.create(tmp_path / 'foreign.db') as db:
        for table in schema.tables:
            await db.connection.execute_fetchall(table.create_sql())
        await db.connection.execute_fetchall('PRAGMA foreign_keys=OFF')
        await db.connection.execute_fetchall('INSERT INTO children VALUES (1, 99)')
        await db.connection.execute_fetchall('PRAGMA foreign_keys=ON')
        with raises(MigrationError, match='foreign key'):
            await adopt(db, schema)
        await db.connection.execute_fetchall('DELETE FROM children')
        migration = await adopt(db, schema)
        await db.connection.execute_fetchall('CREATE INDEX new_index ON children(parent)')
        with raises(MigrationError, match='exact schema'):
            await baseline(db, (migration,), 1)


async def test_metadata_only_changes_and_representation_guard():
    schema = Schema((Table('values_table', 'Value', (integer('id').primary_key(), text('data'))),))
    first = await diff((), schema, 'initial')
    changed = Schema((replace(schema.tables[0], fields=(schema.tables[0].fields[0], json('data'))),))
    with raises(MigrationError, match='representation'):
        await diff((first,), changed, 'change_type')
    factory = Schema(
        (replace(schema.tables[0], fields=(schema.tables[0].fields[0], text('data').default_factory('builtins:str'))),)
    )
    second = await diff((first,), factory, 'factory')
    assert second.statements == ('SELECT 1;',)
    await check((first, second), factory)
    assert schema.to_dict() == Schema.from_dict(schema.to_dict()).to_dict()


def test_invalid_relationship_and_factory_metadata():
    with raises(SchemaError, match='reference'):
        Schema(
            (Table('items', 'Item', (integer('id').primary_key(), text('title').default_factory('lambda: 1'))),)
        ).normalize()
    with raises(SchemaError, match='Immutable'):
        Schema((Table('items', 'Item', (integer('id').primary_key().on_update('builtins:int'),)),)).normalize()
    with raises(SchemaError, match='foreign key'):
        Schema(
            (
                Table(
                    'items',
                    'Item',
                    (integer('id').primary_key(),),
                    relationships=(Relationship('other', 'items', ('id',), ('id',)),),
                ),
            )
        ).normalize()


async def test_cancelled_reader_open_closes_every_started_connection(tmp_path, monkeypatch):
    loop = get_running_loop()
    entered, release = Event(), ThreadEvent()
    connections = []

    def opening(*args, **kwargs):
        if not connections:
            connection = connect(*args, **kwargs)
        else:

            def connector():
                result = sqlite_connect(':memory:', isolation_level=None)
                loop.call_soon_threadsafe(entered.set)
                release.wait(5)

                return result

            connection = Connection(connector, 64)
        connections.append(connection)

        return connection

    monkeypatch.setattr('sqrrl.runtime.connect', opening)
    task = create_task(Database.create(tmp_path / 'opening.db', readers=2))
    try:
        await wait_for(entered.wait(), 2)
        task.cancel()
        await sleep(0)
        assert not task.done()
    finally:
        release.set()
    with raises(CancelledError):
        await task
    assert len(connections) == 2
    for connection in connections:
        with raises(ValueError, match='closed|no active connection'):
            await connection.execute_fetchall('SELECT 1')


async def test_readers_overlap_and_writes_remain_available(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    async with await Database.create(tmp_path / 'parallel.db', readers=2, wal=True, timeout=0.25) as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        reader = db._available[-1]
        async with pause_sql(reader, 'SELECT') as (entered, release):
            paused = create_task(client.counts.query().all())
            try:
                await wait_for(entered.wait(), 2)
                assert await wait_for(client.counts.query().count(), 2) == 0
                assert (await wait_for(client.counts.create(value=4), 2)).value == 4
            finally:
                release.set()
            await paused
        for connection in db._readers:
            assert (await connection.execute_fetchall('PRAGMA busy_timeout'))[0][0] == 250


async def test_bulk_validation_and_decoder_failures_rollback_earlier_chunks(tmp_path):
    table = Table('objects', 'Object', (integer('id').primary_key(), json('data').default("'invalid json'")))
    schema = Schema((table,))
    model = generated(schema)
    async with await Database.create(tmp_path / 'decode.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        with raises(ValidationError, match='JSON'):
            await client.objects.create_many(
                [model.ObjectCreate(data={'valid': True}) for _ in range(1000)] + [model.ObjectCreate()]
            )
        assert await client.objects.query().count() == 0
        with raises(ValidationError):
            await client.objects.create_many(
                [model.ObjectCreate(data=[]) for _ in range(1000)] + [model.ObjectCreate(data=object())]
            )
        assert await client.objects.query().count() == 0


def test_adoption_cli_preserves_external_history(tmp_path):
    invoke(tmp_path, 'init')
    (tmp_path / 'schema.py').write_text(
        'from sqrrl.schema import Schema, Table, integer, text\n'
        "schema = Schema((Table('notes', 'Note', (integer('id').primary_key(), text('title'))),))\n",
        encoding='utf-8',
    )
    preserved = (
        'CREATE TABLE old_history (version TEXT PRIMARY KEY); CREATE INDEX old_versions ON old_history(version);'
    )
    (tmp_path / 'preserved.sql').write_text(preserved, encoding='utf-8')
    with sqlite_connect(tmp_path / 'external.db') as connection:
        connection.executescript(
            'create table notes (id integer primary key not null, title text not null) strict;'
            + preserved
            + "INSERT INTO notes VALUES (1, 'keep'); INSERT INTO old_history VALUES ('v1');"
        )
    invoke(tmp_path, 'migrate', 'adopt', '--db', 'external.db', '--preserve-sql', 'preserved.sql')
    with sqlite_connect(tmp_path / 'external.db') as connection:
        assert not connection.execute("SELECT 1 FROM sqlite_schema WHERE name='sqrrl_migrations'").fetchall()
    invoke(tmp_path, 'migrate', 'baseline', '--db', 'external.db', '--version', '1')
    invoke(tmp_path, 'migrate', 'check')
    assert 'applied' in invoke(tmp_path, 'migrate', 'status', '--db', 'external.db').stdout
    with sqlite_connect(tmp_path / 'external.db') as connection:
        assert connection.execute('SELECT * FROM old_history').fetchall() == [('v1',)]
