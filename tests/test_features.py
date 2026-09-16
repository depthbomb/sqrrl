from os import environ
from re import findall
from pathlib import Path
from subprocess import run
from json import dumps, loads
from dataclasses import replace
from sqrrl.generate import write
from sys import executable, prefix
from pytest import fixture, raises, mark
from examples.library_types import Binding, Label
from sqrrl.schema import Schema, Table, integer, text
from datetime import UTC, datetime, timedelta, timezone
from examples.library_schema import schema as library_schema
from sqlite3 import IntegrityError, SQLITE_LIMIT_VARIABLE_NUMBER
from sqrrl.migrate import apply, baseline, check, diff, adopt, status
from asyncio import CancelledError, Event, create_task, gather, sleep, wait_for
from sqrrl import UNLOADED, Conflict, Database, Increment, MigrationError, SqrrlError, ValidationError


@fixture
def schema():
    return library_schema


@fixture
async def db(tmp_path, schema):
    async with await Database.create(tmp_path / 'library.db', wal=True, readers=2, immediate=True) as database:
        migration = await diff((), schema, 'initial')
        await apply(database, (migration,))
        yield database


async def test_bulk_conflicts_limits_and_atomicity(db, models):
    client = models.Client(db)
    statements = []
    await db.connection.set_trace_callback(statements.append)
    await db.connection._execute(db.connection._conn.setlimit, SQLITE_LIMIT_VARIABLE_NUMBER, 30)
    rows = await client.books.create_many(models.BookCreate(isbn=str(i), title=f'Title {i}') for i in range(23))
    assert len(rows) == 23
    inserts = [s for s in statements if s.startswith('INSERT')]
    assert 1 < len(inserts) < 10
    assert len([s for s in statements if s == 'BEGIN IMMEDIATE']) == 1
    conflict = Conflict((models.BookColumns.isbn,))
    assert await client.books.insert(models.BookCreate(isbn='0', title='skip'), conflict=conflict) is None
    conflict = Conflict((models.BookColumns.isbn,), (models.BookColumns.title, models.BookColumns.copies))
    updated = await client.books.insert(models.BookCreate(isbn='0', title='Updated'), conflict=conflict)
    assert updated.title == 'Updated' and updated.copies == 0
    assert (
        len(
            await client.books.create_many(
                [models.BookCreate(isbn='0', title='twice'), models.BookCreate(isbn='new', title='New')],
                conflict=conflict,
            )
        )
        == 2
    )
    before = await client.books.query().count()
    with raises(IntegrityError):
        await client.books.create_many(
            [models.BookCreate(isbn=f'rollback{i}', title='new') for i in range(20)]
            + [models.BookCreate(isbn='0', title='duplicate')]
        )
    assert await client.books.query().count() == before
    with raises(ValidationError, match='mutable'):
        await client.books.insert(
            models.BookCreate(isbn='0', title='x'),
            conflict=Conflict((models.BookColumns.isbn,), (models.BookColumns.isbn,)),
        )
    with raises(ValidationError, match='unique key'):
        await client.books.insert(
            models.BookCreate(isbn='0', title='x'), conflict=Conflict((models.BookColumns.title,))
        )
    shelves = await client.shelves.create_many([models.ShelfCreate(room='A', number=1, name='One')])
    assert len(shelves) == 1
    row = await client.shelves.insert(
        models.ShelfCreate(room='A', number=1, name='Changed'),
        conflict=Conflict((models.ShelfColumns.room, models.ShelfColumns.number), (models.ShelfColumns.name,)),
    )
    assert row.name == 'Changed'


async def test_predicates_and_conditional_races(db, models):
    client = models.Client(db)
    c = models.BookColumns
    book = await client.books.create(isbn='a', title='100%_! literal', copies=0)
    assert await client.books.query().where(c.title.contains('%_!')).count() == 1
    assert await client.books.query().where(c.title.contains('_no')).count() == 0
    assert await client.books.query().where(c.copies.ge(0) & c.copies.le(0)).count() == 1
    results = await gather(
        *(client.books.update_where(c.id.eq(book.id) & c.copies.eq(0), copies=Increment(1)) for _ in range(12))
    )
    assert sum(results) == 1
    await gather(*(client.books.update_where(c.id.eq(book.id), copies=Increment(1)) for _ in range(12)))
    assert (await client.books.get(book.id)).copies == 13
    assert await client.books.update_where(c.id.eq(book.id), room=None) == 1
    assert await client.books.query().where(c.room.eq(None)).count() == 1
    with raises(ValidationError, match='NULL'):
        c.room.ge(None)
    with raises(ValidationError, match='explicitly'):
        await client.books.update_where(title='oops')
    with raises(ValidationError, match='explicitly'):
        await client.books.delete_where()
    assert await client.books.update_where(_all_rows=True, title='all') == 1
    assert await client.books.delete_where(c.copies.lt(0)) == 0
    assert await client.books.delete_where(all_rows=True) == 1


async def test_relationships_are_batched_and_preserve_pagination(db, models):
    client = models.Client(db)
    await client.shelves.create_many(models.ShelfCreate(room='A', number=i, name=str(i)) for i in range(12))
    await client.books.create_many(
        models.BookCreate(isbn=str(i), title=str(i), room='A', shelf=i // 2) for i in range(20)
    )
    await client.books.create(isbn='missing', title='No shelf')
    await client.signs.create(room='A', shelf=4, caption='Four')
    assert (await client.shelves.get(models.ShelfKey(room='A', number=4))).books is UNLOADED
    statements = []
    for connection in db._readers:
        await connection.set_trace_callback(statements.append)
    parents = (
        await client.shelves.query()
        .order_by(models.ShelfColumns.number.desc())
        .offset(6)
        .limit(3)
        .load(models.ShelfRelations.books, models.ShelfRelations.sign)
        .all()
    )
    assert [p.number for p in parents] == [5, 4, 3]
    assert [len(p.books) for p in parents] == [2, 2, 2]
    assert parents[0].sign is None and parents[1].sign.caption == 'Four'
    assert len([s for s in statements if s.startswith('SELECT')]) == 3
    rows = await client.books.query().load(models.BookRelations.location).all()
    assert rows[-1].location is None
    assert rows[0].location.number == 0
    assert (
        await client.shelves.query().where(models.ShelfRelations.books.exists(models.BookColumns.title.eq('3'))).count()
        == 1
    )
    assert await client.shelves.query().where(~models.ShelfRelations.books.exists()).count() == 2
    assert (
        await client.books.query().where(models.BookRelations.location.exists(models.ShelfColumns.number.ge(5))).count()
        == 10
    )
    assert (
        await client.books.update_where(
            models.BookRelations.location.exists(models.ShelfColumns.number.eq(0)), copies=Increment(2)
        )
        == 2
    )


async def test_codecs_and_defaults(db, models):
    client = models.Client(db)
    instant = datetime(2024, 2, 3, 4, 5, 6, 123456, tzinfo=timezone(timedelta(hours=2)))
    book = await client.books.create(
        isbn='codec',
        title='Typed',
        created=instant,
        binding=Binding.CLOTH,
        metadata={'a': [True, None, 1.5]},
        label=Label('hello'),
    )
    assert book.created == instant.astimezone(UTC)
    assert book.created.tzinfo is UTC
    assert book.binding is Binding.CLOTH and book.label == Label('hello')
    assert book.metadata == {'a': [True, None, 1.5]}
    assert book.edited is None
    changed = await client.books.update(book.id, title='Changed')
    assert changed.edited is not None
    assert (await client.books.update(book.id, edited=None)).edited is None
    assert (await client.books.update(book.id, edited=instant)).edited == instant
    for field, value in [
        ('created', datetime.now()),
        ('metadata', {'a': float('nan')}),
        ('metadata', {1: 'bad'}),
        ('metadata', (1, 2)),
        ('binding', 'CLOTH'),
        ('label', 'bad'),
        ('metadata', None),
    ]:
        with raises(ValidationError):
            await client.books.create(isbn='bad', title='bad', **{field: value})
    async with db.transaction():
        cursor = await db.connection.execute(
            'SELECT created, binding, metadata, label FROM books WHERE id=?', (book.id,)
        )
        stored = await cursor.fetchone()
        await cursor.close()
    assert stored[0] == '2024-02-03T02:05:06.123456+00:00'
    assert stored[1] == 'CLOTH' and stored[2] == '{"a":[true,null,1.5]}' and stored[3] == b'hello'


async def test_reader_snapshot_transaction_pinning_and_cancel(db, models):
    client = models.Client(db)
    await client.books.create(isbn='one', title='One')
    async with db.read_connection():
        assert await client.books.query().count() == 1
        task = create_task(client.books.create(isbn='two', title='Two'))
        await wait_for(task, 2)
        assert await client.books.query().count() == 1
        with raises(SqrrlError, match='reader'):
            await client.books.create(isbn='oops', title='oops')
    assert await client.books.query().count() == 2
    async with db.transaction():
        await client.books.create(isbn='three', title='Three')
        assert await client.books.query().count() == 3
        assert await wait_for(create_task(client.books.query().count()), 2) == 2
    acquired = [Event(), Event()]
    release = Event()

    async def hold(index):
        async with db.read_connection():
            acquired[index].set()
            await release.wait()

    holders = [create_task(hold(i)) for i in range(2)]
    await gather(*(event.wait() for event in acquired))
    queued = create_task(client.books.query().all())
    await sleep(0)
    queued.cancel()
    with raises(CancelledError):
        await queued
    holders[0].cancel()
    with raises(CancelledError):
        await holders[0]
    assert await wait_for(client.books.query().count(), 2) == 3
    release.set()
    await holders[1]
    assert len(db._available) == 2 and not db._read_owners


async def test_adoption_roundtrip_and_strict_drift(tmp_path):
    schema = Schema((Table('notes', 'Note', (integer('id').primary_key(), text('title').default("'untitled'"))),))
    preserved = ('CREATE TABLE tool_history (version TEXT PRIMARY KEY, checksum TEXT NOT NULL)',)
    async with await Database.create(tmp_path / 'external.db') as db:
        await db.connection.execute_fetchall(
            "create table notes (id integer primary key not null, title text not null default 'untitled') strict"
        )
        await db.connection.execute_fetchall(preserved[0])
        await db.connection.execute_fetchall("INSERT INTO notes VALUES (1, 'keep')")
        await db.connection.execute_fetchall("INSERT INTO tool_history VALUES ('old', 'keep')")
        migration = await adopt(db, schema, preserve_sql=preserved)
        await baseline(db, (migration,), 1)
        assert (await status(db, (migration,)))[0].applied
        await check((migration,), schema)
        changed = Schema((replace(schema.tables[0], fields=schema.tables[0].fields + (text('extra').nullable(),)),))
        next_migration = await diff((migration,), changed, 'add_extra')
        await apply(db, (migration, next_migration))
        assert tuple((await db.connection.execute_fetchall('SELECT * FROM notes'))[0]) == (1, 'keep', None)
        assert tuple((await db.connection.execute_fetchall('SELECT * FROM tool_history'))[0]) == ('old', 'keep')
        await db.connection.execute_fetchall('CREATE INDEX surprise ON notes(title)')
        with raises(MigrationError, match='drift'):
            await status(db, (migration, next_migration))


@mark.parametrize(
    'extra',
    [
        'CREATE INDEX surprise ON notes(title)',
        'CREATE TRIGGER surprise AFTER INSERT ON notes BEGIN SELECT 1; END',
        'CREATE VIEW surprise AS SELECT * FROM notes',
    ],
)
async def test_adoption_rejects_unverified_objects(tmp_path, extra):
    schema = Schema((Table('notes', 'Note', (integer('id').primary_key(), text('title'))),))
    async with await Database.create(tmp_path / 'external.db') as db:
        await db.connection.execute_fetchall(schema.tables[0].create_sql())
        await db.connection.execute_fetchall(extra)
        with raises(MigrationError, match='objects'):
            await adopt(db, schema)
        assert not await db.connection.execute_fetchall("SELECT 1 FROM sqlite_schema WHERE name='sqrrl_migrations'")


@mark.parametrize('checker', ['mypy', 'pyright'])
def test_feature_typing(tmp_path, schema, checker):
    write(tmp_path / 'models.py', schema)
    root = Path(__file__).resolve().parents[1]
    config = {
        'pythonVersion': '3.14',
        'typeCheckingMode': 'strict',
        'extraPaths': [str(root)],
        'venvPath': str(Path(prefix).parent),
        'venv': Path(prefix).name,
    }
    (tmp_path / 'pyrightconfig.json').write_text(dumps(config), encoding='utf-8')
    consumer = tmp_path / 'consumer.py'
    consumer.write_text(
        """from typing import assert_type
from datetime import datetime
from sqrrl.runtime import LoadPath, Query
from sqrrl import Conflict, Database, Increment, Unloaded
from models import Book, BookCreate, BookColumns, BookRelations, Shelf, Sign, ShelfRelations, Client
from examples.library_types import Binding, Label

async def valid(db: Database) -> None:
    client = Client(db)
    rows = await client.books.create_many([BookCreate(isbn='a', title='A', binding=Binding.PAPER, label=Label('x'))], conflict=Conflict((BookColumns.isbn,), (BookColumns.title,)))
    assert_type(rows, list[Book])
    assert_type(rows[0].created, datetime)
    assert_type(rows[0].binding, Binding)
    assert_type(await client.books.update_where(BookColumns.copies.ge(0), copies=Increment(1)), int)
    books = await client.books.query().load(BookRelations.location).all()
    if not isinstance(books[0].location, Unloaded) and books[0].location is not None:
        assert_type(books[0].location.room, str)
    path = BookRelations.location.then(ShelfRelations.sign)
    assert_type(path, LoadPath[Book, Sign])
    assert_type(client.books.query().load(path), Query[Book])
    nested = await client.books.query().load(path, BookRelations.location).all()
    assert_type(nested, list[Book])
    if not isinstance(nested[0].location, Unloaded) and nested[0].location is not None:
        assert_type(nested[0].location, Shelf)
        if not isinstance(nested[0].location.sign, Unloaded) and nested[0].location.sign is not None:
            assert_type(nested[0].location.sign, Sign)
    cycle = ShelfRelations.books.then(BookRelations.location).then(ShelfRelations.books)
    assert_type(cycle, LoadPath[Shelf, Book])
    shelves = await client.shelves.query().load(cycle).all()
    if not isinstance(shelves[0].books, Unloaded):
        assert_type(shelves[0].books, list[Book])
    await client.shelves.query().where(ShelfRelations.books.exists(BookColumns.title.contains('%'))).all()
""",
        encoding='utf-8',
    )
    command = [executable, '-m', checker] + (['--strict', '--no-incremental'] if checker == 'mypy' else ['--outputjson'])
    environment = dict(environ) | {'MYPYPATH': str(root)}
    collisions = Schema(
        schema.tables
        + tuple(
            Table(
                'legacy_' + name.lower(),
                name,
                (
                    integer('id').primary_key(),
                    text('Unset').nullable(),
                    text('list').nullable(),
                    text('Unloaded').nullable(),
                ),
            )
            for name in (
                'Conflict',
                'Increment',
                'Iterable',
                'datetime',
                'JsonValue',
                'Related',
                'Unloaded',
                'BookCreate',
                'ShelfRelations',
            )
        )
    )
    collision_path = write(tmp_path / 'collisions.py', collisions)
    result = run(
        command + [str(consumer), str(collision_path)], cwd=tmp_path, env=environment, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
    consumer.write_text(
        consumer.read_text()
        + """
async def invalid(db: Database) -> None:
    client = Client(db)
    await client.books.create_many([BookCreate(isbn='x', title=123)])
    await client.books.update_where(BookColumns.id.eq(1), isbn='changed')
    await client.books.update_where(BookColumns.id.eq(1), copies=Increment('bad'))
    client.books.query().load(ShelfRelations.books)
    ShelfRelations.books.exists(BookColumns.copies.contains('bad'))
    BookCreate(isbn='x', title='x', binding='PAPER')
    BookRelations.location.then(BookRelations.location)  # reject
    BookRelations.location.then(ShelfRelations.books).then(ShelfRelations.sign)  # reject
    client.books.query().load(ShelfRelations.books.then(BookRelations.location))  # reject
    BookRelations.location.then(BookColumns.title)  # reject
    client.books.query().load('location.sign')  # reject
""",
        encoding='utf-8',
    )
    result = run(command + [str(consumer)], cwd=tmp_path, env=environment, capture_output=True, text=True)
    assert result.returncode != 0
    expected = {number for number, line in enumerate(consumer.read_text().splitlines(), 1) if '# reject' in line}
    if checker == 'mypy':
        rejected = {int(number) for number in findall(r'consumer\.py:(\d+): error:', result.stdout)}
    else:
        rejected = {
            item['range']['start']['line'] + 1
            for item in loads(result.stdout)['generalDiagnostics']
            if item['severity'] == 'error'
        }
    assert expected <= rejected and len(rejected) >= 11, result.stdout
