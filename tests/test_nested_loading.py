from math import ceil
from dataclasses import replace
from test_async import pause_sql
from contextlib import nullcontext
from sqrrl.migrate import apply, diff
from test_regressions import generated
from pytest import fixture, mark, raises
from sqrrl.runtime import LoadPath, _fetch_all
from sqlite3 import SQLITE_LIMIT_VARIABLE_NUMBER
from sqrrl import UNLOADED, Database, ValidationError
from asyncio import CancelledError, create_task, wait_for
from examples.library_schema import schema as library_schema
from sqrrl.schema import Relationship, Schema, Table, integer


@fixture
def schema():
    return library_schema


@fixture
async def db(tmp_path, schema):
    async with await Database.create(tmp_path / 'nested.db', wal=True, readers=2) as database:
        await apply(database, (await diff((), schema, 'initial'),))
        yield database


@mark.parametrize('parameter_limit', [3, 100])
async def test_nested_branches_merge_and_batch_after_parent_pagination(db, models, parameter_limit):
    client = models.Client(db)
    locations = [('A', 1), ('B', 1), ('A', 2), ('B', 2)]
    for room, number in locations:
        await client.shelves.create(room=room, number=number, name=f'{room}{number}')
        await client.books.create_many(
            models.BookCreate(isbn=f'{room}{number}-{i}', title=str(i), room=room, shelf=number) for i in range(3)
        )
    await client.books.create(isbn='partial', title='Partial key', room='A')
    await client.books.create(isbn='absent', title='No key')
    for number in (1, 2):
        await client.signs.create(room='B', shelf=number, caption=f'B{number}')

    statements = []
    for connection in db._readers:
        await connection._execute(connection._conn.setlimit, SQLITE_LIMIT_VARIABLE_NUMBER, parameter_limit)
        await connection.set_trace_callback(statements.append)

    location = models.BookRelations.location
    base = client.books.query().order_by(models.BookColumns.id.desc()).offset(1).limit(8)
    query = base.load(location.then(models.ShelfRelations.sign), location.then(models.ShelfRelations.books))
    rows = await query.load(location, location.then(models.ShelfRelations.sign)).all()
    assert base.loading == () and len(query.loading) == 2
    assert [row.id for row in rows] == list(range(13, 5, -1))
    assert rows[0].location is None
    for row in rows[1:]:
        shelf = row.location
        assert (shelf.room, shelf.number) == (row.room, row.shelf)
        assert shelf.sign.caption == shelf.name if shelf.room == 'B' else shelf.sign is None
        assert [book.id for book in shelf.books] == sorted(book.id for book in shelf.books)
        assert len(shelf.books) == 3
        assert all((book.room, book.shelf) == (row.room, row.shelf) for book in shelf.books)
        assert all(book.location is UNLOADED for book in shelf.books)

    # Three distinct composite keys at each edge, regardless of parent count.
    assert len([sql for sql in statements if sql.startswith('SELECT')]) == 1 + 3 * ceil(3 / (parameter_limit // 2))
    assert statements.count('BEGIN') == statements.count('ROLLBACK') == 1
    assert all(not connection.in_transaction for connection in db._readers)


async def test_nested_only_first_empty_and_unselected_relationships(db, models):
    client = models.Client(db)
    await client.shelves.create(room='A', number=1, name='One')
    book = await client.books.create(isbn='one', title='One', room='A', shelf=1)
    await client.books.create(isbn='absent', title='No key')
    path = models.BookRelations.location.then(models.ShelfRelations.sign)
    query = client.books.query().where(models.BookColumns.id.eq(book.id)).load(path)
    row = await query.only()
    assert row.location.sign is None and row.location.books is UNLOADED
    assert await query.first() == row
    assert (await client.books.get(book.id)).location is UNLOADED
    assert (await client.books.query().order_by(models.BookColumns.id.desc()).load(path).first()).location is None

    statements = []
    for connection in db._readers:
        await connection.set_trace_callback(statements.append)
    assert await query.limit(0).all() == []
    assert await query.limit(0).first() is None
    assert await query.where(models.BookColumns.id.eq(-1)).all() == []
    assert len([sql for sql in statements if sql.startswith('SELECT')]) == 3

    # No target keys means neither this edge nor its descendants need a query.
    statements.clear()
    assert (await client.books.query().where(models.BookColumns.isbn.eq('absent')).load(path).only()).location is None
    assert len([sql for sql in statements if sql.startswith('SELECT')]) == 1


async def test_nested_collections_and_finite_self_paths(tmp_path):
    schema = Schema(
        (
            Table(
                'nodes',
                'Node',
                (integer('id').primary_key(), integer('parent').nullable().references('nodes', 'id')),
                relationships=(
                    Relationship('children', 'nodes', ('id',), ('parent',), many=True),
                    Relationship('ancestor', 'nodes', ('parent',), ('id',)),
                ),
            ),
        )
    )
    model = generated(schema)
    async with await Database.create(tmp_path / 'self.db') as db:
        await apply(db, (await diff((), schema, 'initial'),))
        client = model.Client(db)
        await client.nodes.create(id=1)
        await client.nodes.create_many(model.NodeCreate(id=i, parent=1) for i in (2, 3))
        await client.nodes.create_many(model.NodeCreate(id=i, parent=2) for i in (4, 5))
        await client.nodes.create(id=6, parent=3)
        await client.nodes.update(1, parent=1)
        children, ancestor = model.NodeRelations.children, model.NodeRelations.ancestor
        statements = []
        await db.connection.set_trace_callback(statements.append)
        row = await client.nodes.query().where(model.NodeColumns.id.eq(1)).load(children.then(children)).only()
        assert [child.id for child in row.children] == [1, 2, 3]
        assert [[leaf.id for leaf in child.children] for child in row.children] == [[1, 2, 3], [4, 5], [6]]
        assert row.ancestor is UNLOADED
        assert all(child.ancestor is UNLOADED for child in row.children)
        assert all(leaf.children is UNLOADED for child in row.children for leaf in child.children)
        assert len([sql for sql in statements if sql.startswith('SELECT')]) == 3

        statements.clear()
        row = (
            await client.nodes.query()
            .where(model.NodeColumns.id.eq(1))
            .load(ancestor.then(ancestor).then(ancestor))
            .only()
        )
        assert row.ancestor.ancestor.ancestor.id == 1
        assert row.ancestor.ancestor.ancestor.ancestor is UNLOADED
        assert len([sql for sql in statements if sql.startswith('SELECT')]) == 4


def test_invalid_paths_fail_before_execution(db, models):
    location, books, sign = models.BookRelations.location, models.ShelfRelations.books, models.ShelfRelations.sign
    query = models.Client(db).books.query()
    with raises(ValidationError, match='another model'):
        location.then(location)
    with raises(ValidationError, match='another model'):
        location.then(books).then(sign)
    with raises(ValidationError, match='another model'):
        query.load(books.then(location))
    with raises(ValidationError, match='relationship'):
        query.load('location.sign')
    with raises(ValidationError, match='relationship'):
        location.then(models.ShelfColumns.name)
    with raises(ValidationError, match='relationship'):
        location.then(books).then('location')
    with raises(ValidationError, match='requires relationships'):
        LoadPath(models.Book, models.Shelf, ())
    with raises(ValidationError, match='another model'):
        replace(location.then(sign), related_model=models.Book)
    assert query.loading == ()


@mark.parametrize('readers', [0, 2])
@mark.parametrize('scope', ['query', 'read', 'transaction'])
@mark.parametrize('pause_after', ['books', 'shelves'])
async def test_nested_loading_uses_one_snapshot(tmp_path, schema, models, monkeypatch, readers, scope, pause_after):
    path = tmp_path / 'snapshot.db'
    async with await Database.create(path, wal=True, readers=readers) as db, await Database.open(path) as writer:
        await apply(db, (await diff((), schema, 'initial'),))
        client, other = models.Client(db), models.Client(writer)
        await client.shelves.create(room='A', number=1, name='Before')
        book = await client.books.create(isbn='one', title='Before', room='A', shelf=1)
        sign = await client.signs.create(room='A', shelf=1, caption='Before')
        connections = []
        updated = False

        async def fetch(connection, sql, arguments=()):
            nonlocal updated
            result = await _fetch_all(connection, sql, arguments)
            if connection is not writer.connection and sql.startswith('SELECT'):
                connections.append(connection)
                if not updated and f'FROM main."{pause_after}"' in sql:
                    updated = True
                    async with writer.transaction():
                        await other.books.update(book.id, title='After')
                        await other.shelves.update(models.ShelfKey(room='A', number=1), name='After')
                        await other.signs.update(sign.id, caption='After')

            return result

        monkeypatch.setattr('sqrrl.runtime._fetch_all', fetch)
        context = (
            db.read_connection() if scope == 'read' else db.transaction() if scope == 'transaction' else nullcontext()
        )
        async with context:
            row = await client.books.query().load(models.BookRelations.location.then(models.ShelfRelations.sign)).only()
            assert updated
            assert (row.title, row.location.name, row.location.sign.caption) == ('Before', 'Before', 'Before')
            assert len(connections) == 3 and all(connection is connections[0] for connection in connections)
            if scope != 'query':
                assert db.connection.in_transaction
        assert all(not connection.in_transaction for connection in db._readers + [db.connection])
        row = await client.books.query().load(models.BookRelations.location.then(models.ShelfRelations.sign)).only()
        assert (row.title, row.location.name, row.location.sign.caption) == ('After', 'After', 'After')


@mark.parametrize('failure', ['decode', 'cancel', 'parameter_limit'])
async def test_nested_failure_releases_snapshot_and_reader(db, models, failure):
    client = models.Client(db)
    await client.shelves.create(room='A', number=1, name='One')
    await client.books.create(isbn='one', title='One', room='A', shelf=1)
    await client.signs.create(room='A', shelf=1, caption='One')
    sign = models.ShelfRelations.sign
    reader = db._available[-1]

    if failure == 'decode':

        def fail_decode(row):
            raise ValueError('Invalid sign')

        sign = replace(sign, decoder=fail_decode)

    if failure == 'parameter_limit':
        await reader._execute(reader._conn.setlimit, SQLITE_LIMIT_VARIABLE_NUMBER, 1)

    query = client.books.query().load(models.BookRelations.location.then(sign))
    if failure == 'cancel':
        async with pause_sql(reader, 'SELECT * FROM main."signs"') as (entered, release):
            task = create_task(query.all())
            try:
                await wait_for(entered.wait(), 2)
                task.cancel()
            finally:
                release.set()
            with raises(CancelledError):
                await task
    else:
        error, message = (ValueError, 'Invalid sign') if failure == 'decode' else (ValidationError, 'parameter limit')
        with raises(error, match=message):
            await query.all()

    assert not db._read_owners and len(db._available) == 2
    assert all(not connection.in_transaction for connection in db._readers)
    assert await client.books.query().count() == 1
