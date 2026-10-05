from pytest import raises
from dataclasses import replace
from sqrrl import Database, MigrationError
from sqrrl.migrate import apply, check, diff
from sqrrl.schema import Index, Schema, Table, integer, text

async def test_column_identity_swap_preserves_data_with_identical_ddl(tmp_path):
    table = Table('items', 'Item', (
        integer('id').primary_key(), text('left').identity('a'), text('right').identity('b'),
    ))
    original = Schema((table,))
    desired = Schema((replace(table, fields=(
        integer('id').primary_key(), text('left').identity('b'), text('right').identity('a'),
    )),))
    assert table.create_sql() == desired.tables[0].create_sql()
    initial = await diff((), original, 'initial')
    swapped = await diff((initial,), desired, 'swap')

    async with await Database.create(tmp_path / 'swap.db') as database:
        await apply(database, (initial,))
        await database.connection.execute_fetchall("INSERT INTO items VALUES (1, 'A', 'B')")
        await apply(database, (initial, swapped))
        rows = await database.connection.execute_fetchall('SELECT * FROM items')
        assert [tuple(row) for row in rows] == [(1, 'B', 'A')]

    await check((initial, swapped), desired)

async def test_replacing_column_identity_requires_allow_drop_and_uses_default(tmp_path):
    table = Table('items', 'Item', (integer('id').primary_key(), text('value').default("'new'")))
    initial = await diff((), Schema((table,)), 'initial')
    desired = Schema((replace(table, fields=(table.fields[0], table.fields[1].identity('replacement'))),))
    with raises(MigrationError, match='removes fields'):
        await diff((initial,), desired, 'replace')

    replacement = await diff((initial,), desired, 'replace', allow_drop=True)
    async with await Database.create(tmp_path / 'replace.db') as database:
        await apply(database, (initial,))
        await database.connection.execute_fetchall("INSERT INTO items VALUES (1, 'old')")
        await apply(database, (initial, replacement))
        rows = await database.connection.execute_fetchall('SELECT * FROM items')
        assert [tuple(row) for row in rows] == [(1, 'new')]

async def test_primary_key_identity_change_is_rejected_with_identical_ddl():
    table = Table('items', 'Item', (integer('id').primary_key(),))
    initial = await diff((), Schema((table,)), 'initial')
    desired = Schema((replace(table, fields=(table.fields[0].identity('replacement'),)),))
    with raises(MigrationError, match='primary key changes'):
        await diff((initial,), desired, 'replace', allow_drop=True)

async def test_table_name_swap_preserves_data_and_foreign_keys(tmp_path):
    first = Table('first', 'First', (integer('id').primary_key(), text('value')), key='a')
    second = Table('second', 'Second', first.fields, key='b', indexes=(Index('values_index', ('value',)),))
    child = Table('children', 'Child', (
        integer('id').primary_key(), integer('parent').references('first', 'id'),
    ))
    initial = await diff((), Schema((first, second, child)), 'initial')
    desired = Schema((replace(first, name='second'), replace(second, name='first'), replace(
        child, fields=(child.fields[0], integer('parent').references('second', 'id')),
    )))
    swapped = await diff((initial,), desired, 'swap')
    async with await Database.create(tmp_path / 'tables.db') as database:
        await apply(database, (initial,))
        await database.connection.execute_fetchall("INSERT INTO first VALUES (1, 'A')")
        await database.connection.execute_fetchall("INSERT INTO second VALUES (2, 'B')")
        await database.connection.execute_fetchall('INSERT INTO children VALUES (1, 1)')
        await apply(database, (initial, swapped))
        first_rows = await database.connection.execute_fetchall('SELECT * FROM first')
        second_rows = await database.connection.execute_fetchall('SELECT * FROM second')
        assert [tuple(row) for row in first_rows] == [(2, 'B')]
        assert [tuple(row) for row in second_rows] == [(1, 'A')]
        assert await database.connection.execute_fetchall('PRAGMA foreign_key_check') == []

    await check((initial, swapped), desired)
