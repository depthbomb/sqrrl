from dataclasses import replace
from sqrrl.migrate import diff, apply
from pytest import mark, raises, fixture
from sqrrl.runtime import encode, decode_blob, decode_real, decode_field, decode_boolean
from sqrrl.codecs import enum_type, decode_json, encode_json, factory_value, decode_datetime
from sqrrl import Codec, UNSET, Conflict, Database, Increment, SchemaError, NotFoundError, ValidationError
from sqrrl.schema import blob, enum, text, Check, Field, Index, Table, Schema, custom, integer, ForeignKey, Relationship


wrong_result_codec = Codec(lambda value: b'value', lambda value: 42)


@fixture
async def db(tmp_path, schema):
    async with await Database.create(tmp_path / 'validation.db') as database:
        await apply(database, (await diff((), schema, 'initial'),))
        yield database


@mark.parametrize('value', [-1, True, 1.5])
async def test_invalid_reader_counts_do_not_create_database(tmp_path, value):
    path = tmp_path / 'missing.db'
    with raises(ValueError, match='readers'):
        await Database.create(path, readers=value)
    assert not path.exists()


@mark.parametrize('value', [-1, float('nan'), float('inf'), 2**31])
async def test_invalid_sqlite_timeouts_do_not_create_database(tmp_path, value):
    path = tmp_path / 'missing.db'
    with raises(ValueError, match='timeout'):
        await Database.create(path, timeout=value)
    assert not path.exists()


async def test_nullable_operators_offset_and_empty_queries(db, models):
    client = models.Client(db)
    first = await client.users.create(name='one', email=None)
    second = await client.users.create(name='two')
    query = client.users.query()
    columns = models.UserColumns
    for predicate in (columns.email.is_null(), columns.email.eq(None), columns.email.in_(None)):
        assert await query.where(predicate).only() == first
    for predicate in (
        columns.email.is_not_null(),
        columns.email.ne(None),
        columns.id.gt(first.id),
        columns.name.ne('one'),
    ):
        assert await query.where(predicate).only() == second
    assert await query.where(columns.id.in_(first.id, second.id)).count() == 2
    assert await query.order_by(columns.id.asc()).offset(1).all() == [second]
    assert await query.where(columns.id.lt(0)).first() is None
    with raises(NotFoundError):
        await query.where(columns.id.lt(0)).only()
    assert await client.users.update(first.id) == first
    assert await client.users.update_where(columns.id.eq(first.id)) == 0


async def test_invalid_query_and_mutation_arguments_leave_rows_unchanged(db, models):
    client = models.Client(db)
    user = await client.users.create(name='unchanged')
    columns = models.UserColumns
    with raises(ValidationError, match='name is required'):
        await client.users.create(name=UNSET)
    with raises(ValidationError, match='name is required'):
        await client.users.create_many([models.UserCreate(name=UNSET)])
    for value in (-1, True, 1.5):
        for operation in (client.users.query().limit, client.users.query().offset):
            with raises(ValueError):
                operation(value)
    for column, value in ((columns.id, '1'), (columns.name, 1)):
        with raises(ValidationError, match='contains'):
            column.contains(value)
    for predicate, all_rows in ((models.TaskColumns.id.eq(1), False), (columns.id.eq(user.id), True)):
        with raises(ValidationError, match='predicate'):
            await client.users.update_where(predicate, _all_rows=all_rows, name='changed')
    for changes in ({'name': Increment(1)}, {'email': Increment(None)}):
        with raises(ValidationError, match='numeric'):
            await client.users.update_where(columns.id.eq(user.id), **changes)
    document = await client.documents.create(id='number', body=b'', score=1.0)
    with raises(ValidationError, match='NULL'):
        await client.documents.update(document.id, score=Increment(None))
    assert await client.users.get(user.id) == user
    assert await client.documents.get(document.id) == document


async def test_invalid_conflict_targets_are_rejected_before_writing(db, models):
    client = models.Client(db)
    for conflict in (
        Conflict((models.TaskColumns.id,)),
        Conflict((models.UserColumns.id,), index='missing'),
        Conflict((models.UserColumns.id,), index='active_names'),
    ):
        with raises(ValidationError):
            await client.users.create_many([models.UserCreate(name='invalid')], conflict=conflict)
    assert await client.users.query().count() == 0


@mark.parametrize(
    'decoder, value',
    [
        (decode_json, b'{}'),
        (decode_datetime, 42),
        (decode_datetime, 'not a date'),
        (decode_datetime, '2024-01-01T00:00:00+02:00'),
        (decode_boolean, 2),
        (decode_blob, 'not bytes'),
        (decode_real, True),
        (decode_real, 10**400),
    ],
)
def test_invalid_stored_values_raise_validation_errors(decoder, value):
    with raises(ValidationError):
        decoder(value)


def test_cyclic_json_and_invalid_plugin_references():
    cycle = []
    cycle.append(cycle)
    with raises(ValidationError, match='cycles'):
        encode_json(cycle)
    with raises(ValidationError, match='callable'):
        factory_value('math:pi')
    for reference in ('math:pi', 'builtins:str'):
        with raises(ValidationError, match='Enum'):
            enum_type(reference)
    with raises(ValidationError, match='Unknown stored enum'):
        decode_field(enum('binding', 'examples.library_types:Binding'), 'UNKNOWN')
    with raises(ValidationError, match='not nullable'):
        decode_field(text('title'), None)
    with raises(ValidationError, match='Invalid custom'):
        decode_field(custom('data', 'builtins:str', 'math:pi'), b'data')
    with raises(ValidationError, match='wrong Python type'):
        decode_field(custom('data', 'builtins:str', f'{__name__}:wrong_result_codec'), b'data')
    with raises(ValidationError, match='codec'):
        encode(custom('data', 'builtins:str', 'math:pi'), 'data')


@mark.parametrize(
    'changes, message',
    [
        ({'name': 'transaction'}, 'generated client'),
        ({'fields': (integer('id').primary_key(), Field('data', 'unknown'))}, 'Unsupported field'),
        ({'fields': (integer('id').primary_key(), Field('data', 'custom'))}, 'codec'),
        ({'fields': (integer('id').primary_key(), Field('data', 'enum'))}, 'Python type'),
        ({'fields': (integer('id').primary_key(), text('data').default_factory('builtins:class'))}, 'keywords'),
        ({'primary_key': ('id',)}, 'either'),
        ({'checks': (Check('empty', ' '),)}, 'empty'),
        ({'indexes': (Index('empty', ()),)}, 'distinct'),
        ({'indexes': (Index('repeated', ('id', 'id')),)}, 'distinct'),
        ({'indexes': (Index('empty_predicate', ('id',), where=' '),)}, 'empty'),
    ],
)
def test_schema_rejects_invalid_declarations(changes, message):
    table = Table('items', 'Item', (integer('id').primary_key(), text('value')))
    with raises(SchemaError, match=message):
        Schema((replace(table, **changes),)).normalize()


@mark.parametrize(
    'reference, source_kind, message',
    [
        (ForeignKey(('parent',), 'parents', ('id',), 'INVALID'), integer, 'ON DELETE'),
        (ForeignKey(('parent',), 'parents', ('value',)), integer, 'complete unique key'),
        (ForeignKey(('parent',), 'parents', ('id',)), text, 'kinds must match'),
        (ForeignKey(('parent',), 'parents', ('id',), 'SET NULL'), integer, 'nullable'),
    ],
)
def test_foreign_key_contract_is_validated(reference, source_kind, message):
    parent = Table('parents', 'Parent', (integer('id').primary_key(), integer('value')))
    child = Table('children', 'Child', (integer('id').primary_key(), source_kind('parent')), foreign_keys=(reference,))
    with raises(SchemaError, match=message):
        Schema((parent, child)).normalize()


def test_relationship_mapping_and_cardinality_are_validated():
    parent = Table('parents', 'Parent', (integer('id').primary_key(), text('name')))
    child = Table('children', 'Child', (integer('id').primary_key(), integer('parent').references('parents', 'id')))
    for relationship, message in (
        (Relationship('children', 'missing', ('id',), ('parent',), many=True), 'mapping'),
        (Relationship('children', 'children', ('name',), ('parent',), many=True), 'kinds'),
        (Relationship('children', 'children', ('id',), ('parent',)), 'unique target'),
    ):
        with raises(SchemaError, match=message):
            Schema((replace(parent, relationships=(relationship,)), child)).normalize()


@mark.parametrize(
    'changes',
    [
        {'name': 42},
        {'primary_key': [42]},
        {'fields': [{'name': 'id', 'kind': 'integer', 'is_primary': True, 'default_sql': 42}]},
    ],
)
def test_schema_metadata_rejects_wrong_value_types(changes):
    metadata = Schema((Table('items', 'Item', (integer('id').primary_key(),)),)).to_dict()
    metadata['tables'][0].update(changes)
    with raises(SchemaError):
        Schema.from_dict(metadata)


def test_schema_metadata_rejects_unknown_top_level_keys():
    with raises(SchemaError, match='only tables'):
        Schema.from_dict({'tables': [], 'unexpected': True})


def test_sql_dialects_and_postgres_schema_restrictions():
    table = Table(
        'items', 'Item', (integer('id').primary_key(), blob('body').nullable()), indexes=(Index('bodies', ('body',)),)
    )
    assert 'CREATE INDEX' in table.index_sql(dialect='postgresql')[0]
    assert '"renamed"' in table.create_sql('renamed', dialect='postgresql')
    for method in (table.create_sql, table.index_sql):
        with raises(SchemaError, match='Unknown dialect'):
            method(dialect='unknown')
    for invalid in (
        replace(table, name='x' * 64),
        replace(table, non_strict=True),
        replace(table, fields=(integer('id').primary_key().default('1'),)),
    ):
        with raises(SchemaError):
            invalid.create_sql(dialect='postgresql')
