from os import environ
from dataclasses import dataclass
from sqrrl.migrate import apply, diff
from sqrrl.pg_migrate import _scratch
from pytest import fixture, mark, skip
from test_regressions import generated
from sqrrl import Codec, Database, UNLOADED
from sqrrl.schema import ForeignKey, Index, Relationship, Schema, Table, custom, integer, json

@dataclass
class Label:
    value: str

label_codec = Codec(lambda label: label.value.encode(), lambda value: Label(value.decode()))

@fixture(params=['sqlite', 'postgresql'])
async def db(request, tmp_path):
    if request.param == 'sqlite':
        async with await Database.create(tmp_path / 'relations.db', readers=1) as database:
            yield database
    else:
        dsn = environ.get('SQRRL_TEST_POSTGRES')
        if not dsn:
            skip('Set SQRRL_TEST_POSTGRES to run PostgreSQL integration tests')
        async with _scratch(dsn) as database:
            yield database

@mark.parametrize('kind', ['json', 'json_scalars', 'custom'])
async def test_relationships_match_stored_keys(db, kind):
    def field(name):
        return custom(name, f'{__name__}:Label', f'{__name__}:label_codec') if kind == 'custom' else json(name)

    schema = Schema((
        Table('parents', 'Parent', (integer('id').primary_key(), integer('scope'), field('code')),
              indexes=(Index('parent_keys', ('scope', 'code'), unique=True),),
              relationships=(Relationship('children', 'children', ('scope', 'code'), ('scope', 'code'), many=True),)),
        Table('children', 'Child', (integer('id').primary_key(), integer('scope').nullable(), field('code').nullable()),
              foreign_keys=(ForeignKey(('scope', 'code'), 'parents', ('scope', 'code')),),
              relationships=(Relationship('parent', 'parents', ('scope', 'code'), ('scope', 'code')),)),
    ))
    models = generated(schema)
    dsn = db.connection.dsn if db.dialect == 'postgresql' else None
    await apply(db, (await diff((), schema, 'initial', dsn=dsn),))
    client = models.Client(db)
    values = {
        'json': [{'nested': [True, 1]}, [1, 2]],
        'json_scalars': [1, True, 1.0],
        'custom': [Label('first'), Label('second')],
    }[kind]
    for index, value in enumerate(values):
        await client.parents.create(scope=1, code=value)
        await client.children.create_many(models.ChildCreate(scope=1, code=value) for _ in range(index + 1))
    await client.parents.create(scope=2, code=values[0])
    await client.children.create(scope=2, code=values[0])
    missing = await client.children.create(scope=1)
    expected_counts = [*range(1, len(values) + 1), 1]
    if kind == 'json':
        # Stored JSON can have different formatting while decoding to equal objects.
        async with db.transaction():
            for value in (' { "raw": [1] } ', '{"raw":[1]}'):
                await db.connection.execute_fetchall('INSERT INTO parents(scope, code) VALUES (1, ?)', (value,))
                await db.connection.execute_fetchall('INSERT INTO children(scope, code) VALUES (1, ?)', (value,))
                expected_counts.append(1)

    children = models.ParentRelations.children
    parent = models.ChildRelations.parent
    rows = await client.parents.query().order_by(models.ParentColumns.id.asc()).load(children.then(parent)).all()
    assert [len(row.children) for row in rows] == expected_counts
    for row in rows:
        for child in row.children:
            assert child.parent.id == row.id
            assert child.parent.children is UNLOADED
    child = await client.children.query().where(models.ChildColumns.id.eq(missing.id)).load(parent).only()
    assert child.parent is None
