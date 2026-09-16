from sqrrl import SchemaError
from pytest import mark, raises
from sqrrl.schema import ForeignKey, Index, Schema, Table, integer, text

@mark.parametrize(
        "schema",
        [
            Schema((Table("notes", "Note", (text("title"),)),)),
            Schema((Table("notes", "Note", (integer("id").primary_key().nullable(),)),)),
            Schema((Table("sqrrl_migrations", "Note", (integer("id").primary_key(),)),)),
            Schema((Table("notes", "Client", (integer("id").primary_key(),)),)),
            Schema((Table("notes", "Row", (integer("id").primary_key(),)),)),
            Schema((Table("notes", "decode_integer", (integer("id").primary_key(),)),)),
            Schema((Table("notes", "Note", (integer("id").primary_key(), text("ID"))),)),
            Schema((Table("notes", "Note", (integer("id").primary_key(), text("title").default("random()"))),)),
            Schema((Table("notes", "Note", (integer("id").primary_key(), text("title").default("NULL"))),)),
            Schema((Table("notes", "Note", (integer("id").primary_key(),), indexes=(
                    Index("invalid", ("missing",)),)),)),
            Schema((Table("notes", "Note", (integer("id").primary_key(),
                                            integer("parent").references("missing", "id"))),)),
        ],
)
def test_invalid_schemas_fail_before_generation(schema):
    with raises(SchemaError):
        schema.normalize()

def test_schema_roundtrip(schema):
    assert Schema.from_dict(schema.to_dict()) == schema.normalize()

def test_composite_foreign_keys():
    parent = Table("parents", "Parent", (integer("a"), text("b")), primary_key=("a", "b"))
    child = Table(
            "children",
            "Child",
            (integer("id").primary_key(), integer("a"), text("b")),
            foreign_keys=(ForeignKey(("a", "b"), "parents", ("a", "b")),),
    )
    schema = Schema((parent, child)).normalize()
    assert 'FOREIGN KEY ("a", "b")' in schema.tables[0].create_sql()
