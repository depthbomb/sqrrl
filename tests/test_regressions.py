from uuid import uuid4
from sys import modules
from types import ModuleType
from dataclasses import replace
from pytest import mark, raises
from sqrrl.generate import render
from aiosqlite import Connection, connect
from sqrrl.schema import Index, Schema, Table, integer, text
from sqrrl import Database, MigrationError, SchemaError, SqrrlError
from sqlite3 import IntegrityError, OperationalError, Row, connect as sqlite_connect
from sqrrl.migrate import _execute, _split, apply, custom, diff, load, status, write

def generated(schema):
    name = "regression_" + uuid4().hex
    module = ModuleType(name)
    modules[name] = module
    try:
        exec(compile(render(schema), "models.py", "exec"), module.__dict__)
    finally:
        modules.pop(name, None)

    return module

def simple_schema():
    return Schema((Table("notes", "Note", (integer("id").primary_key(), text("title").unique())),))

async def test_caught_sqlite_rollback_cannot_commit_later_mutations(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    async with await Database.create(tmp_path / "rollback.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        client = model.Client(database)
        with raises(SqrrlError, match="transaction|Transaction"):
            async with client.transaction() as transaction:
                (await transaction.notes.create(title="before"))
                try:
                    (
                        await (
                            await database.connection.execute("INSERT OR ROLLBACK INTO notes(title) VALUES ('before')")
                        ).close()
                    )
                except IntegrityError:
                    pass
                (await transaction.notes.create(title="must not commit"))

        assert (await client.notes.query().count()) == 0
        assert (await client.notes.create(title="after scope")).title == "after scope"

async def test_nested_mutation_rollback_invalidates_outer_scope(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    async with await Database.create(tmp_path / "trigger.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        (
            await (
                await database.connection.execute(
                        "CREATE TRIGGER abort_notes BEFORE INSERT ON notes WHEN NEW.title = 'abort' BEGIN SELECT RAISE(ROLLBACK, 'aborted'); END"
                )
            ).close()
        )
        client = model.Client(database)
        with raises(SqrrlError, match="transaction|Transaction"):
            async with client.transaction() as transaction:
                (await transaction.notes.create(title="before"))
                with raises(IntegrityError):
                    (await transaction.notes.create(title="abort"))
                (await transaction.notes.create(title="must not commit"))

        assert (await client.notes.query().count()) == 0

async def test_index_only_migration_keeps_table_storage_and_checks_uniqueness(tmp_path):
    schema = simple_schema()
    first = await diff((), schema, "initial")
    indexed = Schema((replace(schema.tables[0], indexes=(Index("notes_title", ("title",), where="id > 0"),)),))
    second = await diff((first,), indexed, "index")
    assert not any("sqrrl_rebuild" in statement for statement in second.statements)
    async with await Database.create(tmp_path / "index.db") as database:
        (await apply(database, (first,)))
        (await (await database.connection.execute("INSERT INTO notes(title) VALUES ('preserved')")).close())
        rootpage = (
            await (
                await database.connection.execute("SELECT rootpage FROM sqlite_schema WHERE name = 'notes'")
            ).fetchone()
        )[0]
        (await apply(database, (first, second)))
        assert (
                   await (
                       await database.connection.execute("SELECT rootpage FROM sqlite_schema WHERE name = 'notes'")
                   ).fetchone()
               )[0] == rootpage
        assert (await (await database.connection.execute("SELECT title FROM notes")).fetchone())[0] == "preserved"

def test_loading_a_file_as_migration_directory_is_an_error(tmp_path):
    path = tmp_path / "migrations"
    path.write_text("not a directory", encoding="utf-8")
    with raises(MigrationError, match="directory"):
        load(path)

@mark.parametrize(
        "changes",
        [
            {"version": True},
            {"format": True},
            {"name": 3},
            {"checksum": None},
            {"after": "bad"},
            {"parent": []},
            {"statements": (" ",)},
        ],
)
async def test_malformed_migration_metadata_is_rejected(tmp_path, changes):
    first = await diff((), simple_schema(), "initial")
    with raises(MigrationError):
        write(tmp_path / "history", replace(first, **changes))

    assert not (tmp_path / "history").exists()

async def test_index_changes_and_failed_unique_index_are_atomic(tmp_path):
    table = Table(
            "notes", "Note", (integer("id").primary_key(), text("title")), indexes=(Index("notes_title", ("title",)),)
    )
    first = await diff((), Schema((table,)), "initial")
    unique = Schema((replace(table, indexes=(Index("notes_title", ("title",), unique=True),)),))
    second = await diff((first,), unique, "unique_titles")
    async with await Database.create(tmp_path / "unique.db") as database:
        (await apply(database, (first,)))
        (await (await database.connection.execute("INSERT INTO notes(title) VALUES ('same'), ('same')")).close())
        with raises(MigrationError):
            (await apply(database, (first, second)))

        assert (await (await database.connection.execute("SELECT count(*) FROM notes")).fetchone())[0] == 2
        assert (
            await (
                await database.connection.execute("SELECT sql FROM sqlite_schema WHERE name = 'notes_title'")
            ).fetchone()
        )[0].startswith("CREATE INDEX")
        assert len((await status(database, (first,)))) == 1

        removed = await diff((first,), Schema((replace(table, indexes=()),)), "remove_index")
        (await apply(database, (first, removed)))
        assert (
                   await (
                       await database.connection.execute("SELECT count(*) FROM sqlite_schema WHERE name = 'notes_title'")
                   ).fetchone()
               )[0] == 0

async def test_predicate_updates_keep_defaults_and_nulls_with_cached_statements(tmp_path):
    schema = Schema(
            (
                Table(
                        "notes",
                        "Note",
                        (
                            integer("id").primary_key(),
                            text("title").default("'default'"),
                            text("description").nullable().default("'description'"),
                        ),
                ),
            )
    )
    model = generated(schema)
    async with await Database.create(tmp_path / "cached.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        client = model.Client(database)
        omitted = await client.notes.create()
        explicit = await client.notes.create(description=None)
        value = await client.notes.create(description="value")
        repeated = await client.notes.create(description=None)
        assert (omitted.description, explicit.description, value.description, repeated.description) == (
            "description",
            None,
            "value",
            None,
        )
        assert (await client.notes.update(omitted.id, description=None)).description is None
        assert (await client.notes.update(omitted.id, title="changed")).description is None
        assert (await client.notes.update(omitted.id, description="present")).description == "present"

async def test_savepoints_reuse_sql_without_breaking_nested_rollback(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    async with await Database.create(tmp_path / "savepoints.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        statements = []
        (await database.connection.set_trace_callback(statements.append))
        client = model.Client(database)
        async with client.transaction() as transaction:
            for number in range(10):
                (await transaction.notes.create(title=str(number)))
            with raises(ValueError):
                async with transaction.transaction() as inner:
                    (await inner.notes.create(title="rolled back"))
                    raise ValueError("cancel inner")
            (await transaction.notes.create(title="survives"))

        assert (await client.notes.query().count()) == 11
        assert len({statement for statement in statements if statement.startswith("SAVEPOINT")}) == 2

@mark.parametrize(
        "payload", [{"tables": None}, {"tables": [None]}, {"tables": [{"name": "notes"}]}, {"tables": "notes"}]
)
def test_bad_schema_metadata_raises_schema_error(payload):
    with raises(SchemaError):
        Schema.from_dict(payload)

@mark.parametrize("flag", ["false", 0, 1, None, []])
def test_schema_metadata_does_not_coerce_invalid_booleans(flag):
    metadata = simple_schema().to_dict()
    metadata["tables"][0]["fields"][1]["is_nullable"] = flag
    with raises(SchemaError, match="boolean"):
        Schema.from_dict(metadata)

async def test_custom_sql_accepts_comments_between_tokens():
    first = await diff((), simple_schema(), "initial")
    migration = await custom(
            (first,), "backfill", "; -- leading comment\nUPDATE/* explanation */ notes SET title = 'value';"
    )
    assert migration.version == 2

async def test_generated_fields_do_not_shadow_imported_types(tmp_path):
    schema = Schema(
            (
                Table(
                        "records",
                        "Record",
                        (
                            integer("id").primary_key(),
                            text("Column"),
                            text("str"),
                            text("Optional").nullable(),
                            text("Record"),
                            text("value"),
                        ),
                ),
            )
    )
    model = generated(schema)
    async with await Database.create(tmp_path / "names.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        client = model.Client(database)
        row = await client.records.create(Column="column", str="text", Optional=None, Record="record", value="value")
        assert (await client.records.query().where(model.RecordColumns.Column.eq("column")).only()) == row

async def test_generated_model_does_not_shadow_super(tmp_path):
    schema = Schema((Table("records", "super", (integer("id").primary_key(), text("value"))),))
    model = generated(schema)
    async with await Database.create(tmp_path / "super.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        assert (await model.Client(database).records.create(value="ok")).value == "ok"

def test_schema_freezes_input_sequences():
    fields = [integer("id").primary_key(), text("title")]
    tables = [Table("notes", "Note", fields)]
    schema = Schema(tables)
    fields.append(text("later"))
    tables.clear()
    assert len(schema.tables) == 1
    assert len(schema.tables[0].fields) == 2

async def test_temp_ledger_cannot_shadow_main_history(tmp_path):
    schema = simple_schema()
    first = await diff((), schema, "initial")
    async with await Database.create(tmp_path / "shadow.db") as database:
        (await apply(database, (first,)))
        (
            await (
                await database.connection.execute(
                        "CREATE TEMP TABLE sqrrl_migrations(version INTEGER, name TEXT, checksum TEXT, fingerprint TEXT)"
                )
            ).close()
        )
        assert (await status(database, (first,)))[0].applied

async def test_generated_operations_ignore_temporary_table_shadows(tmp_path):
    schema = simple_schema()
    model = generated(schema)
    first = await diff((), schema, "initial")
    async with await Database.create(tmp_path / "temporary.db") as database:
        (await apply(database, (first,)))
        client = model.Client(database)
        note = await client.notes.create(title="main")
        (
            await (
                await database.connection.execute("CREATE TEMP TABLE notes(id INTEGER PRIMARY KEY, title TEXT)")
            ).close()
        )
        (await (await database.connection.execute("INSERT INTO temp.notes VALUES (1, 'temporary')")).close())
        assert (await client.notes.get(note.id)).title == "main"
        assert (await client.notes.query().only()).title == "main"
        assert (await client.notes.update(note.id, title="updated")).title == "updated"
        (await client.notes.delete(note.id))
        (await client.notes.create(title="created"))
        assert (await (await database.connection.execute("SELECT title FROM temp.notes")).fetchone())[0] == "temporary"
        changed = Schema((replace(schema.tables[0], fields=schema.tables[0].fields + (text("extra").nullable(),)),))
        second = await diff((first,), changed, "extra")
        (await apply(database, (first, second)))
        assert tuple((await (await database.connection.execute("SELECT title, extra FROM main.notes")).fetchone())) == (
            "created",
            None,
        )
        assert (await (await database.connection.execute("SELECT title FROM temp.notes")).fetchone())[0] == "temporary"

async def test_sql_splitter_preserves_literals_comments_and_trigger_bodies():
    script = """CREATE TABLE example
                (
                    id    INTEGER PRIMARY KEY,
                    value TEXT
                );
    CREATE TRIGGER change_value
        AFTER INSERT
        ON example
    BEGIN
        UPDATE example SET value = 'it''s; done' WHERE id = NEW.id;
        SELECT CASE WHEN NEW.id > 0 THEN ';' ELSE 'x' END;
    END;
-- a quote ' and a semicolon ; in a comment
    INSERT INTO example(value)
    VALUES ('first;second'); /* ; trailing */ \
             """
    assert len(_split(script)) == 3
    connection = await connect(":memory:", autocommit=True)
    try:
        (await _execute(connection, (script,)))
        assert (await (await connection.execute("SELECT value FROM example")).fetchone())[0] == "it's; done"
    finally:
        (await connection.close())

@mark.parametrize(
        "literal", ["';'", "'one;two;three'", "'it''s; escaped'", '"semi;column"', "`semi;column`", "[semi;column]"]
)
def test_sql_splitter_handles_quoted_delimiters(literal):
    statements = (f"SELECT {literal};", "SELECT 2;")
    assert _split("\n".join(statements)) == statements

class FailedCleanupConnection(Connection):
    fail_rollback = False
    fail_restore = False

    async def execute(self, sql, parameters=()):
        self._check_sql(sql)

        return await super().execute(sql, parameters)

    async def execute_fetchall(self, sql, parameters=()):
        self._check_sql(sql)

        return await super().execute_fetchall(sql, parameters)

    def _check_sql(self, sql):
        if self.fail_rollback and sql == "ROLLBACK":
            raise OperationalError("injected rollback failure")

        if self.fail_restore and sql == "PRAGMA foreign_keys = 1":
            raise OperationalError("injected restore failure")

async def test_failed_migration_cleanup_closes_uncertain_connection():
    connection = await FailedCleanupConnection(lambda: sqlite_connect(":memory:", isolation_level=None), 64)
    (await (await connection.execute("PRAGMA foreign_keys = ON")).close())
    database = Database(connection)
    connection.fail_restore = True
    with raises((MigrationError, OperationalError)):
        (await apply(database, ()))

    with raises(ValueError, match="no active connection"):
        (await connection.execute("SELECT 1"))

async def test_failed_transaction_cleanup_closes_uncertain_connection():
    connection = await FailedCleanupConnection(lambda: sqlite_connect(":memory:", isolation_level=None), 64)
    connection.row_factory = Row
    database = Database(connection)
    connection.fail_rollback = True
    with raises((SqrrlError, OperationalError)):
        async with database.transaction():
            raise ValueError("body failed")

    with raises(ValueError, match="no active connection"):
        (await connection.execute("SELECT 1"))
