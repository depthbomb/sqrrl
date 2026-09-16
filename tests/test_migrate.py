from pathlib import Path
from pytest import raises
from asyncio import gather
from dataclasses import replace
from sqlite3 import DatabaseError
from sqrrl import Database, MigrationError
from sqrrl.schema import Check, Index, Schema, Table, integer, text
from sqrrl.migrate import apply, baseline, check, custom, diff, load, status, write

def notes(*fields, name="notes", key="", checks=(), indexes=()):
    return Schema(
            (
                Table(
                        name,
                        "Note",
                        (integer("id").primary_key(), text("title"), *fields),
                        key=key,
                        checks=checks,
                        indexes=indexes,
                ),
            )
    )

def test_unreadable_history_is_not_treated_as_missing(tmp_path, monkeypatch):
    path = tmp_path / "migrations"
    path.mkdir()
    iterdir = Path.iterdir

    def inaccessible(target):
        if target == path:
            raise PermissionError("unreadable history")

        return iterdir(target)

    monkeypatch.setattr(Path, "iterdir", inaccessible)
    with raises(MigrationError, match="unreadable history"):
        load(path)

async def test_uppercase_metadata_extension_is_rejected(tmp_path):
    first = await diff((), notes(), "initial")
    write(tmp_path, first)
    path = tmp_path / "000001_initial.json"
    path.rename(tmp_path / "000001_initial.JSON")
    with raises(MigrationError, match="filename"):
        load(tmp_path)

async def test_rebuild_rename_indexes_and_preserved_data(tmp_path):
    schema = notes(text("extra").nullable())
    first = await diff((), schema, "initial")
    renamed = Schema(
            (
                Table(
                        "entries",
                        "Note",
                        (
                            integer("id").primary_key(),
                            text("heading").identity("title"),
                            text("extra").nullable(),
                            integer("priority").default("1"),
                        ),
                        key="notes",
                        checks=(Check("positive", "priority > 0"),),
                        indexes=(Index("entries_priority", ("priority",), where="priority > 1"),),
                ),
            )
    )
    second = await diff((first,), renamed, "rename_notes")
    async with await Database.create(tmp_path / "rebuild.db") as database:
        (await apply(database, (first,)))
        (
            await (
                await database.connection.execute("INSERT INTO notes(title, extra) VALUES ('hello', 'preserved')")
            ).close()
        )
        (await apply(database, (first, second)))
        assert tuple(
                (await (
                    await database.connection.execute("SELECT id, heading, extra, priority FROM entries")).fetchone())
        ) == (
                   1,
                   "hello",
                   "preserved",
                   1,
               )
        assert all(item.applied for item in (await status(database, (first, second))))
        assert (await (await database.connection.execute("PRAGMA foreign_keys")).fetchone())[0] == 1

    assert (await diff((first, second), renamed, "noop")) is None
    (await check((first, second), renamed))

async def test_pending_batch_rolls_back_schema_data_and_ledger(tmp_path):
    initial = notes()
    first = await diff((), initial, "initial")
    expanded = notes(text("extra").nullable())
    second = await diff((first,), expanded, "extra")
    constrained = notes(text("extra").nullable(), checks=(Check("long_title", "length(title) > 10"),))
    third = await diff((first, second), constrained, "constraint")
    async with await Database.create(tmp_path / "atomic.db") as database:
        (await apply(database, (first,)))
        (await (await database.connection.execute("INSERT INTO notes(title) VALUES ('short')")).close())
        with raises(MigrationError):
            (await apply(database, (first, second, third)))

        assert tuple((await (await database.connection.execute("SELECT * FROM notes")).fetchone())) == (1, "short")
        assert (await (await database.connection.execute("SELECT count(*) FROM sqrrl_migrations")).fetchone())[0] == 1
        assert [item.applied for item in (await status(database, (first, second, third)))] == [True, False, False]
        assert (await (await database.connection.execute("PRAGMA foreign_keys")).fetchone())[0] == 1
        assert not database.connection.in_transaction

async def test_destructive_changes_and_backfill_requirements():
    first = await diff((), notes(text("extra").nullable()), "initial")
    with raises(MigrationError, match="allow-drop"):
        (await diff((first,), notes(), "drop_extra"))

    assert await diff((first,), notes(), "drop_extra", allow_drop=True)

async def test_new_required_column_and_primary_key_changes():
    first = await diff((), notes(), "initial")
    with raises(MigrationError, match="Migration names"):
        (await diff((first,), notes(), "../bad_name"))

    with raises(MigrationError, match="backfill"):
        (await diff((first,), notes(text("required")), "add_required"))

    changed = Schema((Table("notes", "Note", (integer("id"), text("title").primary_key())),))
    with raises(MigrationError, match="primary key"):
        (await diff((first,), changed, "key_change", allow_drop=True))

async def test_files_checksums_and_drift(tmp_path):
    first = await diff((), notes(), "initial")
    path = write(tmp_path / "migrations", first)
    history = load(path.parent)
    assert history == (first,)
    with raises(FileExistsError):
        write(path.parent, first)

    assert load(path.parent) == history
    path.write_text(path.read_text() + "-- changed\n", encoding="utf-8")
    with raises(MigrationError, match="SQL differs"):
        load(path.parent)

    async with await Database.create(tmp_path / "drift.db") as database:
        (await apply(database, history))
        (await (await database.connection.execute("CREATE TABLE unmanaged (id INTEGER)")).close())
        with raises(MigrationError, match="drift"):
            (await apply(database, history))

        with raises(MigrationError, match="drift"):
            (await status(database, history))

    with raises(MigrationError, match="checksum"):
        (await check((replace(first, name="tampered"),), notes()))

async def test_custom_backfill_comments_semicolons_and_rollback(tmp_path):
    first = await diff((), notes(), "initial")
    backfill = await custom(
            (first,), "backfill", "-- backfill; comment\nUPDATE notes SET title = 'semi;colon'; /* trailing comment */"
    )
    async with await Database.create(tmp_path / "backfill.db") as database:
        (await apply(database, (first,)))
        (await (await database.connection.execute("INSERT INTO notes(title) VALUES ('before')")).close())
        (await apply(database, (first, backfill)))
        assert (await (await database.connection.execute("SELECT title FROM notes")).fetchone())[0] == "semi;colon"

        failed = await custom((first, backfill), "bad_backfill", "UPDATE notes SET title = NULL;")
        with raises(MigrationError):
            (await apply(database, (first, backfill, failed)))

        assert (await (await database.connection.execute("SELECT title FROM notes")).fetchone())[0] == "semi;colon"
        assert (await (await database.connection.execute("SELECT count(*) FROM sqrrl_migrations")).fetchone())[0] == 2

    for script in (
            "COMMIT;",
            "PRAGMA foreign_keys = OFF;",
            "CREATE TABLE bad (id INTEGER);",
            "UPDATE notes SET title = 'x'; COMMIT;",
            "DELETE FROM sqrrl_migrations;",
            "SELECT * FROM sqrrl_migrations;",
    ):
        with raises((MigrationError, DatabaseError)):
            (await custom((first,), "forbidden", script))

async def test_status_does_not_initialize_database_and_missing_pair(tmp_path):
    first = await diff((), notes(), "initial")
    async with await Database.create(tmp_path / "empty.db") as database:
        assert (await status(database, (first,)))[0].applied is False
        assert (await (await database.connection.execute("SELECT count(*) FROM sqlite_schema")).fetchone())[0] == 0

    (tmp_path / "orphan.sql").write_text("SELECT 1;", encoding="utf-8")
    with raises(MigrationError, match="metadata"):
        load(tmp_path)

async def test_baseline_and_later_migrations(tmp_path):
    schema = notes()
    first = await diff((), schema, "initial")
    second = await diff((first,), notes(text("extra").nullable()), "extra")
    async with await Database.create(tmp_path / "existing.db") as database:
        (await (await database.connection.execute(schema.normalize().tables[0].create_sql())).close())
        (await (await database.connection.execute("INSERT INTO notes(title) VALUES ('existing')")).close())
        with raises(MigrationError, match="drift"):
            (await apply(database, (first,)))

        (await baseline(database, (first, second), 1))
        (await baseline(database, (first, second), 1))
        (await apply(database, (first, second)))
        assert tuple((await (await database.connection.execute("SELECT * FROM notes")).fetchone())) == (
            1,
            "existing",
            None,
        )

async def test_baseline_mismatch_and_active_transaction_leave_database_unchanged(tmp_path):
    first = await diff((), notes(), "initial")
    async with await Database.create(tmp_path / "mismatch.db") as database:
        (await (await database.connection.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT)")).close())
        with raises(MigrationError, match="exact schema match"):
            (await baseline(database, (first,), 1))

        assert (
                   await (
                       await database.connection.execute("SELECT count(*) FROM sqlite_schema WHERE name = 'sqrrl_migrations'")
                   ).fetchone()
               )[0] == 0
        async with database.transaction():
            with raises(MigrationError, match="idle"):
                (await apply(database, (first,)))

            assert (await (await database.connection.execute("PRAGMA foreign_keys")).fetchone())[0] == 1

async def test_concurrent_applications_recheck_history_under_write_lock(tmp_path):
    first = await diff((), notes(), "initial")
    path = tmp_path / "concurrent.db"
    async with await Database.create(path, wal=True):
        pass

    async def migrate():
        async with await Database.open(path) as database:
            (await apply(database, (first,)))

    await gather(migrate(), migrate())

    async with await Database.open(path) as database:
        assert (await (await database.connection.execute("SELECT count(*) FROM sqrrl_migrations")).fetchone())[0] == 1

async def test_foreign_key_violation_during_rebuild_is_atomic(tmp_path):
    initial = Schema(
            (
                Table("parents", "Parent", (integer("id").primary_key(),)),
                Table("children", "Child", (integer("id").primary_key(), integer("parent_id"))),
            )
    )
    first = await diff((), initial, "initial")
    changed = Schema(
            (
                initial.tables[0],
                replace(
                        initial.tables[1],
                        fields=(integer("id").primary_key(), integer("parent_id").references("parents", "id")),
                ),
            )
    )
    second = await diff((first,), changed, "foreign_key")
    async with await Database.create(tmp_path / "foreign.db") as database:
        (await apply(database, (first,)))
        (await (await database.connection.execute("INSERT INTO children(parent_id) VALUES (999)")).close())
        with raises(MigrationError, match="foreign key"):
            (await apply(database, (first, second)))

        assert (await (await database.connection.execute("SELECT parent_id FROM children")).fetchone())[0] == 999
        assert (await (await database.connection.execute("SELECT count(*) FROM sqrrl_migrations")).fetchone())[0] == 1
