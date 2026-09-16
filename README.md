# sqrrl (pronounced squirrel)

Typed async SQLite access, generated from a Python schema, with migrations you can
read before you run them.

Describe your tables once, and sqrrl generates dataclasses, repositories, and
column helpers with real type annotations. Your editor knows which fields a
query returns and which arguments a write accepts. Database calls run through
`aiosqlite`.

Requires **Python 3.14+** and **SQLite 3.37+**. This is an early release, and the
API is still taking shape.

## Getting started

With your virtual environment active:

```sh
python -m pip install sqrrl
```

In your app's directory, create a starter schema and configuration:

```sh
sqrrl init
```

That gives you `schema.py` with a small notes table and a `sqrrl.json` file:

```json
{
  "schema": "schema:schema",
  "output": "models.py",
  "migrations": "migrations"
}
```

Generate the Python code and your first migration, then create the database:

```sh
sqrrl generate
sqrrl migrate diff initial
# Review migrations/000001_initial.sql before applying it.
sqrrl migrate up --db app.db
```

Save this as `main.py` next to the generated `models.py`:

```python
from asyncio import run
from sqrrl import Database
from models import Client, NoteColumns


async def main() -> None:
    async with await Database.open("app.db") as database:
        client = Client(database)
        note = await client.notes.create(title="Try sqrrl")
        print(note.id, note.title, note.done)

        pending = await client.notes.query().where(NoteColumns.done.eq(False)).all()
        print([item.title for item in pending])

        await client.notes.update(note.id, done=True)


run(main())
```

Run it with `python main.py`. `Database.open()` requires an existing file;
`Database.create()` creates one if it's missing. Neither applies migrations
automatically. `sqrrl migrate up` creates the database when needed and applies
pending migrations.

You can use `python -m sqrrl` anywhere you'd use `sqrrl`.

## Defining a schema

A schema is a regular Python object. Here's the notes table from the starter:

```python
from sqrrl.schema import Schema, Table, boolean, integer, text

schema = Schema(
    tables=(
        Table(
            "notes",
            model="Note",
            fields=(
                integer("id").primary_key(),
                text("title"),
                boolean("done").default("0"),
            ),
        ),
    )
)
```

The field helpers are `integer`, `text`, `boolean`, `real`, and `blob`. Fields
are required unless you add `.nullable()`. You can also declare unique fields,
foreign keys, immutable fields, indexes, checks, and composite primary keys.
Tables use SQLite's `STRICT` mode.

Defaults are **SQL expressions**, so `.default('0')` stores zero and
`.default("'draft'")` stores the text `draft`. `.immutable()` leaves a field out
of the generated update method; it doesn't prevent changes through raw SQL.

The [example schema](https://github.com/depthbomb/sqrrl/blob/master/examples/schema.py)
includes users, tasks, foreign keys, an index, and a composite key. Its
[generated models](https://github.com/depthbomb/sqrrl/blob/master/examples/models.py)
show what sqrrl produces.

Configuration paths are relative to the configuration file. `schema` names an
importable module and its exported object, such as `myapp.schema:schema`.
Use `--config path/to/sqrrl.json` on a command to select another configuration.
The output file and migrations directory must stay inside that configuration's
directory. Schema modules are imported during generation and schema checks, so
keep them free of side effects and stdout output.

## Reading and writing

Each generated repository has `create`, `get`, `update`, `delete`, and `query`.
Using the starter's `client`:

```python
note = await client.notes.create(title="Ship something small")
note = await client.notes.get(note.id)
note = await client.notes.update(note.id, title="Ship sqrrl")
await client.notes.delete(note.id)
```

Rows are frozen dataclasses. Writes return new rows instead of changing the
objects you already have. A single integer primary key can be omitted on
creation. For composite keys, the generator provides a key dataclass, such as
`SettingKey(user_id=1, key='theme')` in the example.

Omitting an optional write argument uses `UNSET`: on creation, SQLite gets to
apply its default; on update, that field stays unchanged. Passing `None`
explicitly writes SQL `NULL` and requires a nullable field.

Build queries with the generated column helpers:

```python
query = client.notes.query().where(NoteColumns.done.eq(False))
latest = await query.order_by(NoteColumns.id.desc()).limit(10).all()
first = await query.order_by(NoteColumns.id.asc()).first()
count = await query.count()
has_notes = await query.exists()
```

Query builders return new queries, so you can reuse a base query. Values are
bound as SQL parameters. Column helpers support `eq`, `ne`, `gt`, `lt`, `in_`,
`is_null`, and `is_not_null`. Combine predicates with `&`, `|`, and `~`:

```python
matching = await client.notes.query().where(NoteColumns.done.eq(False) & NoteColumns.id.gt(10)).all()
```

`first()` returns `None` when nothing matches. `only()` requires exactly one row:
it raises `NotFoundError` for no matches and `NotSingularError` for multiple
matches. `get()` and `update()` also raise `NotFoundError` for a missing key;
deleting a missing row is fine. These errors are available from `sqrrl`.

## Transactions and connections

Group writes with a transaction:

```python
async with client.transaction() as transaction:
    first = await transaction.notes.create(title="Write the README")
    await transaction.notes.create(title="Publish the package")
    await transaction.notes.update(first.id, done=True)
```

The transaction commits on success and rolls back on errors. Nested
transactions use savepoints. Cancellation waits for queued SQLite work to
settle; a commit that has already started can finish before cancellation arrives.

Each `Database` owns one connection and serializes repository access between
tasks. Keep a transaction's work in the task that opened it. Awaiting another
task that needs the same connection while holding a transaction can deadlock.
Use separate connections for concurrent work inside that scope.

Foreign keys are enabled. `Database.open()` and `Database.create()` also accept
`wal=True`, `timeout=5.0` (seconds), and `immediate=True` for `BEGIN IMMEDIATE`
transactions. WAL is opt-in.

For SQL beyond the query builder, `database.connection` exposes the underlying
`aiosqlite` connection. Use `database.transaction()` to coordinate raw SQL with
repository operations, close your cursors, and keep the connection in its owning
task during a transaction.

## Changing the database

After editing `schema.py`, regenerate the models and create a migration:

```sh
sqrrl generate
sqrrl migrate diff add_description
sqrrl migrate check
sqrrl migrate status --db app.db
sqrrl migrate up --db app.db
```

Each migration has a readable `.sql` file and a matching `.json` file containing
schema metadata and checksums. Commit both, along with your generated models.
Review the SQL before applying it. Don't edit migration history in place:
sqrrl checks that the SQL, metadata, and checksum chain agree.

`migrate check` replays history in a scratch database and compares the result
with your declared schema. Applying migrations checks the live schema for drift
and applies the pending batch in one transaction, including foreign key checks.

Some changes need a little planning:

- Dropping tables or fields requires `migrate diff NAME --allow-drop`.
- For a field rename, keep its old identity with
  `text('new_name').identity('old_name')`. For a table rename, preserve its `key`.
- A new required field needs a default or a staged backfill. Automatic primary
  key changes and field representation changes are unsupported.
- `sqrrl migrate custom backfill --sql backfill.sql` records data-only SQL.
  Custom schema objects, such as views and triggers, aren't supported.
- `sqrrl migrate baseline --db existing.db --version 1` adopts a database only
  when its schema matches that migration exactly. Equivalent DDL written
  differently can still be rejected.

There is no downgrade command. Make further changes with new migrations.

For an app's CI, these commands catch stale models and missing migrations:

```sh
sqrrl generate --check
sqrrl migrate check
```

## Development

From a checkout, create a Python 3.14+ virtual environment with
`python -m venv .venv`, then activate it (`.venv\Scripts\Activate.ps1` in
PowerShell, or `source .venv/bin/activate` in bash).

```sh
python -m pip install -e ".[dev]"
python -m pytest --cov=sqrrl --cov-branch
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pyright
python -m sqrrl generate --config examples/sqrrl.json --check
python -m sqrrl migrate check --config examples/sqrrl.json
python -m examples.main
python -m build --outdir dist/release
python -m twine check --strict dist/release/*
```

The example uses a temporary database and cleans up after itself. The
`benchmarks/` directory has separate runners for database operations and import
overhead.
