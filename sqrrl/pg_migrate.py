from re import match
from uuid import uuid4
from copy import deepcopy
from dataclasses import replace
from sqrrl.schema import Schema, quote
from sqrrl.errors import MigrationError
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from sqrrl.runtime import Database, _settle
from urllib.parse import urlsplit, urlunsplit
from sqrrl.postgres import _tokens as sql_tokens
from sqrrl.migrate import Migration, Status, _checksum, _digest, _new, _validate
from sqrrl.postgres import PostgresConnection, leading_sql, preserved_sql, split_sql
from sqrrl.pg_schema import column_sql, constraints, create_sql, default_sql, index_sql


_LEDGER = 'CREATE TABLE sqrrl_migrations (version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, fingerprint TEXT NOT NULL)'


def _plan(previous: Schema, desired: Schema, allow_drop: bool) -> tuple[str, ...]:
    if previous == desired:
        return ()
    statements: list[str] = []
    desired_tables = {table.key: table for table in desired.tables}
    changed = {
        table.key for table in previous.tables if table.key not in desired_tables or table != desired_tables[table.key]
    }
    kept_constraints: dict[str, set[str]] = {}
    kept_indexes: dict[str, set[str]] = {}
    unsafe_targets = set()
    renamed_columns = {
        table.key
        for table in previous.tables
        if table.key in desired_tables
        and any(
            field.key == replacement.key and field.name != replacement.name
            for field in table.fields
            for replacement in desired_tables[table.key].fields
        )
    }
    for table in previous.tables:
        candidate = desired_tables.get(table.key)
        local = constraints(table)
        target = constraints(candidate) if candidate else {}
        explicit_checks = {check.name for check in table.checks}
        kept_constraints[table.key] = {
            name
            for name, sql in local.items()
            if name in target
            and (name not in explicit_checks or (sql == target[name] and table.key not in renamed_columns))
        }
        old_indexes = {index.name: index for index in table.indexes}
        new_indexes = {index.name: index for index in candidate.indexes} if candidate else {}
        kept_indexes[table.key] = set()
        for name, index in old_indexes.items():
            replacement = new_indexes.get(name)
            if (
                candidate is not None
                and replacement is not None
                and (
                    index.unique == replacement.unique
                    and index.where == replacement.where
                    and (index.where is None or table.key not in renamed_columns)
                    and tuple(table.field(field).key for field in index.fields)
                    == tuple(candidate.field(field).key for field in replacement.fields)
                )
            ):
                kept_indexes[table.key].add(name)
        lost_unique = any(
            sql.startswith(('PRIMARY KEY', 'UNIQUE')) and name not in kept_constraints[table.key]
            for name, sql in local.items()
        )
        lost_index = any(index.unique and index.name not in kept_indexes[table.key] for index in table.indexes)
        if (
            candidate is None
            or lost_unique
            or lost_index
            or table.name != candidate.name
            or table.key in renamed_columns
        ):
            unsafe_targets.add(table.name)
    kept_foreign: dict[str, set[str]] = {}
    for table in previous.tables:
        candidate = desired_tables.get(table.key)
        target = constraints(candidate, foreign=True) if candidate else {}
        references = table.foreign_keys + tuple(
            field.reference for field in table.fields if field.reference is not None
        )
        kept_foreign[table.key] = {
            name
            for (name, sql), reference in zip(constraints(table, foreign=True).items(), references, strict=True)
            if target.get(name) == sql and reference.table not in unsafe_targets and table.key not in renamed_columns
        }
        statements.extend(
            f'ALTER TABLE {quote(table.name)} DROP CONSTRAINT {quote(name)};'
            for name in constraints(table, foreign=True)
            if name not in kept_foreign[table.key]
        )
    index_creates: list[str] = []
    for table in previous.tables:
        statements.extend(
            f'DROP INDEX {quote(index.name)};' for index in table.indexes if index.name not in kept_indexes[table.key]
        )
        statements.extend(
            f'ALTER TABLE {quote(table.name)} DROP CONSTRAINT {quote(name)};'
            for name in constraints(table)
            if name not in kept_constraints[table.key]
        )

    old_tables = {table.key: table for table in previous.tables}
    removed = [table for key, table in old_tables.items() if key not in desired_tables]
    if removed and not allow_drop:
        raise MigrationError('Schema removes tables; preserve their key for renames or pass --allow-drop')
    for table in removed:
        statements.append(f'DROP TABLE {quote(table.name)};')
        del old_tables[table.key]
    renamed = [
        (old_tables[table.key], table)
        for table in desired.tables
        if table.key in old_tables and old_tables[table.key].name != table.name
    ]
    for prior_table, table in renamed:
        temporary = 'sqrrl_rename_' + _digest(table.key)[:24]
        statements.append(f'ALTER TABLE {quote(prior_table.name)} RENAME TO {quote(temporary)};')
    for _, table in renamed:
        temporary = 'sqrrl_rename_' + _digest(table.key)[:24]
        statements.append(f'ALTER TABLE {quote(temporary)} RENAME TO {quote(table.name)};')
    for table in desired.tables:
        # Validate identifiers and backend-specific options even for ALTERs.
        create_sql(table)
        old = old_tables.pop(table.key, None)
        if old is not None:
            if tuple(f.key for f in old.keys) != tuple(f.key for f in table.keys):
                raise MigrationError(f'{table.name}: automatic primary key changes are unsupported')
            prior_fields = {field.key: field for field in old.fields}
            for field in table.fields:
                prior = prior_fields.get(field.key)
                if prior is not None and (field.kind, field.codec, field.python_type) != (
                    prior.kind,
                    prior.codec,
                    prior.python_type,
                ):
                    raise MigrationError(f'{table.name}.{field.name}: automatic representation changes are unsupported')
        index_creates.extend(
            sql
            for index, sql in zip(table.indexes, index_sql(table), strict=True)
            if index.name not in kept_indexes.get(table.key, set())
        )
        if old is not None and table.key not in changed:
            continue
        if old is None:
            statements.append(create_sql(table, foreign=False))
            continue
        prefix = f'ALTER TABLE {quote(table.name)} '
        fields = {field.key: field for field in old.fields}
        removed_fields = [field for field in old.fields if field.key not in {f.key for f in table.fields}]
        if removed_fields and not allow_drop:
            raise MigrationError(f'{table.name} removes fields; preserve identity() for renames or pass --allow-drop')
        for field in removed_fields:
            statements.append(prefix + f'DROP COLUMN {quote(field.name)};')
            del fields[field.key]
        renamed_fields = [
            (fields[field.key], field)
            for field in table.fields
            if field.key in fields and fields[field.key].name != field.name
        ]
        for prior, field in renamed_fields:
            temporary = 'sqrrl_rename_' + _digest(field.key)[:24]
            statements.append(prefix + f'RENAME COLUMN {quote(prior.name)} TO {quote(temporary)};')
        for _, field in renamed_fields:
            temporary = 'sqrrl_rename_' + _digest(field.key)[:24]
            statements.append(prefix + f'RENAME COLUMN {quote(temporary)} TO {quote(field.name)};')
        for field in table.fields:
            prior = fields.pop(field.key, None)
            if prior is None:
                if not field.is_nullable and field.default_sql is None:
                    raise MigrationError(f'{table.name}.{field.name} needs a default or a staged backfill')
                statements.append(prefix + 'ADD COLUMN ' + column_sql(table, field) + ';')
                continue
            column = prefix + f'ALTER COLUMN {quote(field.name)} '
            if field.is_nullable != prior.is_nullable:
                statements.append(column + ('DROP' if field.is_nullable else 'SET') + ' NOT NULL;')
            if field.default_sql != prior.default_sql:
                statements.append(
                    column
                    + ('DROP DEFAULT' if field.default_sql is None else 'SET DEFAULT ' + default_sql(field))
                    + ';'
                )
        statements.extend(
            prefix + f'ADD CONSTRAINT {quote(name)} {sql};'
            for name, sql in constraints(table).items()
            if name not in kept_constraints.get(table.key, set())
        )

    statements.extend(index_creates)
    for table in desired.tables:
        statements.extend(
            f'ALTER TABLE {quote(table.name)} ADD CONSTRAINT {quote(name)} {sql};'
            for name, sql in constraints(table, foreign=True).items()
            if name not in kept_foreign.get(table.key, set())
        )
    return tuple(statements)


async def _objects(connection: PostgresConnection, *, ledger: bool = False) -> list[object]:
    """Read the catalog in bounded round trips, preserving the fingerprint format."""
    raw = connection.raw
    functions = await raw.fetchval(
        "SELECT count(*) FROM pg_catalog.pg_proc WHERE pronamespace = 'public'::regnamespace"
    )
    if functions:
        raise MigrationError('Unmanaged PostgreSQL functions are unsupported')
    tables = await raw.fetch(
        """
        SELECT c.oid, c.relname, c.relkind::text, c.relpersistence::text,
               c.relrowsecurity, c.relforcerowsecurity, c.relreplident::text, c.reloptions
        FROM pg_catalog.pg_class c
        WHERE c.relnamespace = 'public'::regnamespace AND c.relkind NOT IN ('i', 'I', 'S')
          AND ($1 OR c.relname <> 'sqrrl_migrations') ORDER BY c.relname
    """,
        ledger,
    )
    for table in tables:
        if table['relkind'] != 'r':
            raise MigrationError(f'Unsupported PostgreSQL schema object: {table["relname"]}')
    identifiers = [table['oid'] for table in tables]
    columns = await raw.fetch(
        """
        SELECT a.attrelid, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
               a.attidentity::text, a.attgenerated::text, pg_catalog.pg_get_expr(d.adbin, d.adrelid),
               co.collname, a.attstorage::text, a.attcompression::text
        FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
        WHERE a.attrelid = ANY($1::oid[]) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attrelid, a.attname
    """,
        identifiers,
    )
    checks = await raw.fetch(
        """
        SELECT conrelid, conname, contype::text, pg_catalog.pg_get_constraintdef(oid, true), convalidated
        FROM pg_catalog.pg_constraint WHERE conrelid = ANY($1::oid[]) AND contype <> 'n'
        ORDER BY conrelid, conname
    """,
        identifiers,
    )
    indexes = await raw.fetch(
        """
        SELECT i.indrelid, ci.relname, am.amname, i.indisunique, i.indisvalid, i.indisready,
               ARRAY(SELECT pg_catalog.pg_get_indexdef(i.indexrelid, k, true)
                     FROM pg_catalog.generate_series(1, i.indnatts) k),
               pg_catalog.pg_get_expr(i.indpred, i.indrelid), ci.reloptions, i.indnullsnotdistinct,
               i.indnkeyatts, i.indisclustered, i.indisreplident,
               pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)
        FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = ci.relam
        WHERE i.indrelid = ANY($1::oid[]) ORDER BY i.indrelid, ci.relname
    """,
        identifiers,
    )
    unsupported = await raw.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = ANY($1::oid[]) AND (NOT tgisinternal OR tgenabled <> 'O')
            UNION ALL SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid = ANY($1::oid[])
            UNION ALL SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class = ANY($1::oid[])
            UNION ALL SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid = ANY($1::oid[])
        )
    """,
        identifiers,
    )
    if unsupported:
        raise MigrationError('Unsupported triggers, row policies, rewrite rules, or inheritance in PostgreSQL schema')
    groups: list[dict[int, list[object]]] = []
    for records in (columns, checks, indexes):
        group: dict[int, list[object]] = {}
        for row in records:
            group.setdefault(row[0], []).append(list(row)[1:])
        groups.append(group)
    result: list[object] = [[list(table)[1:], *(group.get(table['oid'], []) for group in groups)] for table in tables]
    sequences = await raw.fetch("""
        SELECT c.relname, a.attname, s.seqtypid::regtype::text, s.seqstart, s.seqincrement,
               s.seqmax, s.seqmin, s.seqcache, s.seqcycle
        FROM pg_catalog.pg_sequence s JOIN pg_catalog.pg_class sc ON sc.oid = s.seqrelid
        LEFT JOIN pg_catalog.pg_depend d ON d.objid = sc.oid AND d.deptype IN ('a', 'i') AND d.classid = 'pg_catalog.pg_class'::regclass
        LEFT JOIN pg_catalog.pg_class c ON c.oid = d.refobjid
        LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.refobjsubid
        WHERE sc.relnamespace = 'public'::regnamespace ORDER BY c.relname, a.attname
    """)
    if any(row[0] is None for row in sequences):
        raise MigrationError('Unmanaged PostgreSQL sequences are unsupported')
    if sequences:
        result.append(['sequences', [list(row) for row in sequences]])
    return result


async def _fingerprint(connection: PostgresConnection) -> str:
    return _digest(await _objects(connection))


def _structure(objects: list[object]) -> list[object]:
    result = deepcopy(objects)
    for table in result:
        if not isinstance(table, list) or not isinstance(table[0], list):
            continue
        constraint_names = {row[0] for row in table[2]}
        table[2] = sorted([row[1:] for row in table[2]], key=repr)
        # Indexes backing constraints inherit their names. Compare their full
        # definitions while retaining names of explicitly declared indexes.
        for index in table[3]:
            if index[0] in constraint_names:
                index[0] = ''
                index[-1] = index[-1].partition(' ON ')[2]
        table[3].sort(key=repr)
    return result


async def _constraint_names(connection: PostgresConnection) -> dict[str, dict[str, list[str]]]:
    rows = await connection.raw.fetch("""
        SELECT c.relname, con.conname, pg_catalog.pg_get_constraintdef(con.oid, true) AS definition
        FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        WHERE c.relnamespace = 'public'::regnamespace AND con.contype <> 'n'
        ORDER BY c.relname, con.conname
    """)
    result: dict[str, dict[str, list[str]]] = {}
    for row in rows:
        result.setdefault(row['relname'], {}).setdefault(row['definition'], []).append(row['conname'])
    return result


def _rename_constraints(
    statements: tuple[str, ...], mapping: dict[tuple[str, str], str], *, drops_only: bool = False
) -> tuple[str, ...]:
    result = []
    for sql in statements:
        lexemes = [token for token in sql_tokens(sql) if not token[2].startswith(('--', '/*'))]
        words = [token[2] for token in lexemes]
        if len(words) < 3 or words[:2] not in (['CREATE', 'TABLE'], ['ALTER', 'TABLE']):
            result.append(sql)
            continue
        if drops_only and words[:2] != ['ALTER', 'TABLE']:
            result.append(sql)
            continue
        table = words[2][1:-1].replace('""', '"')
        replacements = []
        for index, (start, end, token) in enumerate(lexemes):
            if index and words[index - 1] == 'CONSTRAINT' and token.startswith('"'):
                if drops_only and (index < 2 or words[index - 2] != 'DROP'):
                    continue
                name = token[1:-1].replace('""', '"')
                if (table, name) in mapping:
                    replacements.append((start, end, quote(mapping[table, name])))
        for start, end, replacement in reversed(replacements):
            sql = sql[:start] + replacement + sql[end:]
        result.append(sql)
    return tuple(result)


@asynccontextmanager
async def _scratch(dsn: str) -> AsyncIterator[Database[PostgresConnection]]:
    # An isolated database also contains explicitly public-qualified backfills.
    from sqrrl.postgres import connect

    admin = await connect(dsn, 10.0)
    name = 'sqrrl_replay_' + uuid4().hex
    connection = None
    try:
        await _settle(admin.raw.execute(f'CREATE DATABASE {quote(name)} TEMPLATE template0'))
        parsed = urlsplit(dsn)
        connection = await connect(urlunsplit(parsed._replace(path='/' + name)), 10.0, database=name)
        yield Database(connection)
    finally:

        async def cleanup() -> None:
            try:
                if connection is not None:
                    await connection.close()
                await admin.raw.execute(f'DROP DATABASE IF EXISTS {quote(name)} WITH (FORCE)')
            finally:
                await admin.close()

        await _settle(cleanup())


async def _execute(connection: PostgresConnection, statements: tuple[str, ...]) -> None:
    for statement in statements:
        await connection.raw.execute(statement)


async def _applied(
    connection: PostgresConnection, history: tuple[Migration, ...], objects: Optional[list[object]] = None
) -> int:
    if not await connection.raw.fetchval("SELECT pg_catalog.to_regclass('public.sqrrl_migrations')"):
        return 0
    columns = await connection.raw.fetch("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
               a.attidentity::text, a.attgenerated::text, pg_get_expr(d.adbin, d.adrelid)
        FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = 'public.sqrrl_migrations'::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
    """)
    expected = [
        (name, kind, True, '', '', None)
        for name, kind in (('version', 'bigint'), ('name', 'text'), ('checksum', 'text'), ('fingerprint', 'text'))
    ]
    checks = await connection.raw.fetch("""
        SELECT contype::text, pg_get_constraintdef(oid, true), convalidated
        FROM pg_catalog.pg_constraint WHERE conrelid = 'public.sqrrl_migrations'::regclass AND contype <> 'n'
    """)
    if [tuple(row) for row in columns] != expected or [tuple(row) for row in checks] != [
        ('p', 'PRIMARY KEY (version)', True)
    ]:
        raise MigrationError('Migration ledger schema has changed')
    # Also reject unsupported relation properties, triggers and row policies.
    if objects is None:
        objects = await _objects(connection, ledger=True)
    ledger = next(item for item in objects if isinstance(item, list) and item[0][0] == 'sqrrl_migrations')
    if ledger[0][1:] != ['r', 'p', False, False, 'd', None] or len(ledger[3]) != 1:
        raise MigrationError('Migration ledger schema has changed')
    rows = await connection.raw.fetch(
        'SELECT version, name, checksum, fingerprint FROM public.sqrrl_migrations ORDER BY version'
    )
    if len(rows) > len(history):
        raise MigrationError('Database history is newer than the supplied history')
    for row, migration in zip(rows, history):
        if tuple(row) != (migration.version, migration.name, migration.checksum, migration.after):
            raise MigrationError('Applied migration differs from supplied history')
    return len(rows)


async def _inspect_history(connection: PostgresConnection, history: tuple[Migration, ...]) -> tuple[int, str]:
    objects = await _objects(connection, ledger=True)
    count = await _applied(connection, history, objects)
    application = [
        item
        for item in objects
        if not (isinstance(item, list) and isinstance(item[0], list) and item[0][0] == 'sqrrl_migrations')
    ]
    return count, _digest(application)


async def _record(connection: PostgresConnection, migration: Migration) -> None:
    await connection.raw.execute(
        'INSERT INTO public.sqrrl_migrations (version, name, checksum, fingerprint) VALUES ($1, $2, $3, $4)',
        migration.version,
        migration.name,
        migration.checksum,
        migration.after,
    )


def _validate_pg(history: tuple[Migration, ...]) -> None:
    _validate(history)
    if any(migration.dialect != 'postgresql' for migration in history):
        raise MigrationError('PostgreSQL requires PostgreSQL migration history; use a separate migration directory')


@asynccontextmanager
async def _transaction(database: Database[PostgresConnection]) -> AsyncIterator[PostgresConnection]:
    async with database._access():
        if database.connection.in_transaction:
            raise MigrationError('Migrations require a connection outside a transaction')
        async with database.transaction():
            connection = database.connection
            await connection.raw.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
            await connection.raw.execute('SET LOCAL search_path TO public, pg_catalog, pg_temp')
            # Serialize migration runners even before a ledger exists.
            await connection.raw.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':sqrrl', 0))"
            )
            yield connection


async def apply(database: Database[PostgresConnection], history: tuple[Migration, ...]) -> None:
    _validate_pg(history)
    async with _transaction(database) as connection:
        count, fingerprint = await _inspect_history(connection, history)
        expected = history[count - 1].after if count else _digest([])
        if fingerprint != expected:
            raise MigrationError('Live schema drift detected; refusing to modify the database')
        await connection.raw.execute(_LEDGER.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', 1))
        for migration in history[count:]:
            await _execute(connection, migration.statements)
            if await _fingerprint(connection) != migration.after:
                raise MigrationError(f'Migration {migration.version} did not produce the expected schema')
            await _record(connection, migration)


async def diff(
    history: tuple[Migration, ...], schema: Schema, name: str, *, dsn: str, allow_drop: bool = False
) -> Optional[Migration]:
    _validate_pg(history)
    desired = schema.normalize()
    previous = history[-1].schema if history else Schema(())
    statements = _plan(previous, desired, allow_drop)
    if not statements:
        statements = ('SELECT 1;',)
    async with _scratch(dsn) as replay, _scratch(dsn) as expected:
        await apply(replay, history)
        if history and history[0].structural and previous != desired:
            await _execute(expected.connection, _plan(Schema(()), previous, False))
            actual_names = await _constraint_names(replay.connection)
            canonical_names = await _constraint_names(expected.connection)
            mapping = {
                (table, name): actual
                for table, definitions in canonical_names.items()
                for definition, names in definitions.items()
                for name, actual in zip(names, actual_names[table][definition], strict=True)
            }
            statements = _rename_constraints(statements, mapping, drops_only=True)
            await expected.connection.raw.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public')
        await _execute(expected.connection, _plan(Schema(()), desired, False))
        if history and history[0].preserve_sql:
            await _execute(expected.connection, history[0].preserve_sql)
        before = await _fingerprint(replay.connection)
        async with replay.transaction():
            await _execute(replay.connection, statements)
        after = await _fingerprint(replay.connection)
        if after != await _fingerprint(expected.connection):
            if (
                not history
                or not history[0].structural
                or _structure(await _objects(replay.connection)) != _structure(await _objects(expected.connection))
            ):
                raise MigrationError('Candidate migration does not reproduce the declared schema')
        if previous == desired:
            return None
        migration = replace(_new(history, desired, name, statements, before, after), dialect='postgresql')
        migration = replace(migration, checksum=_checksum(migration))
    async with _scratch(dsn) as replay:
        await apply(replay, history + (migration,))
    return migration


async def status(database: Database[PostgresConnection], history: tuple[Migration, ...]) -> tuple[Status, ...]:
    _validate_pg(history)
    async with _transaction(database) as connection:
        count, fingerprint = await _inspect_history(connection, history)
        expected = history[count - 1].after if count else _digest([])
        if fingerprint != expected:
            raise MigrationError('Live schema drift detected')
    return tuple(Status(m.version, m.name, i < count) for i, m in enumerate(history))


async def baseline(database: Database[PostgresConnection], history: tuple[Migration, ...], version: int) -> None:
    _validate_pg(history)
    if type(version) is not int or not 1 <= version <= len(history):
        raise MigrationError('Baseline version is outside the supplied history')
    async with _scratch(database.connection.dsn) as replay:
        await apply(replay, history[:version])
    async with _transaction(database) as connection:
        count = await _applied(connection, history)
        if count not in (0, version):
            raise MigrationError('Baseline version conflicts with existing migration history')
        if await _fingerprint(connection) != history[version - 1].after:
            raise MigrationError('Baseline requires an exact schema match with the selected migration')
        if count == version:
            return
        await connection.raw.execute(_LEDGER)
        for migration in history[:version]:
            await _record(connection, migration)


async def adopt(
    database: Database[PostgresConnection], schema: Schema, name: str, *, preserve_sql: tuple[str, ...]
) -> Migration:
    desired = schema.normalize()
    for script in preserve_sql:
        if not preserved_sql(script):
            raise MigrationError('Preserved objects require reviewed CREATE TABLE or INDEX statements')
    statements = _plan(Schema(()), desired, False) + preserve_sql
    async with _scratch(database.connection.dsn) as expected:
        await _execute(expected.connection, statements)
        async with _transaction(database) as connection:
            if await connection.raw.fetchval("SELECT pg_catalog.to_regclass('public.sqrrl_migrations')"):
                raise MigrationError('Adoption requires a database without Sqrrl history')
            before = await _fingerprint(connection)
            if _structure(await _objects(connection)) != _structure(await _objects(expected.connection)):
                raise MigrationError('Unverified schema objects differ from the declared structure')
            actual_names = await _constraint_names(connection)
            expected_names = await _constraint_names(expected.connection)
            mapping = {
                (table, constraint): actual
                for table, definitions in expected_names.items()
                for definition, names in definitions.items()
                for constraint, actual in zip(names, actual_names[table][definition], strict=True)
            }
            generated = _plan(Schema(()), desired, False)
            statements = _rename_constraints(generated, mapping) + preserve_sql
            migration = replace(
                _new((), desired, name, statements, _digest([]), before),
                dialect='postgresql',
                structural=True,
                preserve_sql=preserve_sql,
            )
            migration = replace(migration, checksum=_checksum(migration))
    async with _scratch(database.connection.dsn) as replay:
        await apply(replay, (migration,))
    return migration


async def custom(history: tuple[Migration, ...], name: str, sql: str, *, dsn: str) -> Migration:
    _validate_pg(history)
    statements = tuple(part for part in split_sql(sql) if leading_sql(part))
    if not statements:
        raise MigrationError('Custom migration is empty')
    for part in statements:
        keyword = match(r'[A-Za-z]+', leading_sql(part))
        if keyword is None or keyword.group().upper() not in ('INSERT', 'UPDATE', 'DELETE', 'SELECT', 'WITH'):
            raise MigrationError('Custom migrations currently accept data statements only')
    async with _scratch(dsn) as replay:
        await apply(replay, history)
        before = await _fingerprint(replay.connection)
        async with replay.transaction():
            for part in statements:
                statement = await replay.connection.raw.prepare(part)
                await statement.fetch()
            if await _fingerprint(replay.connection) != before or await _applied(replay.connection, history) != len(
                history
            ):
                raise MigrationError('Custom migration changed schema or migration history')
        schema = history[-1].schema if history else Schema(())
        migration = replace(_new(history, schema, name, statements, before, before), dialect='postgresql')
        return replace(migration, checksum=_checksum(migration))
