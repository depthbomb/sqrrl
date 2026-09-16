from pathlib import Path
from math import isfinite
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from sqrrl.schema import Field, Table, quote
from aiosqlite import Connection, Cursor, connect
from sqlite3 import Row, sqlite_version_info, SQLITE_LIMIT_VARIABLE_NUMBER
from sqrrl.errors import NotFoundError, NotSingularError, SqrrlError, ValidationError
from asyncio import CancelledError, Condition, Lock, Task, current_task, ensure_future, shield
from typing import Any, AsyncIterator, Awaitable, Callable, Generic, Iterable, Optional, TypeVar, cast
from sqrrl.codecs import Codec, decode_datetime, decode_json, encode_datetime, encode_json, enum_type, factory_value, resolve

M = TypeVar("M")
T = TypeVar("T")
R = TypeVar('R')

class Unloaded:
    """A relationship that has not been explicitly loaded."""

UNLOADED = Unloaded()

@dataclass(frozen=True)
class Increment(Generic[T]):
    amount: T

@dataclass(frozen=True)
class Conflict(Generic[M]):
    """An explicit complete unique target and fields copied from incoming values."""

    target: tuple[Column[M, Any], ...]
    update: tuple[Column[M, Any], ...] = ()
    index: Optional[str] = None

class Unset:
    """An omitted mutation field. None always means SQL NULL."""

    def __repr__(self) -> str:
        return "UNSET"

class Database:
    """One serialized writer with optional bounded file-backed reader connections.

    Open with ``async with await Database.open(path)``. Keep a transaction's
    work in its owning task; use separate connections for concurrent work
    inside that scope.
    """

    def __init__(self, connection: Connection, *, immediate: bool = False) -> None:
        if not isinstance(connection, Connection):
            raise TypeError("Database requires an aiosqlite connection; use await Database.open() or create()")

        self._connection = connection
        self._immediate = immediate
        self._savepoint = 0
        self._transaction_failed = False
        self._savepoint_prefix = f"sqrrl_{id(self):x}"
        self._lock = Lock()
        self._owner: Optional[Task[object]] = None
        self._readers: list[Connection] = []
        self._available: list[Connection] = []
        self._read_owners: dict[Task[object], Connection] = {}
        self._reader_condition = Condition()
        self._closing = False
        self._closed = False

    async def __aenter__(self) -> Database:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    @property
    def connection(self) -> Connection:
        """Async SQL access; use a transaction to coordinate raw SQL across tasks.

        The caller owns cursor cleanup and must not share a saved connection
        or cursor with other tasks during a transaction.
        """
        task = current_task()
        if task is not None and task in self._read_owners:
            return self._read_owners[task]

        if self._owner is not None and self._owner is not task:
            raise SqrrlError("The connection is in use by another task")

        self._check_transaction()

        return self._connection

    def _check_transaction(self) -> None:
        if self._savepoint and (self._transaction_failed or not self._connection.in_transaction):
            self._transaction_failed = True
            raise SqrrlError("Transaction ended unexpectedly; leave the transaction scope before reusing the database")

    @classmethod
    async def _connect(cls, path: str | Path, create: bool, wal: bool, timeout: float, immediate: bool, readers: int = 0) -> Database:
        if type(readers) is not int or readers < 0:
            raise ValueError('readers must be a nonnegative integer')
        if sqlite_version_info < (3, 37, 0):
            raise SqrrlError("sqrrl requires SQLite 3.37.0 or newer")

        if not isfinite(timeout) or timeout < 0 or timeout * 1000 > 2 ** 31 - 1:
            raise ValueError("timeout must be a finite, nonnegative number of seconds within SQLite's range")

        mode = "rwc" if create else "rw"
        uri = Path(path).resolve().as_uri() + f"?mode={mode}"
        connection = connect(uri, uri=True, timeout=timeout, isolation_level=None)
        database = cls(connection, immediate=immediate)
        try:
            await _settle(connection)
            connection.row_factory = Row
            await _execute_sql(connection, "PRAGMA foreign_keys = ON")
            await _execute_sql(connection, "PRAGMA synchronous = FULL")
            if wal:
                async with _cursor(connection, "PRAGMA journal_mode = WAL") as cursor:
                    result = await cursor.fetchone()
                    if result is None or result[0].lower() != "wal":
                        raise SqrrlError("SQLite could not enable WAL")
            for _ in range(readers):
                reader = connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=timeout, isolation_level=None)
                database._readers.append(reader)
                await _settle(reader)
                reader.row_factory = Row
                await _execute_sql(reader, 'PRAGMA query_only = ON')
                await _execute_sql(reader, 'PRAGMA foreign_keys = ON')
                database._available.append(reader)
        except BaseException:
            await _settle(database._close_connections())
            raise

        return database

    @classmethod
    async def open(
            cls, path: str | Path, *, wal: bool = False, timeout: float = 5.0, immediate: bool = False, readers: int = 0
    ) -> Database:
        """Open an existing file. Migrations are always applied explicitly."""
        return await cls._connect(path, False, wal, timeout, immediate, readers)

    @classmethod
    async def create(
            cls, path: str | Path, *, wal: bool = False, timeout: float = 5.0, immediate: bool = False, readers: int = 0
    ) -> Database:
        """Create a file if missing, or open an existing database."""
        return await cls._connect(path, True, wal, timeout, immediate, readers)

    async def close(self) -> None:
        if current_task() in self._read_owners:
            raise SqrrlError('Leave the read scope before closing the database')

        if self._closed:
            return

        if self._owner is current_task():
            raise SqrrlError('Leave the transaction or raw read scope before closing the database')

        async with self._lock:
            if self._closed:
                return
            await _settle(self._close_connections())

    async def _close_connections(self) -> None:
        async with self._reader_condition:
            self._closing = True
            self._reader_condition.notify_all()
            await self._reader_condition.wait_for(lambda: not self._read_owners)
        failure = None
        for reader in self._readers + [self._connection]:
            try:
                await reader.close()
            except BaseException as error:
                failure = failure or error
        self._closed = True
        if failure is not None:
            raise failure

    @asynccontextmanager
    async def _read_access(self) -> AsyncIterator[Connection]:
        task = current_task()
        if task is None:
            raise SqrrlError('Reads require an asyncio task')

        if task in self._read_owners:
            yield self._read_owners[task]
            return

        if self._owner is task or not self._readers:
            async with self._access():
                yield self.connection
            return

        async with self._reader_condition:
            await self._reader_condition.wait_for(lambda: self._closing or bool(self._available))
            if self._closing:
                raise SqrrlError('Database is closing or closed')
            connection = self._available.pop()
            self._read_owners[task] = connection
        try:
            yield connection
        finally:
            # No await between releasing ownership and making the connection available.
            del self._read_owners[task]
            self._available.append(connection)
            await _settle(self._notify_readers())

    async def _notify_readers(self) -> None:
        async with self._reader_condition:
            self._reader_condition.notify_all()

    @asynccontextmanager
    async def read_connection(self) -> AsyncIterator[Connection]:
        """Pin a read snapshot. Raw callers must close cursors before leaving.

        Repository reads outside this scope see the latest committed snapshot
        at each statement. Transaction reads always use the writer connection.
        """
        async with self._read_access() as connection:
            nested = connection.in_transaction
            try:
                if not nested:
                    await _execute_sql(connection, 'BEGIN')
                yield connection
            finally:
                if not nested and connection.in_transaction:
                    await _settle(_execute_sql(connection, 'ROLLBACK'))

    @asynccontextmanager
    async def _access(self) -> AsyncIterator[None]:
        task = current_task()
        if task in self._read_owners:
            raise SqrrlError('Cannot write while holding a reader connection')
        if self._owner is task:
            yield

            return

        async with self._lock:
            if self._closing:
                raise SqrrlError('Database is closing or closed')
            self._owner = task
            try:
                yield
            finally:
                self._owner = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Database]:
        """Commit on success and roll back on errors or cancellation.

        Cleanup waits for queued SQLite work. A commit already executing can
        finish before cancellation is delivered.
        """
        async with self._access():
            async with self._transaction():
                yield self

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Database]:
        self._check_transaction()
        nested = self._connection.in_transaction
        savepoint = f"{self._savepoint_prefix}_{self._savepoint + 1}"
        begin = f"SAVEPOINT {savepoint}" if nested else ("BEGIN IMMEDIATE" if self._immediate else "BEGIN")
        commit = f"RELEASE {savepoint}" if nested else "COMMIT"
        beginning = ensure_future(self._connection.execute_fetchall(begin))
        ending = None
        self._savepoint += 1
        try:
            await _settle(beginning)
            yield self
            self._check_transaction()
            ending = ensure_future(self._connection.execute_fetchall(commit))
            await _settle(ending)
        except BaseException:
            started = beginning.done() and not beginning.cancelled() and beginning.exception() is None
            committed = ending is not None and ending.done() and not ending.cancelled() and ending.exception() is None
            if started and not committed:
                await _settle(self._rollback(nested, savepoint))
            raise
        finally:
            self._savepoint -= 1
            if not self._savepoint:
                self._transaction_failed = False

    async def _rollback(self, nested: bool, savepoint: str) -> None:
        try:
            if self._connection.in_transaction:
                if nested:
                    await _execute_sql(self._connection, f"ROLLBACK TO {savepoint}")
                    await _execute_sql(self._connection, f"RELEASE {savepoint}")
                else:
                    await _execute_sql(self._connection, "ROLLBACK")
            elif nested:
                self._transaction_failed = True
        except BaseException as cleanup:
            await self._connection.close()
            raise SqrrlError("Transaction cleanup failed; the connection was closed") from cleanup

@dataclass(frozen=True)
class Predicate(Generic[M]):
    model: type[M]
    expression: str
    arguments: tuple[object, ...] = ()

    def __and__(self, other: Predicate[M]) -> Predicate[M]:
        return self._combine(other, "AND")

    def __or__(self, other: Predicate[M]) -> Predicate[M]:
        return self._combine(other, "OR")

    def __invert__(self) -> Predicate[M]:
        return Predicate(self.model, f"NOT ({self.expression})", self.arguments)

    def _combine(self, other: Predicate[M], operator: str) -> Predicate[M]:
        if self.model is not other.model:
            raise ValidationError("Predicates must belong to the same model")

        return Predicate(
                self.model, f"({self.expression}) {operator} ({other.expression})", self.arguments + other.arguments
        )

@dataclass(frozen=True)
class Order(Generic[M]):
    model: type[M]
    expression: str

@dataclass(frozen=True)
class Column(Generic[M, T]):
    model: type[M]
    field: Field

    def _compare(self, operator: str, value: T) -> Predicate[M]:
        encoded = encode(self.field, value)
        if encoded is None:
            if operator not in ("=", "!="):
                raise ValidationError("NULL only supports equality and inequality comparisons")

            return Predicate(self.model, f"{quote(self.field.name)} IS {'NOT ' if operator == '!=' else ''}NULL")

        return Predicate(self.model, f"{quote(self.field.name)} {operator} ?", (encoded,))

    def eq(self, value: T) -> Predicate[M]:
        return self._compare("=", value)

    def ne(self, value: T) -> Predicate[M]:
        return self._compare("!=", value)

    def gt(self, value: T) -> Predicate[M]:
        return self._compare(">", value)

    def lt(self, value: T) -> Predicate[M]:
        return self._compare("<", value)

    def ge(self, value: T) -> Predicate[M]:
        return self._compare('>=', value)

    def le(self, value: T) -> Predicate[M]:
        return self._compare('<=', value)

    def contains(self: Column[M, str] | Column[M, Optional[str]], value: str) -> Predicate[M]:
        if self.field.kind != 'text' or not isinstance(value, str):
            raise ValidationError('contains requires a text column and string')

        escaped = value.replace('!', '!!').replace('%', '!%').replace('_', '!_')

        return Predicate(self.model, f"{quote(self.field.name)} LIKE ? ESCAPE '!'", ('%' + escaped + '%',))

    def in_(self, *values: T) -> Predicate[M]:
        encoded = tuple(encode(self.field, value) for value in values)
        if not encoded:
            return Predicate(self.model, "0")

        present = tuple(value for value in encoded if value is not None)
        expressions = []
        if present:
            expressions.append(f"{quote(self.field.name)} IN ({', '.join('?' for _ in present)})")

        if None in encoded:
            expressions.append(f"{quote(self.field.name)} IS NULL")

        return Predicate(self.model, "(" + " OR ".join(expressions) + ")", present)

    def is_null(self) -> Predicate[M]:
        return Predicate(self.model, f"{quote(self.field.name)} IS NULL")

    def is_not_null(self) -> Predicate[M]:
        return Predicate(self.model, f"{quote(self.field.name)} IS NOT NULL")

    def asc(self) -> Order[M]:
        return Order(self.model, f"{quote(self.field.name)} ASC")

    def desc(self) -> Order[M]:
        return Order(self.model, f"{quote(self.field.name)} DESC")

@dataclass(frozen=True)
class Query(Generic[M]):
    database: Database
    table: Table
    model: type[M]
    decoder: Callable[[Row], M]
    predicates: tuple[Predicate[M], ...] = ()
    ordering: tuple[Order[M], ...] = ()
    row_limit: Optional[int] = None
    row_offset: int = 0
    loading: tuple[Related[M, Any] | LoadPath[M, Any], ...] = ()

    def _sql(self, projection: str, *, count: bool = False) -> tuple[str, tuple[object, ...]]:
        query = f"SELECT {projection} FROM main.{quote(self.table.name)}"
        arguments = tuple(value for predicate in self.predicates for value in predicate.arguments)
        if self.predicates:
            query += " WHERE " + " AND ".join(f"({predicate.expression})" for predicate in self.predicates)

        if not count and self.ordering:
            query += " ORDER BY " + ", ".join(order.expression for order in self.ordering)

        if not count and self.row_limit is not None:
            query += " LIMIT ?"
            arguments += (self.row_limit,)

        if not count and self.row_offset:
            if self.row_limit is None:
                query += ' LIMIT -1'
            query += ' OFFSET ?'
            arguments += (self.row_offset,)

        return query, arguments

    def where(self, *predicates: Predicate[M]) -> Query[M]:
        if any(predicate.model is not self.model for predicate in predicates):
            raise ValidationError("Predicate belongs to another model")

        return replace(self, predicates=self.predicates + predicates)

    def order_by(self, *ordering: Order[M]) -> Query[M]:
        if any(order.model is not self.model for order in ordering):
            raise ValidationError("Ordering belongs to another model")

        return replace(self, ordering=self.ordering + ordering)

    def limit(self, count: int) -> Query[M]:
        if type(count) is not int or count < 0:
            raise ValueError("limit must be a nonnegative integer")

        return replace(self, row_limit=count)

    def offset(self, count: int) -> Query[M]:
        if type(count) is not int or count < 0:
            raise ValueError('offset must be a nonnegative integer')

        return replace(self, row_offset=count)

    def load(self, *relations: Related[M, Any] | LoadPath[M, Any]) -> Query[M]:
        """Load relationships or typed paths built with ``relation.then(next)``.

        Shared prefixes are fetched once per key batch, within one read snapshot.
        Only the explicit paths are followed, including for self relationships.
        """
        for relation in relations:
            if not isinstance(relation, (Related, LoadPath)):
                raise ValidationError('Expected a relationship or load path')
            if relation.model is not self.model:
                raise ValidationError('Relationship belongs to another model')

        return replace(self, loading=self.loading + relations)

    async def all(self) -> list[M]:
        projection = ", ".join(quote(field.name) for field in self.table.fields)
        sql, arguments = self._sql(projection)
        if self.loading:
            async with self.database.read_connection() as connection:
                rows = [self.decoder(row) for row in await _fetch_all(connection, sql, arguments)]
                nodes: dict[str, _LoadNode] = {}
                for selection in self.loading:
                    branch = nodes
                    path = selection.relations if isinstance(selection, LoadPath) else (selection,)
                    for relation in path:
                        node = branch.setdefault(relation.name, _LoadNode(relation, {}))
                        branch = node.children
                for node in nodes.values():
                    rows = await node.relation._load(connection, rows, node.children)

                return rows

        async with self.database._read_access() as connection:
            return [self.decoder(row) for row in await _fetch_all(connection, sql, arguments)]

    async def first(self) -> Optional[M]:
        rows = await self.limit(0 if self.row_limit == 0 else 1).all()

        return rows[0] if rows else None

    async def only(self) -> M:
        rows = await self.limit(2).all()
        if not rows:
            raise NotFoundError(f"No {self.table.model} matched")

        if len(rows) != 1:
            raise NotSingularError(f"More than one {self.table.model} matched")

        return rows[0]

    async def count(self) -> int:
        """Count matching rows independently of ordering and limit."""
        sql, arguments = self._sql("count(*)", count=True)
        async with self.database._read_access() as connection:
            rows = await _fetch_all(connection, sql, arguments)
            if not rows:
                raise SqrrlError("Count did not return a row")

            return decode_integer(rows[0][0])

    async def exists(self) -> bool:
        sql, arguments = self.limit(0 if self.row_limit == 0 else 1)._sql("1")
        async with self.database._read_access() as connection:
            return bool(await _fetch_all(connection, sql, arguments))

@dataclass(frozen=True)
class Related(Generic[M, R]):
    model: type[M]
    name: str
    parent: Table
    table: Table
    related_model: type[R]
    decoder: Callable[[Row], R]
    fields: tuple[str, ...]
    target: tuple[str, ...]
    many: bool = False

    def then(self, relation: Related[R, T]) -> LoadPath[M, T]:
        """Append a relationship belonging to this relationship's target model."""
        if not isinstance(relation, Related):
            raise ValidationError('Expected a relationship in load path')

        return LoadPath(self.model, relation.related_model, (self, relation))

    def exists(self, *predicates: Predicate[R]) -> Predicate[M]:
        if any(predicate.model is not self.related_model for predicate in predicates):
            raise ValidationError('Related predicate belongs to another model')

        # The derived table exposes outer keys under names that cannot collide
        # with public schema identifiers, including for self relationships.
        projection = ', '.join(f'{quote(source)} AS {quote("_sqrrl_" + str(i))}' for i, source in enumerate(self.fields))
        correlation = ' AND '.join(
            f'{quote(target)} = {quote("_sqrrl_" + str(i))}' for i, target in enumerate(self.target)
        )
        filters = ''.join(f' AND ({p.expression})' for p in predicates)
        keys = ', '.join(quote(field.name) for field in self.parent.keys)
        outer_keys = ', '.join(f'{quote(self.parent.name)}.{quote(field.name)}' for field in self.parent.keys)
        expression = (
            f'({outer_keys}) IN (SELECT {keys} FROM '
            f'(SELECT *, {projection} FROM main.{quote(self.parent.name)}) AS "_sqrrl_parent" '
            f'WHERE EXISTS (SELECT 1 FROM main.{quote(self.table.name)} WHERE {correlation}{filters}))'
        )

        return Predicate(self.model, expression, tuple(arg for p in predicates for arg in p.arguments))

    async def _load(
            self, connection: Connection, parents: list[M], children: Optional[dict[str, _LoadNode]] = None
    ) -> list[M]:
        keys = list(dict.fromkeys(tuple(getattr(row, name) for name in self.fields) for row in parents))
        keys = [key for key in keys if all(value is not None for value in key)]
        width = len(self.target)
        limit = await _parameter_limit(connection)
        if keys and width > limit:
            raise ValidationError('Relationship key exceeds the SQLite parameter limit')

        size = max(1, limit // width)
        related: list[R] = []
        for start in range(0, len(keys), size):
            batch = keys[start:start + size]
            arguments = tuple(encode(self.table.field(name), value) for key in batch for name, value in zip(self.target, key, strict=True))
            columns = ', '.join(map(quote, self.target))
            placeholders = ', '.join('(' + ', '.join('?' for _ in self.target) + ')' for _ in batch)
            order = ', '.join(quote(field.name) for field in self.table.keys)
            sql = f'SELECT * FROM main.{quote(self.table.name)} WHERE ({columns}) IN (VALUES {placeholders}) ORDER BY {order}'
            related.extend(self.decoder(row) for row in await _fetch_all(connection, sql, arguments))

        if related and children:
            for node in children.values():
                related = await node.relation._load(connection, related, node.children)

        groups: dict[tuple[object, ...], list[R]] = {}
        for decoded in related:
            key = tuple(getattr(decoded, name) for name in self.target)
            groups.setdefault(key, []).append(decoded)

        results = []
        for parent in parents:
            matches = groups.get(tuple(getattr(parent, name) for name in self.fields), [])
            if not self.many and len(matches) > 1:
                raise NotSingularError('Singular relationship returned multiple rows')

            value = matches if self.many else (matches[0] if matches else None)
            # Generated models are frozen dataclasses; replace makes a loaded copy.
            results.append(cast(M, replace(cast(Any, parent), **{self.name: value})))

        return results

@dataclass(frozen=True)
class LoadPath(Generic[M, R]):
    """A finite typed relationship path, created with ``Related.then()``."""

    model: type[M]
    related_model: type[R]
    relations: tuple[Related[Any, Any], ...]

    def __post_init__(self) -> None:
        if not self.relations or any(not isinstance(relation, Related) for relation in self.relations):
            raise ValidationError('Load path requires relationships')

        expected: type[Any] = self.model
        for relation in self.relations:
            if relation.model is not expected:
                raise ValidationError('Load path relationship belongs to another model')
            expected = relation.related_model

        if expected is not self.related_model:
            raise ValidationError('Load path target belongs to another model')

    def then(self, relation: Related[R, T]) -> LoadPath[M, T]:
        """Append a relationship belonging to the last target model."""
        if not isinstance(relation, Related):
            raise ValidationError('Expected a relationship in load path')

        return LoadPath(self.model, relation.related_model, self.relations + (relation,))

@dataclass
class _LoadNode:
    relation: Related[Any, Any]
    children: dict[str, _LoadNode]

class Repository(Generic[M]):
    def __init__(self, database: Database, table: Table, model: type[M], decoder: Callable[[Row], M]) -> None:
        self._database = database
        self._table = table
        self._model = model
        self._decoder = decoder
        self._keys = table.keys
        self._quoted_fields = {field.name: quote(field.name) for field in table.fields}
        self._columns = ", ".join(self._quoted_fields.values())
        self._key_expression = " AND ".join(f"{quote(field.name)} = ?" for field in self._keys)
        self._get_sql = f"SELECT {self._columns} FROM main.{quote(table.name)} WHERE {self._key_expression} LIMIT 2"
        self._insert_statements: dict[tuple[str, ...], str] = {}
        self._update_statements: dict[tuple[str, ...], str] = {}

    def _key_predicate(self, values: tuple[object, ...]) -> Predicate[M]:
        if len(values) != len(self._keys):
            raise ValidationError("Wrong number of primary key components")

        arguments = tuple(encode(field, value) for field, value in zip(self._keys, values, strict=True))

        return Predicate(self._model, self._key_expression, arguments)

    async def _insert(self, values: dict[str, object]) -> M:
        names = []
        arguments = []
        for field in self._table.fields:
            value = values.get(field.name, UNSET)
            if isinstance(value, Unset) and field.factory is not None:
                value = factory_value(field.factory)
            if isinstance(value, Unset):
                generated = self._table.auto_key and field in self._keys
                if not generated and not field.is_nullable and field.default_sql is None:
                    raise ValidationError(f"{field.name} is required")
                continue

            names.append(self._quoted_fields[field.name])
            arguments.append(encode(field, value))

        shape = tuple(names)
        sql = self._insert_statements.get(shape)
        if sql is None:
            sql = f"INSERT INTO main.{quote(self._table.name)}"
            if names:
                sql += f" ({', '.join(names)}) VALUES ({', '.join('?' for _ in names)})"
            else:
                sql += " DEFAULT VALUES"

            sql += " RETURNING " + self._columns
            if len(self._insert_statements) < 128:
                self._insert_statements[shape] = sql
        async with self._database.transaction():
            rows = await _fetch_all(self._database.connection, sql, tuple(arguments))
            if not rows:
                raise NotFoundError("Insert did not return a row")

            return self._decoder(rows[0])

    async def _get(self, key: tuple[object, ...]) -> M:
        predicate = self._key_predicate(key)
        async with self._database._read_access() as connection:
            rows = [
                self._decoder(row)
                for row in await _fetch_all(connection, self._get_sql, predicate.arguments)
            ]

        if not rows:
            raise NotFoundError(f"No {self._table.model} matched")

        if len(rows) != 1:
            raise NotSingularError(f"More than one {self._table.model} matched")

        return rows[0]

    async def _update(self, key: tuple[object, ...], values: dict[str, object]) -> M:
        values = self._update_defaults(values)
        predicate = self._key_predicate(key)
        assignments = []
        arguments = []
        for name, value in values.items():
            if isinstance(value, Unset):
                continue

            field = self._table.field(name)
            if field in self._keys or field.is_immutable:
                raise ValidationError(f"{name} cannot be updated")

            assignment, argument = self._assignment(field, value)
            assignments.append(assignment)
            arguments.append(argument)

        if not assignments:
            return await self._get(key)

        shape = tuple(assignments)
        sql = self._update_statements.get(shape)
        if sql is None:
            sql = f"UPDATE main.{quote(self._table.name)} SET {', '.join(assignments)} WHERE {predicate.expression} RETURNING {self._columns}"
            if len(self._update_statements) < 128:
                self._update_statements[shape] = sql
        async with self._database.transaction():
            rows = await _fetch_all(self._database.connection, sql, tuple(arguments) + predicate.arguments)
            if not rows:
                raise NotFoundError(f"No {self._table.model} matched")

            return self._decoder(rows[0])

    async def _delete(self, key: tuple[object, ...]) -> None:
        predicate = self._key_predicate(key)
        sql = f"DELETE FROM main.{quote(self._table.name)} WHERE {predicate.expression} RETURNING 1"
        async with self._database.transaction():
            rows = await _fetch_all(self._database.connection, sql, predicate.arguments)
            if len(rows) != 1:
                raise NotFoundError(f"No {self._table.model} matched")

    def query(self) -> Query[M]:
        return Query(self._database, self._table, self._model, self._decoder)

    def _update_defaults(self, values: dict[str, object]) -> dict[str, object]:
        result = dict(values)
        for field in self._table.fields:
            if field.update_factory is not None and isinstance(result.get(field.name, UNSET), Unset):
                result[field.name] = factory_value(field.update_factory)

        return result

    def _assignment(self, field: Field, value: object) -> tuple[str, object]:
        name = quote(field.name)
        if isinstance(value, Increment):
            if field.kind not in ('integer', 'real'):
                raise ValidationError('Only numeric fields support increments')
            if value.amount is None:
                raise ValidationError('Increment amount cannot be NULL')

            return f'{name} = {name} + ?', encode(field, value.amount)

        return f'{name} = ?', encode(field, value)

    def _mutation_predicate(self, predicate: Optional[Predicate[M]], all_rows: bool) -> Predicate[M]:
        if predicate is None:
            if all_rows is not True:
                raise ValidationError('Supply a predicate or explicitly set all_rows=True')

            return Predicate(self._model, '1')

        if predicate.model is not self._model or all_rows:
            raise ValidationError('Invalid mutation predicate or all_rows combination')

        return predicate

    async def _update_where(self, predicate: Optional[Predicate[M]], values: dict[str, object], *, all_rows: bool = False) -> int:
        predicate = self._mutation_predicate(predicate, all_rows)
        assignments = []
        arguments = []
        incremented = []
        for name, value in self._update_defaults(values).items():
            if isinstance(value, Unset):
                continue
            field = self._table.field(name)
            if field in self._keys or field.is_immutable:
                raise ValidationError(f'{name} cannot be updated')

            assignment, argument = self._assignment(field, value)
            assignments.append(assignment)
            arguments.append(argument)
            if isinstance(value, Increment):
                incremented.append(field)

        if not assignments:
            return 0

        sql = f'UPDATE main.{quote(self._table.name)} SET {", ".join(assignments)} WHERE {predicate.expression}'
        async with self._database.transaction():
            if incremented:
                sql += ' RETURNING ' + ', '.join(quote(field.name) for field in incremented)
                rows = await _fetch_all(self._database.connection, sql, tuple(arguments) + predicate.arguments)
                for row in rows:
                    for field in incremented:
                        decode_field(field, row[field.name])

                return len(rows)

            async with _cursor(self._database.connection, sql, tuple(arguments) + predicate.arguments) as cursor:
                return cursor.rowcount

    async def delete_where(self, predicate: Optional[Predicate[M]] = None, /, *, all_rows: bool = False) -> int:
        predicate = self._mutation_predicate(predicate, all_rows)
        sql = f'DELETE FROM main.{quote(self._table.name)} WHERE {predicate.expression}'
        async with self._database.transaction():
            async with _cursor(self._database.connection, sql, predicate.arguments) as cursor:
                return cursor.rowcount

    def _conflict_sql(self, conflict: Optional[Conflict[M]]) -> str:
        if conflict is None:
            return ''

        for column in conflict.target + conflict.update:
            if column.model is not self._model or self._table.field(column.field.name) != column.field:
                raise ValidationError('Conflict column belongs to another model')

        target = tuple(column.field.name for column in conflict.target)
        unique = {tuple(f.name for f in self._keys)} | {(f.name,) for f in self._table.fields if f.is_unique}
        unique |= {index.fields for index in self._table.indexes if index.unique and index.where is None}
        target_where = ''
        if conflict.index is not None:
            index = next((index for index in self._table.indexes if index.name == conflict.index), None)
            if index is None or not index.unique or index.fields != target:
                raise ValidationError('Conflict index must match the declared unique target')
            if index.where is not None:
                target_where = f' WHERE {index.where}'
        elif target not in unique:
            raise ValidationError('Conflict target must be a complete nonpartial unique key')

        updates = tuple(column.field for column in conflict.update)
        if len(set(updates)) != len(updates) or any(f in self._keys or f.is_immutable for f in updates):
            raise ValidationError('Conflict updates must name distinct mutable fields')

        sql = ' ON CONFLICT (' + ', '.join(map(quote, target)) + ')' + target_where + ' DO '
        if not updates:
            return sql + 'NOTHING'

        return sql + 'UPDATE SET ' + ', '.join(f'{quote(f.name)} = excluded.{quote(f.name)}' for f in updates)

    async def _insert_many(self, values: Iterable[dict[str, object]], conflict: Optional[Conflict[M]] = None) -> list[M]:
        """Return affected rows, excluding skipped rows, in unspecified order.

        Updated rows contain their post-update values. SQLite does not identify
        which returned rows were inserted versus updated. Every chunk belongs
        to one transaction, including decoding and factory failures.
        """
        suffix = self._conflict_sql(conflict)
        results: list[M] = []
        async with self._database.transaction():
            connection = self._database.connection
            limit = await _parameter_limit(connection)
            rows: list[str] = []
            arguments: list[object] = []
            prefix = f'INSERT INTO main.{quote(self._table.name)} ({self._columns}) VALUES '
            for item in values:
                cells = []
                parameters = []
                for name in item:
                    self._table.field(name)
                for field in self._table.fields:
                    value = item.get(field.name, UNSET)
                    if isinstance(value, Unset) and field.factory is not None:
                        value = factory_value(field.factory)

                    if isinstance(value, Unset):
                        if self._table.auto_key and field in self._keys:
                            cells.append('NULL')
                        elif field.default_sql is not None:
                            cells.append(field.default_sql)
                        elif field.is_nullable:
                            cells.append('NULL')
                        else:
                            raise ValidationError(f'{field.name} is required')
                    else:
                        cells.append('?')
                        parameters.append(encode(field, value))

                if len(parameters) > limit:
                    raise ValidationError('One row exceeds the SQLite parameter limit')

                if rows and (len(arguments) + len(parameters) > limit or len(rows) >= 1000):
                    result = await _fetch_all(connection, prefix + ', '.join(rows) + suffix + ' RETURNING ' + self._columns, tuple(arguments))
                    results.extend(self._decoder(row) for row in result)
                    rows.clear()
                    arguments.clear()
                rows.append('(' + ', '.join(cells) + ')')
                arguments.extend(parameters)

            if rows:
                result = await _fetch_all(connection, prefix + ', '.join(rows) + suffix + ' RETURNING ' + self._columns, tuple(arguments))
                results.extend(self._decoder(row) for row in result)

        return results

UNSET = Unset()

async def _parameter_limit(connection: Connection) -> int:
    # aiosqlite has no public getlimit wrapper. Run sqlite3's API on its worker.
    execute = cast(Callable[..., Awaitable[int]], connection._execute)

    return await _settle(execute(connection._conn.getlimit, SQLITE_LIMIT_VARIABLE_NUMBER))

async def _settle(action: Awaitable[T]) -> T:
    """Finish cleanup before propagating cancellation, including repeated cancels."""
    pending = ensure_future(action)
    cancelled = None
    while not pending.done():
        try:
            await shield(pending)
        except CancelledError as error:
            cancelled = error

    result = pending.result()
    if cancelled is not None:
        raise cancelled

    return result

async def _close_cursor(pending: Awaitable[Cursor]) -> None:
    cursor = await pending
    await cursor.close()

@asynccontextmanager
async def _cursor(connection: Connection, sql: str, arguments: tuple[object, ...] = ()) -> AsyncIterator[Cursor]:
    pending = ensure_future(connection.execute(sql, arguments))
    try:
        yield await shield(pending)
    finally:
        await _settle(_close_cursor(pending))

async def _execute_sql(connection: Connection, sql: str, arguments: tuple[object, ...] = ()) -> None:
    await _settle(connection.execute_fetchall(sql, arguments))

async def _fetch_all(connection: Connection, sql: str, arguments: tuple[object, ...] = ()) -> list[Row]:
    rows = await _settle(connection.execute_fetchall(sql, arguments))

    return list(rows)

def encode(field: Field, value: object) -> object:
    if value is None:
        if not field.is_nullable:
            raise ValidationError(f"{field.name} is not nullable")

        return None

    if field.kind == 'datetime':
        return encode_datetime(value)

    if field.kind == 'json':
        return encode_json(value)

    if field.kind == 'enum' and field.python_type is not None:
        kind = enum_type(field.python_type)
        if not isinstance(value, kind):
            raise ValidationError(f'{field.name} requires {kind.__name__}')

        return value.name

    if field.kind == 'custom' and field.codec is not None and field.python_type is not None:
        custom_type = resolve(field.python_type)
        codec = resolve(field.codec)
        if not isinstance(custom_type, type) or not isinstance(value, custom_type) or not isinstance(codec, Codec):
            raise ValidationError('Invalid custom value or codec')

        return decode_blob(codec.encode(value))

    if field.kind == "boolean":
        if type(value) is not bool:
            raise ValidationError(f"{field.name} requires bool")

        return int(value)

    if field.kind == "integer":
        return decode_integer(value)

    if field.kind == "text":
        return decode_text(value)

    if field.kind == "real":
        return decode_real(value)

    if field.kind == "blob":
        return decode_blob(value)

    raise ValidationError(f"Unsupported kind {field.kind}")

def decode_field(field: Field, value: object) -> object:
    if value is None:
        if not field.is_nullable:
            raise ValidationError(f'{field.name} is not nullable')

        return None

    if field.kind == 'enum' and field.python_type is not None:
        try:
            return enum_type(field.python_type)[decode_text(value)]
        except KeyError as error:
            raise ValidationError('Unknown stored enum member') from error

    if field.kind == 'custom' and field.codec is not None and field.python_type is not None:
        codec = resolve(field.codec)
        kind = resolve(field.python_type)
        if not isinstance(codec, Codec) or not isinstance(kind, type):
            raise ValidationError('Invalid custom codec or Python type')

        decoded = codec.decode(decode_blob(value))
        if not isinstance(decoded, kind):
            raise ValidationError('Codec decoded the wrong Python type')

        return decoded

    decoders: dict[str, Callable[[object], object]] = {
        'integer': decode_integer, 'text': decode_text, 'boolean': decode_boolean,
        'real': decode_real, 'blob': decode_blob, 'datetime': decode_datetime, 'json': decode_json,
    }

    return decoders[field.kind](value)

def decode_integer(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not -(2 ** 63) <= value < 2 ** 63:
        raise ValidationError("Expected a signed 64-bit integer")

    return value

def decode_boolean(value: object) -> bool:
    integer = decode_integer(value)
    if integer not in (0, 1):
        raise ValidationError("Expected a stored boolean of 0 or 1")

    return bool(integer)

def decode_text(value: object) -> str:
    if not isinstance(value, str):
        raise ValidationError("Expected text")

    return value

def decode_real(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValidationError("Expected a finite real number")

    try:
        result = float(value)
    except OverflowError as error:
        raise ValidationError("Real number is out of range") from error

    if not isfinite(result):
        raise ValidationError("Expected a finite real number")

    return result

def decode_blob(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise ValidationError("Expected bytes")

    return value

def decode_nullable(value: object, decoder: Callable[[object], T]) -> Optional[T]:
    return None if value is None else decoder(value)
