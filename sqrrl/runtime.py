from pathlib import Path
from math import isfinite
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from sqlite3 import Row, sqlite_version_info
from sqrrl.schema import Field, Table, quote
from aiosqlite import Connection, Cursor, connect
from typing import AsyncIterator, Awaitable, Callable, Generic, Optional, TypeVar
from asyncio import CancelledError, Lock, Task, current_task, ensure_future, shield
from sqrrl.errors import NotFoundError, NotSingularError, SqrrlError, ValidationError

M = TypeVar("M")
T = TypeVar("T")

class Unset:
    """An omitted mutation field. None always means SQL NULL."""

    def __repr__(self) -> str:
        return "UNSET"

class Database:
    """One async SQLite connection. Transactions serialize access across tasks.

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
        if self._owner is not None and self._owner is not current_task():
            raise SqrrlError("The connection is in use by another task")

        self._check_transaction()

        return self._connection

    def _check_transaction(self) -> None:
        if self._savepoint and (self._transaction_failed or not self._connection.in_transaction):
            self._transaction_failed = True
            raise SqrrlError("Transaction ended unexpectedly; leave the transaction scope before reusing the database")

    @classmethod
    async def _connect(cls, path: str | Path, create: bool, wal: bool, timeout: float, immediate: bool) -> Database:
        if sqlite_version_info < (3, 37, 0):
            raise SqrrlError("sqrrl requires SQLite 3.37.0 or newer")

        if not isfinite(timeout) or timeout < 0 or timeout * 1000 > 2 ** 31 - 1:
            raise ValueError("timeout must be a finite, nonnegative number of seconds within SQLite's range")

        mode = "rwc" if create else "rw"
        uri = Path(path).resolve().as_uri() + f"?mode={mode}"
        connection = connect(uri, uri=True, timeout=timeout, isolation_level=None)
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
        except BaseException:
            await _settle(connection.close())
            raise

        return cls(connection, immediate=immediate)

    @classmethod
    async def open(
            cls, path: str | Path, *, wal: bool = False, timeout: float = 5.0, immediate: bool = False
    ) -> Database:
        """Open an existing file. Migrations are always applied explicitly."""
        return await cls._connect(path, False, wal, timeout, immediate)

    @classmethod
    async def create(
            cls, path: str | Path, *, wal: bool = False, timeout: float = 5.0, immediate: bool = False
    ) -> Database:
        """Create a file if missing, or open an existing database."""
        return await cls._connect(path, True, wal, timeout, immediate)

    async def close(self) -> None:
        async with self._access():
            await _settle(self._connection.close())

    @asynccontextmanager
    async def _access(self) -> AsyncIterator[None]:
        task = current_task()
        if self._owner is task:
            yield

            return

        async with self._lock:
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

    async def all(self) -> list[M]:
        projection = ", ".join(quote(field.name) for field in self.table.fields)
        sql, arguments = self._sql(projection)
        async with self.database._access():
            return [self.decoder(row) for row in await _fetch_all(self.database.connection, sql, arguments)]

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
        async with self.database._access():
            rows = await _fetch_all(self.database.connection, sql, arguments)
            if not rows:
                raise SqrrlError("Count did not return a row")

            return decode_integer(rows[0][0])

    async def exists(self) -> bool:
        sql, arguments = self.limit(0 if self.row_limit == 0 else 1)._sql("1")
        async with self.database._access():
            return bool(await _fetch_all(self.database.connection, sql, arguments))

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
        async with self._database._access():
            rows = [
                self._decoder(row)
                for row in await _fetch_all(self._database.connection, self._get_sql, predicate.arguments)
            ]

        if not rows:
            raise NotFoundError(f"No {self._table.model} matched")

        if len(rows) != 1:
            raise NotSingularError(f"More than one {self._table.model} matched")

        return rows[0]

    async def _update(self, key: tuple[object, ...], values: dict[str, object]) -> M:
        predicate = self._key_predicate(key)
        assignments = []
        arguments = []
        for name, value in values.items():
            if isinstance(value, Unset):
                continue

            field = self._table.field(name)
            if field in self._keys or field.is_immutable:
                raise ValidationError(f"{name} cannot be updated")

            assignments.append(f"{self._quoted_fields[name]} = ?")
            arguments.append(encode(field, value))

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

UNSET = Unset()

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
