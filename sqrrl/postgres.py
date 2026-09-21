from collections import deque
from functools import lru_cache
from sqrrl.errors import SqrrlError
from re import compile as compile_pattern
from typing import TYPE_CHECKING, Any, Iterator, Optional, Protocol, Sequence, overload

if TYPE_CHECKING:
    from asyncpg import Connection as DriverConnection


class Row(Protocol):
    @overload
    def __getitem__(self, key: str, /) -> Any: ...
    @overload
    def __getitem__(self, key: int, /) -> Any: ...
    @overload
    def __getitem__(self, key: slice, /) -> tuple[Any, ...]: ...
    def __iter__(self) -> Iterator[Any]: ...


class Cursor:
    def __init__(self, rows: Sequence[Row], rowcount: int) -> None:
        self._rows = deque(rows)
        self.rowcount = rowcount

    async def fetchone(self) -> Optional[Row]:
        return self._rows.popleft() if self._rows else None

    async def fetchall(self) -> list[Row]:
        rows = list(self._rows)
        self._rows.clear()
        return rows

    async def fetchmany(self, size: int = 1) -> list[Row]:
        if size < 0:
            raise ValueError('size must be nonnegative')
        return [self._rows.popleft() for _ in range(min(size, len(self._rows)))]

    async def close(self) -> None:
        self._rows.clear()


class PostgresConnection:
    """Small SQL adapter; ``raw`` exposes the native asyncpg connection.

    Sqrrl SQL uses question-mark parameters. Use asyncpg's dollar parameters
    when calling ``raw``. Both interfaces perform native asynchronous I/O.
    """

    def __init__(self, raw: DriverConnection, dsn: str, timeout: float) -> None:
        self.raw = raw
        self.dsn = dsn
        self.timeout = timeout

    @property
    def in_transaction(self) -> bool:
        return self.raw.is_in_transaction()

    async def execute_fetchall(self, sql: str, arguments: tuple[object, ...] = ()) -> Sequence[Row]:
        return await self.raw.fetch(compile_sql(sql), *arguments)

    async def execute(self, sql: str, arguments: tuple[object, ...] = ()) -> Cursor:
        statement = await self.raw.prepare(compile_sql(sql))
        rows = await statement.fetch(*arguments)
        status = statement.get_statusmsg() or ''
        count = status.rsplit(' ', 1)[-1]
        return Cursor(rows, int(count) if count.isdigit() else -1)

    async def execute_count(self, sql: str, arguments: tuple[object, ...] = ()) -> int:
        status = await self.raw.execute(compile_sql(sql), *arguments)
        return int(status.rsplit(' ', 1)[-1])

    async def execute_control(self, sql: str) -> None:
        status = await self.raw.execute(sql)
        if sql == 'COMMIT' and status != 'COMMIT':
            raise SqrrlError('PostgreSQL transaction was aborted and rolled back')

    async def close(self) -> None:
        await self.raw.close()


_TOKEN = compile_pattern(
    r"(?i:\bE)'(?:\\.|''|[^'\\])*'|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*|(?<![\w$])\$(?:[^\W\d]\w*)?\$|\?|\bmain\.|;|[^\W\d][\w$]*"
)


def _tokens(sql: str) -> Iterator[tuple[int, int, str]]:
    position = 0
    while match := _TOKEN.search(sql, position):
        token = match.group()
        position = match.end()
        if token == '/*':
            depth = 1
            while depth and position < len(sql):
                if sql.startswith('/*', position):
                    depth += 1
                    position += 2
                elif sql.startswith('*/', position):
                    depth -= 1
                    position += 2
                else:
                    position += 1
        elif token.startswith('$'):
            end = sql.find(token, position)
            position = len(sql) if end < 0 else end + len(token)
        yield match.start(), position, token


@lru_cache(maxsize=1024)
def compile_sql(sql: str) -> str:
    """Translate generated SQL without touching literals, identifiers or comments."""
    parts = []
    start = number = 0
    for beginning, ending, token in _tokens(sql):
        if token in ('?', 'main.'):
            parts.append(sql[start:beginning])
            if token == '?':
                number += 1
                parts.append(f'${number}')
            else:
                parts.append('public.')
            start = ending
    parts.append(sql[start:])
    return ''.join(parts)


def split_sql(sql: str) -> tuple[str, ...]:
    parts = []
    start = 0
    for _, ending, token in _tokens(sql):
        if token == ';':
            parts.append(sql[start:ending].strip())
            start = ending
    parts.append(sql[start:].strip())
    return tuple(part for part in parts if part.strip('; \r\n\t'))


def leading_sql(sql: str) -> str:
    start = 0
    for beginning, ending, token in _tokens(sql):
        if sql[start:beginning].strip('\ufeff \t\r\n;') or not token.startswith(('--', '/*', ';')):
            break
        start = ending
    return sql[start:].lstrip('\ufeff \t\r\n;')


def preserved_sql(sql: str) -> bool:
    for part in split_sql(sql):
        words = [token.upper() for _, _, token in _tokens(part) if not token.startswith(('--', '/*', ';'))]
        if words and words[:2] not in (['CREATE', 'TABLE'], ['CREATE', 'INDEX'], ['CREATE', 'UNIQUE']):
            return False
    return True


async def connect(dsn: str, timeout: float, *, database: Optional[str] = None) -> PostgresConnection:
    try:
        from asyncpg import PostgresError
        from asyncpg import connect as driver_connect
    except ImportError as error:
        raise SqrrlError('PostgreSQL requires the pg extra: pip install "sqrrl[pg]"') from error

    try:
        raw = await driver_connect(
            dsn,
            database=database,
            timeout=timeout,
            server_settings={'search_path': 'public, pg_catalog, pg_temp', 'standard_conforming_strings': 'on'},
        )
    except PostgresError as error:
        raise SqrrlError(f'Cannot connect to PostgreSQL: {error}') from error
    if raw.get_server_version().major < 15:
        await raw.close()
        raise SqrrlError('sqrrl requires PostgreSQL 15 or newer')
    return PostgresConnection(raw, dsn, timeout)
