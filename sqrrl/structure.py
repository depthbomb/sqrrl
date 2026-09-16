from sqrrl.schema import quote
from aiosqlite import Connection
from re import compile as pattern
from sqrrl.runtime import _fetch_all
from sqrrl.errors import MigrationError

_TOKENS = pattern(
    r"""\s+|--[^\n]*|/\*[\s\S]*?\*/|'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[A-Za-z_][A-Za-z_0-9]*|[0-9]+(?:\.[0-9]+)?|<=|>=|<>|!=|\|\||[^\s]"""
)


def tokens(sql: str) -> tuple[str, ...]:
    """Normalize only provably harmless lexical differences, preserving literals."""
    result = []
    for token in _TOKENS.findall(sql):
        if token.isspace() or token.startswith(('--', '/*')) or token == ';':
            continue

        if token.startswith("'"):
            result.append(token)
        elif token.startswith(('"', '`', '[')):
            delimiter = token[0]
            result.append(token[1:-1].replace(delimiter * 2, delimiter).lower())
        else:
            result.append(token.lower())

    # SQLite removes main qualification and IF NOT EXISTS from stored DDL.
    return tuple(result)


async def _details(connection: Connection, name: str) -> tuple[object, ...]:
    columns = await _fetch_all(connection, f'PRAGMA main.table_xinfo({quote(name)})')
    foreign = await _fetch_all(connection, f'PRAGMA main.foreign_key_list({quote(name)})')
    indexes = await _fetch_all(connection, f'PRAGMA main.index_list({quote(name)})')
    index_details = []
    for index in indexes:
        fields = await _fetch_all(connection, f'PRAGMA main.index_xinfo({quote(str(index[1]))})')
        index_details.append((index[2], index[3], index[4], tuple(tuple(row) for row in fields)))

    return (tuple(tuple(row) for row in columns), tuple(tuple(row) for row in foreign), tuple(sorted(index_details)))


async def compare(connection: Connection, expected: Connection) -> None:
    """Reject every unverified difference, including SQL clauses PRAGMA omits.

    Checks, collations, conflict policies, deferral, generated columns, and
    table options must have identical tokens. This intentionally rejects some
    equivalent DDL. It never rewrites sqlite_schema or application data.
    """
    sql = (
        'SELECT type, name, tbl_name, sql FROM main.sqlite_schema '
        "WHERE lower(substr(name, 1, 7)) != 'sqlite_' AND name != 'sqrrl_migrations' ORDER BY type, name"
    )
    live = await _fetch_all(connection, sql)
    desired = await _fetch_all(expected, sql)
    if [tuple(row[:3]) for row in live] != [tuple(row[:3]) for row in desired]:
        raise MigrationError('Unverified schema objects differ from the declared structure')

    for actual, declared in zip(live, desired, strict=True):
        kind, name, _, definition = actual
        if kind not in ('table', 'index') or not definition:
            raise MigrationError(f'Unsupported schema object: {name}')

        if tokens(definition) != tokens(declared[3]):
            raise MigrationError(f'Unverified constraints or SQL structure on {name}')

        if kind == 'table' and await _details(connection, name) != await _details(expected, name):
            raise MigrationError(f'Column, default, index, or foreign key structure differs on {name}')
