from re import fullmatch
from keyword import iskeyword
from typing import Any, Optional
from sqrrl.errors import SchemaError
from functools import cached_property
from dataclasses import asdict, dataclass, replace

@dataclass(frozen=True)
class Check:
    name: str
    expression: str

@dataclass(frozen=True)
class ForeignKey:
    fields: tuple[str, ...]
    table: str
    target: tuple[str, ...]
    on_delete: str = "NO ACTION"

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", tuple(self.fields))
        object.__setattr__(self, "target", tuple(self.target))

@dataclass(frozen=True)
class Index:
    name: str
    fields: tuple[str, ...]
    unique: bool = False
    where: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", tuple(self.fields))

@dataclass(frozen=True)
class Relationship:
    """Explicit local-to-related column mapping, backed by a declared foreign key."""

    name: str
    table: str
    fields: tuple[str, ...]
    target: tuple[str, ...]
    many: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, 'fields', tuple(self.fields))
        object.__setattr__(self, 'target', tuple(self.target))

@dataclass(frozen=True)
class Field:
    name: str
    kind: str
    key: str = ""
    is_nullable: bool = False
    is_primary: bool = False
    is_unique: bool = False
    is_immutable: bool = False
    default_sql: Optional[str] = None
    reference: Optional[ForeignKey] = None
    codec: Optional[str] = None
    python_type: Optional[str] = None
    factory: Optional[str] = None
    update_factory: Optional[str] = None

    def default_factory(self, reference: str) -> Field:
        """Use an importable module:callable on omission, before SQL defaults."""
        return replace(self, factory=reference)

    def on_update(self, reference: str) -> Field:
        """Opt into an importable module:callable for omitted update values."""
        return replace(self, update_factory=reference)

    @property
    def storage_type(self) -> str:
        return _STORAGE[self.kind]

    def nullable(self) -> Field:
        return replace(self, is_nullable=True)

    def primary_key(self) -> Field:
        return replace(self, is_primary=True)

    def unique(self) -> Field:
        return replace(self, is_unique=True)

    def immutable(self) -> Field:
        return replace(self, is_immutable=True)

    def default(self, sql: str) -> Field:
        """Set a SQL literal default, evaluated by SQLite when the field is omitted."""
        return replace(self, default_sql=sql)

    def identity(self, key: str) -> Field:
        """Keep this identity unchanged when renaming a column."""
        return replace(self, key=key)

    def references(self, table: str, column: str, *, on_delete: str = "NO ACTION") -> Field:
        return replace(self, reference=ForeignKey((self.name,), table, (column,), on_delete))

@dataclass(frozen=True)
class Table:
    name: str
    model: str
    fields: tuple[Field, ...]
    key: str = ""
    primary_key: tuple[str, ...] = ()
    indexes: tuple[Index, ...] = ()
    checks: tuple[Check, ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    non_strict: bool = False
    relationships: tuple[Relationship, ...] = ()

    def __post_init__(self) -> None:
        for name in ("fields", "primary_key", "indexes", "checks", "foreign_keys", 'relationships'):
            object.__setattr__(self, name, tuple(getattr(self, name)))

    @cached_property
    def _fields_by_name(self) -> dict[str, Field]:
        return {field.name: field for field in self.fields}

    @cached_property
    def keys(self) -> tuple[Field, ...]:
        names = self.primary_key or tuple(field.name for field in self.fields if field.is_primary)

        return tuple(self.field(name) for name in names)

    @cached_property
    def auto_key(self) -> bool:
        return len(self.keys) == 1 and self.keys[0].kind == "integer"

    def field(self, name: str) -> Field:
        field = self._fields_by_name.get(name)
        if field is not None:
            return field

        raise SchemaError(f"Unknown field {self.name}.{name}")

    def create_sql(self, name: Optional[str] = None) -> str:
        keys = tuple(field.name for field in self.keys)
        parts = []
        for field in self.fields:
            column = f"{quote(field.name)} {field.storage_type}"
            if self.auto_key and field.name in keys:
                column += " PRIMARY KEY"

            if not field.is_nullable:
                column += " NOT NULL"

            if field.is_unique:
                column += " UNIQUE"

            if field.default_sql is not None:
                column += f" DEFAULT {field.default_sql}"

            if field.kind == "boolean":
                column += f" CHECK ({quote(field.name)} IN (0, 1))"

            parts.append(column)

        if not self.auto_key:
            parts.append(f"PRIMARY KEY ({', '.join(map(quote, keys))})")

        for constraint in self.checks:
            parts.append(f"CONSTRAINT {quote(constraint.name)} CHECK ({constraint.expression})")

        references = self.foreign_keys + tuple(f.reference for f in self.fields if f.reference is not None)
        for reference in references:
            source = ", ".join(map(quote, reference.fields))
            target = ", ".join(map(quote, reference.target))
            parts.append(
                    f"FOREIGN KEY ({source}) REFERENCES {quote(reference.table)} ({target}) ON DELETE {reference.on_delete}"
            )

        suffix = ";" if self.non_strict else " STRICT;"

        return f"CREATE TABLE main.{quote(name or self.name)} (\n  " + ",\n  ".join(parts) + "\n)" + suffix

    def index_sql(self) -> tuple[str, ...]:
        statements = []
        for index in self.indexes:
            unique = "UNIQUE " if index.unique else ""
            columns = ", ".join(map(quote, index.fields))
            where = f" WHERE {index.where}" if index.where else ""
            statements.append(
                    f"CREATE {unique}INDEX main.{quote(index.name)} ON {quote(self.name)} ({columns}){where};"
            )

        return tuple(statements)

@dataclass(frozen=True)
class Schema:
    tables: tuple[Table, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "tables", tuple(self.tables))

    def normalize(self) -> Schema:
        return self._normalized

    @cached_property
    def _normalized(self) -> Schema:
        names: set[str] = set()
        identities: set[str] = set()
        symbols = {name.lower() for name in _RESERVED_SYMBOLS}
        normalized = []
        for table in self.tables:
            _identifier(table.name)
            _identifier(table.model)
            _identifier(table.key or table.name)
            if table.name in _CLIENT_MEMBERS:
                raise SchemaError(f"Table name {table.name!r} conflicts with the generated client")

            _unique(names, table.name, "database object")
            _unique(identities, table.key or table.name, "table identity")
            for symbol in (
                    table.model,
                    f"{table.model}Key",
                    f"{table.model}Repository",
                    f"{table.model}Columns",
                    f"_{table.model}Columns",
            ):
                _unique(symbols, symbol, "generated symbol")

            fields = []
            field_names: set[str] = set()
            field_keys: set[str] = set()
            for field in table.fields:
                _identifier(field.name)
                _identifier(field.key or field.name)
                _unique(field_names, field.name, "field")
                _unique(field_keys, field.key or field.name, "field identity")
                if field.kind not in _STORAGE:
                    raise SchemaError(f"Unsupported field kind {field.kind!r}")

                for python_reference in (field.codec, field.python_type, field.factory, field.update_factory):
                    if python_reference is not None:
                        _import_reference(python_reference)

                if (field.kind == 'custom') != (field.codec is not None):
                    raise SchemaError('Custom fields require a codec; other fields cannot declare one')

                if (field.kind in ('custom', 'enum')) != (field.python_type is not None):
                    raise SchemaError('Custom and enum fields require a Python type reference')

                if field.update_factory is not None and (field.is_immutable or field.is_primary or field.name in table.primary_key):
                    raise SchemaError('Immutable fields cannot have update factories')

                if field.default_sql is not None and not _literal(field.default_sql):
                    raise SchemaError(f"{table.name}.{field.name}: defaults must be SQL literals")

                if field.default_sql == "NULL" and not field.is_nullable:
                    raise SchemaError(f"{table.name}.{field.name}: NULL default requires nullable()")

                fields.append(replace(field, key=field.key or field.name))

            candidate = replace(table, key=table.key or table.name, fields=tuple(fields))
            if table.primary_key and any(field.is_primary for field in fields):
                raise SchemaError("Use either Table.primary_key or field primary_key(), not both")

            keys = candidate.keys
            if not keys or len({field.name for field in keys}) != len(keys):
                raise SchemaError(f"{table.name}: declare a distinct primary key")

            if any(field.is_nullable or field.kind not in ("integer", "text") for field in keys):
                raise SchemaError("Primary keys must be non-null integer or text fields")

            checks: set[str] = set()
            for check in table.checks:
                _identifier(check.name)
                _unique(checks, check.name, "check constraint")
                if not check.expression.strip():
                    raise SchemaError("Check expressions cannot be empty")

            for index in table.indexes:
                _identifier(index.name)
                _unique(names, index.name, "database object")
                if not index.fields or len(set(index.fields)) != len(index.fields):
                    raise SchemaError("Indexes must name distinct fields")

                for name in index.fields:
                    candidate.field(name)

                if index.where is not None and not index.where.strip():
                    raise SchemaError("Partial index predicates cannot be empty")

            normalized.append(
                    replace(
                            candidate,
                            indexes=tuple(sorted(table.indexes, key=lambda i: i.name)),
                            checks=tuple(sorted(table.checks, key=lambda c: c.name)),
                    )
            )

        lookup = {table.name: table for table in normalized}
        for table in normalized:
            relation_names = {field.name.lower() for field in table.fields}
            for relation in table.relationships:
                _identifier(relation.name)
                _unique(relation_names, relation.name, 'relationship')
                target = lookup.get(relation.table)
                if target is None or not relation.fields or len(relation.fields) != len(relation.target):
                    raise SchemaError('Invalid relationship mapping')

                for source_name, target_name in zip(relation.fields, relation.target, strict=True):
                    source_field = table.field(source_name)
                    target_field = target.field(target_name)
                    if (source_field.kind, source_field.codec, source_field.python_type) != (target_field.kind, target_field.codec, target_field.python_type):
                        raise SchemaError('Relationship field kinds must match')

                forward = _references(table, relation.fields, target.name, relation.target)
                reverse = _references(target, relation.target, table.name, relation.fields)
                if not forward and not reverse:
                    raise SchemaError('Relationships require a declared foreign key')

                if not relation.many and relation.target not in _unique_keys(target):
                    raise SchemaError('Singular relationships require a unique target')

            references = table.foreign_keys + tuple(f.reference for f in table.fields if f.reference is not None)
            for reference in references:
                if reference.on_delete not in ("NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"):
                    raise SchemaError("Invalid ON DELETE action")

                if (
                        reference.table not in lookup
                        or not reference.fields
                        or len(reference.fields) != len(reference.target)
                ):
                    raise SchemaError(f"Invalid foreign key on {table.name}")

                target = lookup[reference.table]
                target_keys = tuple(field.name for field in target.keys)
                unique_targets = {target_keys} | {(field.name,) for field in target.fields if field.is_unique}
                unique_targets |= {index.fields for index in target.indexes if index.unique and index.where is None}
                if reference.target not in unique_targets:
                    raise SchemaError("Foreign keys must reference a primary key or a complete unique key")

                for source_name, target_name in zip(reference.fields, reference.target, strict=True):
                    source_field = table.field(source_name)
                    target_field = target.field(target_name)
                    if (source_field.kind, source_field.codec, source_field.python_type) != (target_field.kind, target_field.codec, target_field.python_type):
                        raise SchemaError("Foreign key field kinds must match")

                    if reference.on_delete == "SET NULL" and not source_field.is_nullable:
                        raise SchemaError("SET NULL requires nullable foreign key fields")

        return Schema(tuple(sorted(normalized, key=lambda table: table.name)))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self.normalize())
        # Preserve checksums of metadata written before these optional features.
        for table in data['tables']:
            if not table['relationships']:
                del table['relationships']
            for field in table['fields']:
                for name in ('codec', 'python_type', 'factory', 'update_factory'):
                    if field[name] is None:
                        del field[name]

        return data

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> Schema:
        if set(data) != {"tables"}:
            raise SchemaError("Schema metadata must contain only tables")

        tables = []
        for raw in _sequence(data["tables"]):
            _record_types(raw, strings=("name", "model", "key"), booleans=("non_strict",))
            values = dict(raw)
            fields = []
            for item in _sequence(values.pop("fields")):
                _record_types(
                        item,
                        strings=("name", "kind", "key"),
                        booleans=("is_nullable", "is_primary", "is_unique", "is_immutable"),
                        nullable_strings=("default_sql",),
                )
                value = dict(item)
                if value.get("reference") is not None:
                    value["reference"] = _foreign(value["reference"])

                fields.append(Field(**value))

            values["fields"] = tuple(fields)
            values["primary_key"] = _strings(values.get("primary_key", ()))
            values["indexes"] = tuple(_index(i) for i in _sequence(values.get("indexes", ())))
            values["checks"] = tuple(_check(c) for c in _sequence(values.get("checks", ())))
            values["foreign_keys"] = tuple(_foreign(f) for f in _sequence(values.get("foreign_keys", ())))
            values['relationships'] = tuple(_relationship(r) for r in _sequence(values.get('relationships', ())))
            tables.append(Table(**values))

        return cls(tuple(tables)).normalize()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Schema:
        try:
            return cls._from_dict(data)
        except (TypeError, ValueError, KeyError, AttributeError) as error:
            raise SchemaError(f"Invalid schema metadata: {error}") from error

_STORAGE = {'integer': 'INTEGER', 'text': 'TEXT', 'boolean': 'INTEGER', 'real': 'REAL', 'blob': 'BLOB',
            'datetime': 'TEXT', 'json': 'TEXT', 'enum': 'TEXT', 'custom': 'BLOB'}
_CLIENT_MEMBERS = {"database", "transaction"}
_RESERVED_SYMBOLS = {
    "Row",
    "decode_integer",
    "decode_boolean",
    "decode_text",
    "decode_real",
    "decode_blob",
    "decode_nullable",
    "Client",
    "Database",
    "Schema",
    "Field",
    "Table",
    "Column",
    "Predicate",
    "Query",
    "Repository",
    "Unset",
    "UNSET",
    "Optional",
    "AsyncIterator",
    "dataclass",
    "asynccontextmanager",
    "int",
    "str",
    "float",
    "bytes",
    "bool",
    "list",
    "tuple",
}

def _import_reference(value: str) -> None:
    if not isinstance(value, str) or fullmatch(r'[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*', value) is None:
        raise SchemaError('Python references must be module:public_symbol strings')

    if any(iskeyword(part) for part in value.replace(':', '.').split('.')):
        raise SchemaError('Python references cannot contain keywords')

def _relationship(data: dict[str, Any]) -> Relationship:
    _record_types(data, strings=('name', 'table'), booleans=('many',))
    values = dict(data)
    values['fields'] = _strings(data['fields'])
    values['target'] = _strings(data['target'])

    return Relationship(**values)

def _unique_keys(table: Table) -> set[tuple[str, ...]]:
    return {tuple(f.name for f in table.keys)} | {(f.name,) for f in table.fields if f.is_unique} | {
        i.fields for i in table.indexes if i.unique and i.where is None
    }

def _references(table: Table, fields: tuple[str, ...], target: str, columns: tuple[str, ...]) -> bool:
    references = table.foreign_keys + tuple(f.reference for f in table.fields if f.reference is not None)

    return any(r.fields == fields and r.table == target and r.target == columns for r in references)

def _foreign(data: dict[str, Any]) -> ForeignKey:
    _record_types(data, strings=("table", "on_delete"))
    values = dict(data)
    values["fields"] = _strings(data["fields"])
    values["target"] = _strings(data["target"])

    return ForeignKey(**values)

def _index(data: dict[str, Any]) -> Index:
    _record_types(data, strings=("name",), booleans=("unique",), nullable_strings=("where",))
    values = dict(data)
    values["fields"] = _strings(data["fields"])

    return Index(**values)

def _check(data: dict[str, Any]) -> Check:
    _record_types(data, strings=("name", "expression"))

    return Check(**data)

def _record_types(
        data: dict[str, Any],
        *,
        strings: tuple[str, ...] = (),
        booleans: tuple[str, ...] = (),
        nullable_strings: tuple[str, ...] = (),
) -> None:
    if not isinstance(data, dict):
        raise SchemaError("Schema records must be objects")

    for name in strings:
        if name in data and not isinstance(data[name], str):
            raise SchemaError(f"{name} must be a string")

    for name in booleans:
        if name in data and type(data[name]) is not bool:
            raise SchemaError(f"{name} must be a boolean")

    for name in nullable_strings:
        if name in data and data[name] is not None and not isinstance(data[name], str):
            raise SchemaError(f"{name} must be a string or null")

def _sequence(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise SchemaError("Schema sequences must be arrays")

    return tuple(value)

def _strings(value: Any) -> tuple[str, ...]:
    values = _sequence(value)
    if any(not isinstance(item, str) for item in values):
        raise SchemaError("Field names must be strings")

    return values

def _identifier(value: str) -> None:
    valid = fullmatch(r"[A-Za-z][A-Za-z0-9_]*", value)
    if not valid or iskeyword(value) or value.lower().startswith(("sqrrl_", "sqlite_")):
        raise SchemaError(f"Unsupported identifier {value!r}")

def _unique(seen: set[str], value: str, kind: str) -> None:
    lowered = value.lower()
    if lowered in seen:
        raise SchemaError(f"Duplicate {kind}: {value}")

    seen.add(lowered)

def _literal(value: str) -> bool:
    return (
            value == "NULL"
            or fullmatch(r"-?[0-9]+(?:\.[0-9]+)?|'(?:[^']|'')*'|[xX]'(?:[0-9a-fA-F]{2})*'", value) is not None
    )

def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def integer(name: str) -> Field:
    return Field(name, "integer")

def text(name: str) -> Field:
    return Field(name, "text")

def boolean(name: str) -> Field:
    return Field(name, "boolean")

def real(name: str) -> Field:
    return Field(name, "real")

def blob(name: str) -> Field:
    return Field(name, "blob")

def datetime(name: str) -> Field:
    """Aware datetimes stored as UTC ISO text with six fractional digits."""
    return Field(name, 'datetime')

def json(name: str) -> Field:
    """JSON values stored as validated canonical text; None always means SQL NULL."""
    return Field(name, 'json')

def enum(name: str, python_type: str) -> Field:
    """Enum members stored by name, using an importable module:Enum reference."""
    return Field(name, 'enum', python_type=python_type)

def custom(name: str, python_type: str, codec: str) -> Field:
    """A Python type with an importable Codec instance encoding to bytes."""
    return Field(name, 'custom', python_type=python_type, codec=codec)
