from enum import Enum
from math import isfinite
from functools import cache
from json import dumps, loads
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from sqrrl.errors import ValidationError
from typing import Callable, Generic, TypeVar

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
T = TypeVar('T')


@dataclass(frozen=True)
class Codec(Generic[T]):
    """Explicit bytes representation. Change its reference when the format changes."""

    encode: Callable[[T], bytes]
    decode: Callable[[bytes], T]


@cache
def resolve(reference: str) -> object:
    module, _, name = reference.partition(':')

    return getattr(import_module(module), name)


def factory_value(reference: str) -> object:
    factory = resolve(reference)
    if not callable(factory):
        raise ValidationError('A default factory must be callable')

    return factory()


def validate_json(value: object, ancestors: frozenset[int] = frozenset()) -> JsonValue:
    if value is None or type(value) in (bool, int, str):
        if value is None or isinstance(value, (bool, int, str)):
            return value

    if isinstance(value, float) and isfinite(value):
        return value

    if id(value) in ancestors:
        raise ValidationError('JSON cannot contain cycles')

    nested = ancestors | {id(value)}
    if isinstance(value, list):
        return [validate_json(item, nested) for item in value]

    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: validate_json(item, nested) for key, item in value.items()}

    raise ValidationError('Expected JSON primitives, lists, or dictionaries with string keys')


def encode_json(value: object) -> str:
    return dumps(validate_json(value), sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def decode_json(value: object) -> JsonValue:
    if not isinstance(value, str):
        raise ValidationError('Expected stored JSON text')

    try:
        return validate_json(loads(value))
    except (ValueError, RecursionError) as error:
        raise ValidationError('Invalid stored JSON') from error


def encode_datetime(value: object) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError('Expected a timezone-aware datetime')

    return value.astimezone(UTC).isoformat(timespec='microseconds')


def decode_datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValidationError('Expected stored datetime text')

    try:
        result = datetime.fromisoformat(value)
        if encode_datetime(result) != value:
            raise ValueError('Noncanonical datetime')
    except ValueError as error:
        raise ValidationError('Expected canonical UTC datetime text') from error

    return result


def enum_type(reference: str) -> type[Enum]:
    kind = resolve(reference)
    if not isinstance(kind, type) or not issubclass(kind, Enum):
        raise ValidationError('Expected an Enum class')

    return kind
