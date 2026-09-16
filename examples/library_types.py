from enum import Enum
from sqrrl import Codec
from dataclasses import dataclass
from datetime import UTC, datetime


class Binding(Enum):
    PAPER = 'paperback'
    CLOTH = 'hardcover'


@dataclass(frozen=True)
class Label:
    value: str


def utc_now() -> datetime:
    return datetime.now(UTC)


def empty_metadata() -> dict[str, str]:
    return {}


def encode_label(value: Label) -> bytes:
    return value.value.encode('utf-8')


def decode_label(value: bytes) -> Label:
    return Label(value.decode('utf-8'))


label_codec = Codec(encode_label, decode_label)
