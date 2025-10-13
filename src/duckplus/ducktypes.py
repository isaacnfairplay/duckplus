"""DuckDB-oriented type markers for typed column expressions."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal as DecimalValue
from typing import Any, ClassVar, Iterable

__all__ = [
    "DuckType",
    "Unknown",
    "Comparable",
    "Numeric",
    "Temporal",
    "StringLike",
    "Boolean",
    "Blob",
    "Date",
    "Double",
    "Decimal",
    "Float",
    "HugeInt",
    "BigInt",
    "Integer",
    "Interval",
    "Json",
    "SmallInt",
    "Timestamp",
    "TimestampTz",
    "Time",
    "TinyInt",
    "UTinyInt",
    "USmallInt",
    "UInteger",
    "UBigInt",
    "Varchar",
    "lookup",
]


class DuckType:
    """Base marker describing a DuckDB logical type."""

    duckdb_name: ClassVar[str] = "ANY"
    categories: ClassVar[frozenset[str]] = frozenset()
    python_types: ClassVar[tuple[type[Any], ...]] = ()
    python_annotation: ClassVar[Any] = Any

    @classmethod
    def supports(cls, category: str) -> bool:
        """Return ``True`` when the logical type participates in *category*."""

        return category in cls.categories

    @classmethod
    def describe(cls) -> str:
        """Return a human-friendly description of the logical type."""

        return cls.duckdb_name

class Unknown(DuckType):
    """Sentinel representing an unspecified DuckDB type."""

    duckdb_name: ClassVar[str] = "UNKNOWN"
    python_annotation: ClassVar[Any] = Any


class Comparable(DuckType):
    """Marker for values that can be ordered or compared."""

    categories: ClassVar[frozenset[str]] = frozenset({"comparable"})
    python_annotation: ClassVar[Any] = object


class Numeric(Comparable):
    """Marker for numeric types."""

    categories: ClassVar[frozenset[str]] = Comparable.categories | {"numeric"}
    python_types: ClassVar[tuple[type[Any], ...]] = (int, float, DecimalValue)
    python_annotation: ClassVar[Any] = int | float | DecimalValue


class Temporal(Comparable):
    """Marker for temporal types such as ``TIMESTAMP`` and ``DATE``."""

    categories: ClassVar[frozenset[str]] = Comparable.categories | {"temporal"}
    python_types: ClassVar[tuple[type[Any], ...]] = (datetime, date, time)
    python_annotation: ClassVar[Any] = datetime | date | time


class StringLike(Comparable):
    """Marker for textual values."""

    categories: ClassVar[frozenset[str]] = Comparable.categories | {"string"}
    python_types: ClassVar[tuple[type[Any], ...]] = (str,)
    python_annotation: ClassVar[Any] = str


class Boolean(Comparable):
    """DuckDB ``BOOLEAN`` values."""

    duckdb_name: ClassVar[str] = "BOOLEAN"
    python_types: ClassVar[tuple[type[Any], ...]] = (bool,)
    python_annotation: ClassVar[Any] = bool


class TinyInt(Numeric):
    """DuckDB ``TINYINT`` values."""

    duckdb_name: ClassVar[str] = "TINYINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class SmallInt(Numeric):
    """DuckDB ``SMALLINT`` values."""

    duckdb_name: ClassVar[str] = "SMALLINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class Integer(Numeric):
    """DuckDB ``INTEGER`` values."""

    duckdb_name: ClassVar[str] = "INTEGER"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class BigInt(Numeric):
    """DuckDB ``BIGINT`` values."""

    duckdb_name: ClassVar[str] = "BIGINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class UInteger(Numeric):
    """DuckDB ``UINTEGER`` values."""

    duckdb_name: ClassVar[str] = "UINTEGER"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class UBigInt(Numeric):
    """DuckDB ``UBIGINT`` values."""

    duckdb_name: ClassVar[str] = "UBIGINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class USmallInt(Numeric):
    """DuckDB ``USMALLINT`` values."""

    duckdb_name: ClassVar[str] = "USMALLINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class UTinyInt(Numeric):
    """DuckDB ``UTINYINT`` values."""

    duckdb_name: ClassVar[str] = "UTINYINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class HugeInt(Numeric):
    """DuckDB ``HUGEINT`` values."""

    duckdb_name: ClassVar[str] = "HUGEINT"
    python_types: ClassVar[tuple[type[Any], ...]] = (int,)
    python_annotation: ClassVar[Any] = int


class Float(Numeric):
    """DuckDB ``FLOAT`` values."""

    duckdb_name: ClassVar[str] = "FLOAT"
    python_types: ClassVar[tuple[type[Any], ...]] = (float,)
    python_annotation: ClassVar[Any] = float


class Double(Numeric):
    """DuckDB ``DOUBLE`` values."""

    duckdb_name: ClassVar[str] = "DOUBLE"
    python_types: ClassVar[tuple[type[Any], ...]] = (float,)
    python_annotation: ClassVar[Any] = float


class Decimal(Numeric):
    """DuckDB ``DECIMAL`` values."""

    duckdb_name: ClassVar[str] = "DECIMAL"
    python_types: ClassVar[tuple[type[Any], ...]] = (DecimalValue,)
    python_annotation: ClassVar[Any] = DecimalValue


class Date(Temporal):
    """DuckDB ``DATE`` values."""

    duckdb_name: ClassVar[str] = "DATE"
    python_types: ClassVar[tuple[type[Any], ...]] = (date,)
    python_annotation: ClassVar[Any] = date


class Timestamp(Temporal):
    """DuckDB ``TIMESTAMP`` values."""

    duckdb_name: ClassVar[str] = "TIMESTAMP"
    python_types: ClassVar[tuple[type[Any], ...]] = (datetime,)
    python_annotation: ClassVar[Any] = datetime


class TimestampTz(Temporal):
    """DuckDB ``TIMESTAMPTZ`` values."""

    duckdb_name: ClassVar[str] = "TIMESTAMPTZ"
    python_types: ClassVar[tuple[type[Any], ...]] = (datetime,)
    python_annotation: ClassVar[Any] = datetime


class Time(Temporal):
    """DuckDB ``TIME`` values."""

    duckdb_name: ClassVar[str] = "TIME"
    python_types: ClassVar[tuple[type[Any], ...]] = (time,)
    python_annotation: ClassVar[Any] = time


class Interval(DuckType):
    """DuckDB ``INTERVAL`` values."""

    duckdb_name: ClassVar[str] = "INTERVAL"
    categories: ClassVar[frozenset[str]] = frozenset({"interval"})
    python_types: ClassVar[tuple[type[Any], ...]] = (timedelta,)
    python_annotation: ClassVar[Any] = timedelta


class Blob(DuckType):
    """DuckDB ``BLOB`` values."""

    duckdb_name: ClassVar[str] = "BLOB"
    python_types: ClassVar[tuple[type[Any], ...]] = (bytes, bytearray, memoryview)
    python_annotation: ClassVar[Any] = bytes | bytearray | memoryview


class Json(DuckType):
    """DuckDB ``JSON`` values."""

    duckdb_name: ClassVar[str] = "JSON"
    python_annotation: ClassVar[Any] = Any


class Varchar(StringLike):
    """DuckDB ``VARCHAR`` values."""

    duckdb_name: ClassVar[str] = "VARCHAR"
    python_types: ClassVar[tuple[type[Any], ...]] = (str,)
    python_annotation: ClassVar[Any] = str


def _all_subclasses(base: type[DuckType]) -> Iterable[type[DuckType]]:
    """Yield every :class:`DuckType` subclass recursively."""

    for subclass in base.__subclasses__():
        yield subclass
        yield from _all_subclasses(subclass)


_TYPE_INDEX = {
    subclass.duckdb_name.upper(): subclass
    for subclass in _all_subclasses(DuckType)
    if getattr(subclass, "duckdb_name", "ANY").upper() != "ANY"
}


def lookup(name: str) -> type[DuckType]:
    """Return the :class:`DuckType` for *name* falling back to :class:`Unknown`."""

    normalized = name.upper()
    return _TYPE_INDEX.get(normalized, Unknown)

