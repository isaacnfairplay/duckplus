"""DuckDB type hierarchy used to annotate typed expressions."""

# pylint: disable=missing-class-docstring,too-few-public-methods

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Tuple


class DuckDBType(ABC):
    """Abstract base class for DuckDB types."""

    __slots__ = ()
    category: str = "generic"

    def render(self) -> str:
        """Return the canonical SQL rendering of the type."""

        raise NotImplementedError

    def describe(self) -> str:
        """Return a developer friendly description for repr/docstrings."""

        return self.render()

    @abstractmethod
    def key(self) -> Tuple[object, ...]:
        raise NotImplementedError

    def accepts(self, candidate: "DuckDBType") -> bool:
        """Return whether ``candidate`` can be used where this type is expected."""

        return type(self) is type(candidate) and self.key() == candidate.key()

    def __eq__(self, other: object) -> bool:  # pragma: no cover - trivial wrapper
        if not isinstance(other, DuckDBType):
            return False
        return type(self) is type(other) and self.key() == other.key()

    def __hash__(self) -> int:  # pragma: no cover - trivial wrapper
        return hash((type(self), self.key()))

    def __str__(self) -> str:  # pragma: no cover - trivial wrapper
        return self.render()

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"{self.__class__.__name__}({self.describe()!r})"


class SimpleType(DuckDBType):
    """DuckDB type with no parameters (e.g. INTEGER, BOOLEAN)."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name.upper()

    def render(self) -> str:
        return self.name

    def key(self) -> Tuple[object, ...]:
        return (self.name,)


class GenericType(SimpleType):
    """Catch-all type when specific semantics are unknown."""

    __slots__ = ()

    def accepts(self, candidate: DuckDBType) -> bool:
        return True


class BooleanType(SimpleType):
    __slots__ = ()
    category = "boolean"


class VarcharType(SimpleType):
    __slots__ = ()
    category = "varchar"


class BlobType(SimpleType):
    __slots__ = ()
    category = "blob"


class NumericType(SimpleType):
    __slots__ = ()
    category = "numeric"

    def accepts(self, candidate: DuckDBType) -> bool:
        if isinstance(candidate, NumericType):
            if self.name == "NUMERIC":
                return True
        return super().accepts(candidate)


class IntegerType(NumericType):
    __slots__ = ()

    def accepts(self, candidate: DuckDBType) -> bool:
        if isinstance(candidate, NumericType) and getattr(candidate, "name", None) == "NUMERIC":
            return True
        if isinstance(candidate, IntegerType):
            if _integer_family(self.name) == _integer_family(candidate.name):
                candidate_rank = _integer_rank(candidate.name)
                expected_rank = _integer_rank(self.name)
                if candidate_rank is not None and expected_rank is not None:
                    return candidate_rank <= expected_rank
        if isinstance(candidate, UnknownType):
            return True
        return super().accepts(candidate)


class FloatingType(NumericType):
    __slots__ = ()


class IntervalType(NumericType):
    __slots__ = ()


class TemporalType(SimpleType):
    __slots__ = ()


class IdentifierType(SimpleType):
    __slots__ = ()
    category = "identifier"


class DecimalType(NumericType):
    """DuckDB DECIMAL/NUMERIC type."""

    __slots__ = ("precision", "scale")

    def __init__(self, precision: int, scale: int) -> None:
        super().__init__("DECIMAL")
        self.precision = precision
        self.scale = scale

    def render(self) -> str:
        return f"DECIMAL({self.precision}, {self.scale})"

    def key(self) -> Tuple[object, ...]:
        return (self.precision, self.scale)


_UNSIGNED_INTEGER_ORDER = ("UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT")
_SIGNED_INTEGER_ORDER = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT")
_UNSIGNED_INTEGER_RANK = {name: index for index, name in enumerate(_UNSIGNED_INTEGER_ORDER)}
_SIGNED_INTEGER_RANK = {name: index for index, name in enumerate(_SIGNED_INTEGER_ORDER)}


def _integer_family(name: str) -> str | None:
    if name in _UNSIGNED_INTEGER_RANK:
        return "unsigned"
    if name in _SIGNED_INTEGER_RANK:
        return "signed"
    return None


def _integer_rank(name: str) -> int | None:
    if name in _UNSIGNED_INTEGER_RANK:
        return _UNSIGNED_INTEGER_RANK[name]
    if name in _SIGNED_INTEGER_RANK:
        return _SIGNED_INTEGER_RANK[name]
    return None


class UnknownType(DuckDBType):
    """Type placeholder when no metadata is available."""

    __slots__ = ()

    def render(self) -> str:
        return "UNKNOWN"

    def key(self) -> Tuple[object, ...]:
        return ("UNKNOWN",)

    def accepts(self, candidate: DuckDBType) -> bool:
        return True


def join_type_arguments(arguments: Iterable[DuckDBType]) -> str:
    """Render child types for parameterised types."""

    return ", ".join(argument.render() for argument in arguments)
