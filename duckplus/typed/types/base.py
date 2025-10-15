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
    def _key(self) -> Tuple[object, ...]:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:  # pragma: no cover - trivial wrapper
        if not isinstance(other, DuckDBType):
            return False
        return type(self) is type(other) and self._key() == other._key()

    def __hash__(self) -> int:  # pragma: no cover - trivial wrapper
        return hash((type(self), self._key()))

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

    def _key(self) -> Tuple[object, ...]:
        return (self.name,)


class GenericType(SimpleType):
    """Catch-all type when specific semantics are unknown."""

    __slots__ = ()


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


class IntegerType(NumericType):
    __slots__ = ()


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

    def _key(self) -> Tuple[object, ...]:
        return (self.precision, self.scale)


class UnknownType(DuckDBType):
    """Type placeholder when no metadata is available."""

    __slots__ = ()

    def render(self) -> str:
        return "UNKNOWN"

    def _key(self) -> Tuple[object, ...]:
        return ("UNKNOWN",)


def join_type_arguments(arguments: Iterable[DuckDBType]) -> str:
    """Render child types for parameterised types."""

    return ", ".join(argument.render() for argument in arguments)
