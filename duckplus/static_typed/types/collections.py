"""Nested DuckDB type structures (lists, maps, structs, ...)."""

# pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods

from __future__ import annotations

from typing import Iterable, Sequence, Tuple

from .base import DuckDBType, join_type_arguments


class ListType(DuckDBType):
    __slots__ = ("element_type",)

    def __init__(self, element_type: DuckDBType) -> None:
        self.element_type = element_type

    def render(self) -> str:
        return f"LIST({self.element_type.render()})"

    def describe(self) -> str:
        return f"LIST OF {self.element_type.describe()}"

    def key(self) -> Tuple[object, ...]:
        return (self.element_type,)


class ArrayType(DuckDBType):
    __slots__ = ("element_type", "length")

    def __init__(self, element_type: DuckDBType, length: int | None = None) -> None:
        self.element_type = element_type
        self.length = length

    def render(self) -> str:
        if self.length is None:
            return f"ARRAY({self.element_type.render()})"
        return f"ARRAY({self.element_type.render()}, {self.length})"

    def describe(self) -> str:
        size = "variable" if self.length is None else str(self.length)
        return f"ARRAY[{size}] OF {self.element_type.describe()}"

    def key(self) -> Tuple[object, ...]:
        return (self.element_type, self.length)


class MapType(DuckDBType):
    __slots__ = ("key_type", "value_type")

    def __init__(self, key_type: DuckDBType, value_type: DuckDBType) -> None:
        self.key_type = key_type
        self.value_type = value_type

    def render(self) -> str:
        return f"MAP({self.key_type.render()}, {self.value_type.render()})"

    def describe(self) -> str:
        return f"MAP[{self.key_type.describe()} -> {self.value_type.describe()}]"

    def key(self) -> Tuple[object, ...]:
        return (self.key_type, self.value_type)


class UnionType(DuckDBType):
    __slots__ = ("options",)

    def __init__(self, options: Sequence[DuckDBType]) -> None:
        self.options = tuple(options)

    def render(self) -> str:
        return f"UNION({join_type_arguments(self.options)})"

    def describe(self) -> str:
        option_descriptions = ", ".join(option.describe() for option in self.options)
        return f"UNION[{option_descriptions}]"

    def key(self) -> Tuple[object, ...]:
        return self.options


class StructField:
    """Field definition used by :class:`StructType`."""

    __slots__ = ("name", "type")

    def __init__(self, name: str, field_type: DuckDBType) -> None:
        self.name = name
        self.type = field_type

    def render(self) -> str:
        return f"{self.name} {self.type.render()}"

    def describe(self) -> str:
        return f"{self.name}: {self.type.describe()}"

    def key(self) -> Tuple[object, ...]:
        return (self.name, self.type)

    def __eq__(self, other: object) -> bool:  # pragma: no cover - trivial wrapper
        return isinstance(other, StructField) and self.key() == other.key()

    def __hash__(self) -> int:  # pragma: no cover - trivial wrapper
        return hash(self.key())

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"StructField({self.name!r}, {self.type!r})"


class StructType(DuckDBType):
    __slots__ = ("fields",)

    def __init__(self, fields: Iterable[StructField]) -> None:
        self.fields = tuple(fields)

    def render(self) -> str:
        if not self.fields:
            return "STRUCT()"
        joined = ", ".join(field.render() for field in self.fields)
        return f"STRUCT({joined})"

    def describe(self) -> str:
        if not self.fields:
            return "STRUCT<>"
        joined = ", ".join(field.describe() for field in self.fields)
        return f"STRUCT<{joined}>"

    def key(self) -> Tuple[object, ...]:
        return self.fields


class EnumType(DuckDBType):
    __slots__ = ("values",)

    def __init__(self, values: Sequence[str]) -> None:
        self.values = tuple(values)

    def render(self) -> str:
        quoted = ", ".join(repr(value) for value in self.values)
        return f"ENUM({quoted})"

    def describe(self) -> str:
        return f"ENUM[{', '.join(self.values)}]"

    def key(self) -> Tuple[object, ...]:
        return self.values
