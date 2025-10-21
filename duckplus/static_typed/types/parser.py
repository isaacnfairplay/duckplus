"""Parser translating DuckDB type strings into :mod:`duckplus.static_typed.types`."""

# pylint: disable=too-many-return-statements,missing-function-docstring,too-few-public-methods,duplicate-code

from __future__ import annotations

from typing import Iterable

from .base import (
    BlobType,
    BooleanType,
    DecimalType,
    DuckDBType,
    FloatingType,
    GenericType,
    IdentifierType,
    IntegerType,
    IntervalType,
    NumericType,
    TemporalType,
    UnknownType,
    VarcharType,
)
from .collections import ArrayType, EnumType, ListType, MapType, StructField, StructType, UnionType

_BOOLEAN_ALIASES = ("BOOLEAN", "BOOL", "LOGICAL")
_BLOB_ALIASES = ("BLOB", "BYTEA", "VARBINARY")
_VARCHAR_ALIASES = ("VARCHAR", "STRING", "TEXT", "JSON", "UUID")
_UNSIGNED_INTEGER_ALIASES = ("UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")
_SIGNED_INTEGER_ALIASES = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT")
_FLOATING_ALIASES = ("FLOAT", "REAL", "DOUBLE")
_NUMERIC_ALIASES = ("NUMERIC", "DECIMAL", "BIT", "VARBIT")
_TEMPORAL_ALIASES = ("DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ")


def _boolean_type(name: str) -> DuckDBType | None:
    if name in _BOOLEAN_ALIASES:
        return BooleanType(name)
    return None


def _blob_type(name: str) -> DuckDBType | None:
    if name in _BLOB_ALIASES:
        return BlobType(name)
    return None


def _varchar_type(name: str) -> DuckDBType | None:
    if name in _VARCHAR_ALIASES:
        return VarcharType(name)
    return None


def _integer_type(name: str) -> DuckDBType | None:
    if name in _UNSIGNED_INTEGER_ALIASES or name in _SIGNED_INTEGER_ALIASES:
        return IntegerType(name)
    return None


def _floating_type(name: str) -> DuckDBType | None:
    if name in _FLOATING_ALIASES:
        return FloatingType(name)
    return None


def _interval_type(name: str) -> DuckDBType | None:
    if name == "INTERVAL":
        return IntervalType("INTERVAL")
    return None


def _numeric_type(name: str) -> DuckDBType | None:
    if name in _NUMERIC_ALIASES:
        return NumericType(name)
    return None


def _temporal_type(name: str) -> DuckDBType | None:
    if name in _TEMPORAL_ALIASES:
        return TemporalType(name)
    if name == "TIMESTAMP WITH TIME ZONE":
        return TemporalType("TIMESTAMPTZ")
    if name == "TIME WITH TIME ZONE":
        return TemporalType("TIMETZ")
    return None


def _generic_alias_type(name: str) -> DuckDBType | None:
    if name == "ANY":
        return GenericType("ANY")
    return None


def _unknown_type(name: str) -> DuckDBType | None:
    if name == "UNKNOWN":
        return UnknownType()
    return None


def _identifier_type(name: str) -> DuckDBType | None:
    if name == "IDENTIFIER":
        return IdentifierType("IDENTIFIER")
    return None


def _build_simple_type(name: str) -> DuckDBType | None:
    for factory in (
        _boolean_type,
        _blob_type,
        _varchar_type,
        _integer_type,
        _floating_type,
        _interval_type,
        _numeric_type,
        _temporal_type,
        _generic_alias_type,
        _unknown_type,
        _identifier_type,
    ):
        result = factory(name)
        if result is not None:
            return result
    return None


def parse_type(type_spec: str | None) -> DuckDBType | None:
    """Parse ``type_spec`` into a :class:`DuckDBType` hierarchy."""

    if type_spec is None:
        return None
    parser = _TypeParser(type_spec)
    return parser.parse()


class _TypeParser:
    __slots__ = ("_text", "_length", "_position")

    def __init__(self, text: str) -> None:
        self._text = text.strip()
        self._length = len(self._text)
        self._position = 0

    def parse(self) -> DuckDBType:
        self._skip_whitespace()
        if self._position >= self._length:
            return UnknownType()
        name = self._parse_identifier()
        upper = name.upper()
        # Handle multi-word temporal types such as "TIMESTAMP WITH TIME ZONE".
        if upper in {"TIMESTAMP", "TIME"}:
            checkpoint = self._position
            self._skip_whitespace()
            suffix = self._text[self._position :].upper()
            if suffix.startswith("WITH TIME ZONE"):
                self._position += len("WITH TIME ZONE")
                upper = f"{upper} WITH TIME ZONE"
            else:
                self._position = checkpoint
        if self._peek() == "(":
            result = self._parse_parameterised(upper)
        else:
            simple_type = _build_simple_type(upper)
            if simple_type is None:
                result = GenericType(upper)
            else:
                result = simple_type
        return self._apply_array_suffix(result)

    # Parsing helpers -------------------------------------------------
    def _parse_parameterised(self, name: str) -> DuckDBType:
        self._consume("(")
        if name in {"DECIMAL", "NUMERIC"}:
            precision = self._parse_integer()
            self._consume(",")
            scale = self._parse_integer()
            self._consume(")")
            return DecimalType(precision, scale)
        if name == "LIST":
            element_type = self.parse()
            self._consume(")")
            return ListType(element_type)
        if name == "ARRAY":
            element_type = self.parse()
            length = None
            if self._peek() == ",":
                self._consume(",")
                length = self._parse_integer()
            self._consume(")")
            return ArrayType(element_type, length)
        if name == "MAP":
            key_type = self.parse()
            self._consume(",")
            value_type = self.parse()
            self._consume(")")
            return MapType(key_type, value_type)
        if name == "UNION":
            options = list(self._parse_type_list())
            self._consume(")")
            return UnionType(options)
        if name == "STRUCT":
            checkpoint = self._position
            try:
                fields = list(self._parse_struct_fields())
            except ValueError:
                self._position = checkpoint
            else:
                self._consume(")")
                return StructType(fields)
        if name == "ENUM":
            values = list(self._parse_enum_values())
            self._consume(")")
            return EnumType(values)
        # Fallback to treat nested parameters as child types
        arguments = list(self._parse_type_list())
        self._consume(")")
        if name == "TABLE":
            return GenericType(f"TABLE({', '.join(arg.render() for arg in arguments)})")
        return GenericType(f"{name}({', '.join(arg.render() for arg in arguments)})")

    def _apply_array_suffix(self, base: DuckDBType) -> DuckDBType:
        while self._peek() == "[":
            self._consume("[")
            length = None
            if self._peek() == "]":
                self._consume("]")
                base = ArrayType(base, length)
                continue
            if self._peek().isalpha():
                token = self._parse_identifier()
                self._consume("]")
                return GenericType(f"{base.render()}[{token}]")
            length = self._parse_integer()
            self._consume("]")
            base = ArrayType(base, length)
        return base

    def _parse_type_list(self) -> Iterable[DuckDBType]:
        first = True
        while True:
            self._skip_whitespace()
            if not first and self._peek() != ",":
                break
            if not first:
                self._consume(",")
            self._skip_whitespace()
            if self._peek() == ")":
                break
            yield self.parse()
            first = False
        self._skip_whitespace()

    def _parse_struct_fields(self) -> Iterable[StructField]:
        first = True
        while True:
            self._skip_whitespace()
            if self._peek() == ")":
                break
            if not first:
                self._consume(",")
                self._skip_whitespace()
            field_name = self._parse_identifier(allow_quoted=True)
            field_type = self.parse()
            yield StructField(field_name, field_type)
            first = False
            self._skip_whitespace()

    def _parse_enum_values(self) -> Iterable[str]:
        first = True
        while True:
            self._skip_whitespace()
            if self._peek() == ")":
                break
            if not first:
                self._consume(",")
                self._skip_whitespace()
            yield self._parse_string_literal()
            first = False
            self._skip_whitespace()

    def _parse_identifier(self, *, allow_quoted: bool = False) -> str:
        self._skip_whitespace()
        if allow_quoted and self._peek() == '"':
            return self._parse_quoted_identifier()
        start = self._position
        while self._position < self._length and self._text[self._position] not in "(),' []":
            self._position += 1
        identifier = self._text[start:self._position].strip()
        if not identifier and allow_quoted:
            return self._parse_quoted_identifier()
        if not identifier:
            raise ValueError("Expected identifier while parsing DuckDB type")
        return identifier

    def _parse_quoted_identifier(self) -> str:
        self._consume('"')
        start = self._position
        while self._position < self._length:
            char = self._text[self._position]
            if char == '"':
                if self._peek(offset=1) == '"':
                    self._position += 2
                    continue
                break
            self._position += 1
        identifier = self._text[start:self._position]
        self._consume('"')
        return identifier.replace('""', '"')

    def _parse_integer(self) -> int:
        self._skip_whitespace()
        start = self._position
        while self._position < self._length and self._text[self._position].isdigit():
            self._position += 1
        if start == self._position:
            raise ValueError("Expected integer while parsing DuckDB type")
        return int(self._text[start:self._position])

    def _parse_string_literal(self) -> str:
        self._skip_whitespace()
        if self._peek() != "'":
            raise ValueError("Expected string literal while parsing ENUM values")
        self._consume("'")
        value_chars: list[str] = []
        while self._position < self._length:
            char = self._text[self._position]
            if char == "'":
                if self._peek(offset=1) == "'":
                    value_chars.append("'")
                    self._position += 2
                    continue
                break
            value_chars.append(char)
            self._position += 1
        self._consume("'")
        return "".join(value_chars)

    def _skip_whitespace(self) -> None:
        while self._position < self._length and self._text[self._position].isspace():
            self._position += 1

    def _peek(self, offset: int = 0) -> str:
        position = self._position + offset
        if position >= self._length:
            return ""
        return self._text[position]

    def _consume(self, expected: str) -> None:
        self._skip_whitespace()
        if not expected:
            return
        if self._text[self._position : self._position + len(expected)] != expected:
            raise ValueError(f"Expected '{expected}' while parsing DuckDB type")
        self._position += len(expected)
        self._skip_whitespace()
