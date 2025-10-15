"""Helpers for inferring :mod:`duckplus.typed.types` from Python values."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

from .base import DecimalType, DuckDBType, FloatingType, IntegerType, NumericType

# pylint: disable=too-many-return-statements

_UTINYINT_MAX = 255
_USMALLINT_MAX = 65_535
_UINTEGER_MAX = 4_294_967_295
_UBIGINT_MAX = 18_446_744_073_709_551_615

_TINYINT_MIN = -128
_TINYINT_MAX = 127
_SMALLINT_MIN = -32_768
_SMALLINT_MAX = 32_767
_INTEGER_MIN = -2_147_483_648
_INTEGER_MAX = 2_147_483_647
_BIGINT_MIN = -9_223_372_036_854_775_808
_BIGINT_MAX = 9_223_372_036_854_775_807

_HUGEINT_MIN = -(1 << 127)
_HUGEINT_MAX = (1 << 127) - 1


def _infer_integer_type(value: int) -> DuckDBType:
    if value < 0:
        if value >= _TINYINT_MIN:
            return IntegerType("TINYINT")
        if value >= _SMALLINT_MIN:
            return IntegerType("SMALLINT")
        if value >= _INTEGER_MIN:
            return IntegerType("INTEGER")
        if value >= _BIGINT_MIN:
            return IntegerType("BIGINT")
        if value >= _HUGEINT_MIN:
            return IntegerType("HUGEINT")
        return NumericType("NUMERIC")
    if value <= _UTINYINT_MAX:
        return IntegerType("UTINYINT")
    if value <= _USMALLINT_MAX:
        return IntegerType("USMALLINT")
    if value <= _UINTEGER_MAX:
        return IntegerType("UINTEGER")
    if value <= _UBIGINT_MAX:
        return IntegerType("UBIGINT")
    if value <= _HUGEINT_MAX:
        return IntegerType("HUGEINT")
    return NumericType("NUMERIC")


def _infer_decimal_type(value: Decimal) -> DuckDBType:
    _sign, digits, exponent = value.as_tuple()
    exponent = cast(int, exponent)
    digits_list = [cast(int, digit) for digit in digits]
    if exponent < 0:
        while digits_list and digits_list[-1] == 0:
            digits_list.pop()
            exponent += 1
    if not digits_list:
        digits_list = [0]
    precision = len(digits_list)
    if exponent > 0:
        precision += exponent
        scale = 0
    else:
        scale = -exponent
    # Avoid absurd precision for repeating decimals by capping at DuckDB's max (38).
    precision = min(precision, 38)
    scale = min(scale, 38)
    return DecimalType(precision, scale)


def infer_numeric_literal_type(value: int | float | Decimal) -> DuckDBType:
    """Return the tightest DuckDB numeric type that can represent ``value``."""

    if isinstance(value, bool):  # bool is a subclass of ``int``
        raise TypeError("Boolean values are not valid numeric literals")
    if isinstance(value, Decimal):
        return _infer_decimal_type(value)
    if isinstance(value, float):
        return FloatingType("DOUBLE")
    return _infer_integer_type(int(value))


__all__ = ["infer_numeric_literal_type"]
