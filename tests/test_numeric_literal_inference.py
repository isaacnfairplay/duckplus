"""Tests covering numeric literal inference for typed expressions."""

from __future__ import annotations

from decimal import Decimal

import pytest

from duckplus.typed import ducktype
from duckplus.typed.types import DecimalType, IntegerType, NumericType, infer_numeric_literal_type


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "UTINYINT"),
        (255, "UTINYINT"),
        (256, "USMALLINT"),
        (65_535, "USMALLINT"),
        (65_536, "UINTEGER"),
        (4_294_967_295, "UINTEGER"),
        (4_294_967_296, "UBIGINT"),
        (18_446_744_073_709_551_615, "UBIGINT"),
        (18_446_744_073_709_551_616, "HUGEINT"),
        (1 << 127, "NUMERIC"),
    ],
)
def test_infer_numeric_literal_type_for_unsigned_integers(value: int, expected: str) -> None:
    duck_type = infer_numeric_literal_type(value)
    if expected == "NUMERIC":
        assert isinstance(duck_type, NumericType)
    else:
        assert isinstance(duck_type, IntegerType)
        assert duck_type.render() == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (-1, "TINYINT"),
        (-128, "TINYINT"),
        (-129, "SMALLINT"),
        (-32_768, "SMALLINT"),
        (-32_769, "INTEGER"),
        (-2_147_483_648, "INTEGER"),
        (-2_147_483_649, "BIGINT"),
        (-9_223_372_036_854_775_808, "BIGINT"),
        (-9_223_372_036_854_775_809, "HUGEINT"),
        (-(1 << 127), "HUGEINT"),
        (-(1 << 127) - 1, "NUMERIC"),
    ],
)
def test_infer_numeric_literal_type_for_signed_integers(value: int, expected: str) -> None:
    duck_type = infer_numeric_literal_type(value)
    if expected == "NUMERIC":
        assert isinstance(duck_type, NumericType)
    else:
        assert isinstance(duck_type, IntegerType)
        assert duck_type.render() == expected


def test_infer_numeric_literal_type_for_float() -> None:
    duck_type = infer_numeric_literal_type(3.14)
    assert duck_type.render() == "DOUBLE"


@pytest.mark.parametrize(
    ("value", "precision", "scale"),
    [
        (Decimal("1"), 1, 0),
        (Decimal("12.34"), 4, 2),
        (Decimal("0.001"), 1, 3),
        (Decimal("-123456789012345678901234567890.1234"), 34, 4),
    ],
)
def test_infer_numeric_literal_type_for_decimal(
    value: Decimal, precision: int, scale: int
) -> None:
    duck_type = infer_numeric_literal_type(value)
    assert isinstance(duck_type, DecimalType)
    assert duck_type.precision == precision
    assert duck_type.scale == scale


def test_numeric_expression_literal_uses_inferred_type() -> None:
    expression = ducktype.Numeric.literal(42)
    assert expression.duck_type.render() == "UTINYINT"


def test_numeric_expression_literal_decimal_precision() -> None:
    expression = ducktype.Numeric.literal(Decimal("12.340"))
    assert isinstance(expression.duck_type, DecimalType)
    assert expression.duck_type.precision == 4
    assert expression.duck_type.scale == 2


def test_infer_numeric_literal_type_rejects_boolean() -> None:
    with pytest.raises(TypeError):
        infer_numeric_literal_type(True)
