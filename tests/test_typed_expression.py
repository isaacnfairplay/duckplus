"""Unit tests for the typed expression sub-module."""

import pytest

from duckplus.typed import BooleanExpression, NumericExpression, ducktype


def test_numeric_column_carries_metadata() -> None:
    expression = ducktype.Numeric("total")
    assert isinstance(expression, NumericExpression)
    assert expression.render() == '"total"'
    assert expression.dependencies == {"total"}
    assert expression.type_annotation == "NUMERIC"


def test_numeric_aggregate_sum_uses_dependencies() -> None:
    expression = ducktype.Numeric.Aggregate.sum("sales")
    assert expression.render() == 'sum("sales")'
    assert expression.dependencies == {"sales"}


def test_varchar_equality_to_literal() -> None:
    expression = ducktype.Varchar("customer") == "prime"
    assert isinstance(expression, BooleanExpression)
    assert expression.render() == "(\"customer\" = 'prime')"
    assert expression.dependencies == {"customer"}


def test_boolean_composition_with_literals() -> None:
    predicate = ducktype.Boolean("is_active") & ducktype.Boolean.literal(True)
    assert predicate.render() == '("is_active" AND TRUE)'
    assert predicate.dependencies == {"is_active"}


def test_numeric_arithmetic_and_aliasing() -> None:
    expression = (ducktype.Numeric("subtotal") + 5).alias("order_total")
    assert expression.render() == '("subtotal" + 5) AS "order_total"'
    assert expression.dependencies == {"subtotal"}


def test_numeric_operand_validation() -> None:
    expression = ducktype.Numeric("price")
    with pytest.raises(TypeError) as error_info:
        _ = expression + "unexpected"
    assert "numeric" in str(error_info.value).lower()


def test_function_catalog_scalar_numeric_abs() -> None:
    expression = ducktype.Functions.Scalar.Numeric.abs(ducktype.Numeric.literal(-5))
    assert expression.render() == "abs(-5)"
    assert expression.type_annotation.upper() in {"TINYINT", "SMALLINT", "INTEGER"}

def test_function_catalog_boolean_namespace_handles_dependencies() -> None:
    expression = ducktype.Functions.Scalar.Boolean.starts_with(
        ducktype.Varchar("name"), ducktype.Varchar.literal("A")
    )
    assert expression.render() == "starts_with(\"name\", 'A')"
    assert expression.dependencies == {"name"}
    assert expression.type_annotation == "BOOLEAN"

def test_function_catalog_aggregate_numeric_sum_alias() -> None:
    expression = ducktype.Functions.Aggregate.Numeric.sum("revenue").alias("total")
    assert expression.render() == 'sum("revenue") AS "total"'
    assert expression.dependencies == {"revenue"}
