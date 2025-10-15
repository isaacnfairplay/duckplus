"""Unit tests for the typed expression sub-module."""

from decimal import Decimal

import pytest

from duckplus.typed import BooleanExpression, GenericExpression, NumericExpression, ducktype


def test_numeric_column_carries_metadata() -> None:
    expression = ducktype.Numeric("total")
    assert isinstance(expression, NumericExpression)
    assert expression.render() == '"total"'
    assert expression.dependencies == {"total"}
    assert expression.duck_type.render() == "NUMERIC"


def test_numeric_aggregate_sum_uses_dependencies() -> None:
    expression = ducktype.Numeric.Aggregate.sum("sales")
    assert expression.render() == 'sum("sales")'
    assert expression.dependencies == {"sales"}


def test_varchar_equality_to_literal() -> None:
    expression = ducktype.Varchar("customer") == "prime"
    assert isinstance(expression, BooleanExpression)
    assert expression.render() == "(\"customer\" = 'prime')"
    assert expression.dependencies == {"customer"}


def test_numeric_equality_to_decimal_literal() -> None:
    expression = ducktype.Numeric("balance") == Decimal("12.50")
    assert isinstance(expression, BooleanExpression)
    assert expression.render() == '("balance" = 12.50)'
    assert expression.dependencies == {"balance"}


def test_boolean_composition_with_literals() -> None:
    predicate = ducktype.Boolean("is_active") & ducktype.Boolean.literal(True)
    assert predicate.render() == '("is_active" AND TRUE)'
    assert predicate.dependencies == {"is_active"}


def test_numeric_arithmetic_and_aliasing() -> None:
    expression = (ducktype.Numeric("subtotal") + 5).alias("order_total")
    assert expression.render() == '("subtotal" + 5) AS "order_total"'
    assert expression.dependencies == {"subtotal"}


def test_varchar_concatenation_with_literal() -> None:
    expression = ducktype.Varchar("first_name") + " "
    assert expression.render() == "(\"first_name\" || ' ')"
    assert expression.dependencies == {"first_name"}


def test_varchar_right_concatenation_literal() -> None:
    expression = "Hello, " + ducktype.Varchar("name")
    assert expression.render() == "('Hello, ' || \"name\")"
    assert expression.dependencies == {"name"}


def test_numeric_operand_validation() -> None:
    expression = ducktype.Numeric("price")
    with pytest.raises(TypeError) as error_info:
        _ = expression + "unexpected"
    assert "numeric" in str(error_info.value).lower()


def test_function_catalog_scalar_numeric_abs() -> None:
    expression = ducktype.Functions.Scalar.Numeric.abs(ducktype.Numeric.literal(-5))
    assert expression.render() == "abs(-5)"
    assert expression.duck_type.render().upper() in {"TINYINT", "SMALLINT", "INTEGER"}

def test_function_catalog_boolean_namespace_handles_dependencies() -> None:
    expression = ducktype.Functions.Scalar.Boolean.starts_with(
        ducktype.Varchar("name"), "A"
    )
    assert expression.render() == "starts_with(\"name\", 'A')"
    assert expression.dependencies == {"name"}
    assert expression.duck_type.render() == "BOOLEAN"

def test_function_catalog_numeric_pow_accepts_literal_exponent() -> None:
    expression = ducktype.Functions.Scalar.Numeric.pow(
        ducktype.Numeric("base"), 2
    )
    assert expression.render() == 'pow("base", 2)'
    assert expression.dependencies == {"base"}

def test_function_catalog_aggregate_numeric_sum_alias() -> None:
    expression = ducktype.Functions.Aggregate.Numeric.sum("revenue").alias("total")
    assert expression.render() == 'sum("revenue") AS "total"'
    assert expression.dependencies == {"revenue"}


def test_numeric_expression_method_sum() -> None:
    aggregated = ducktype.Numeric("amount").sum()
    assert isinstance(aggregated, NumericExpression)
    assert aggregated.render() == 'sum("amount")'
    assert aggregated.dependencies == {"amount"}


def test_generic_expression_lacks_sum_method() -> None:
    customer = ducktype.Generic("customer")
    assert isinstance(customer, GenericExpression)
    with pytest.raises(AttributeError):
        customer.sum()  # type: ignore[attr-defined]


def test_generic_max_by_accepts_numeric() -> None:
    winner = ducktype.Generic("customer").max_by(ducktype.Numeric("score"))
    assert "max_by" in winner.render()
    assert winner.dependencies == {"customer", "score"}
