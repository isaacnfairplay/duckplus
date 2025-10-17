"""Unit tests for the typed expression sub-module."""

from decimal import Decimal

import pytest

from duckplus.typed import (
    BooleanExpression,
    ExpressionDependency,
    GenericExpression,
    NumericExpression,
    ducktype,
)
from duckplus.typed.functions import (
    DuckDBFunctionDefinition,
    _coerce_operands_for_overload,
)
from duckplus.typed.types import parse_type


def col_dep(name: str, *, table: str | None = None) -> ExpressionDependency:
    return ExpressionDependency.column(name, table=table)


def table_dep(name: str) -> ExpressionDependency:
    return ExpressionDependency.table(name)


def test_numeric_column_carries_metadata() -> None:
    expression = ducktype.Numeric("total")
    assert isinstance(expression, NumericExpression)
    assert expression.render() == '"total"'
    assert expression.dependencies == {col_dep('total')}
    assert expression.duck_type.render() == "NUMERIC"


def test_numeric_column_with_table_dependency() -> None:
    expression = ducktype.Numeric("total", table="orders")
    assert expression.render() == '"orders"."total"'
    assert expression.dependencies == {col_dep('total', table='orders')}


def test_raw_expression_allows_table_dependency() -> None:
    expression = ducktype.Numeric.raw(
        "count(*)",
        dependencies=[("orders", None)],
    )
    assert expression.dependencies == {table_dep('orders')}


def test_numeric_aggregate_sum_uses_dependencies() -> None:
    expression = ducktype.Numeric.Aggregate.sum("sales")
    assert expression.render() == 'sum("sales")'
    assert expression.dependencies == {col_dep('sales')}


def test_varchar_equality_to_literal() -> None:
    expression = ducktype.Varchar("customer") == "prime"
    assert isinstance(expression, BooleanExpression)
    assert expression.render() == "(\"customer\" = 'prime')"
    assert expression.dependencies == {col_dep('customer')}


def test_numeric_equality_to_decimal_literal() -> None:
    expression = ducktype.Numeric("balance") == Decimal("12.50")
    assert isinstance(expression, BooleanExpression)
    assert expression.render() == '("balance" = 12.50)'
    assert expression.dependencies == {col_dep('balance')}


def test_boolean_composition_with_literals() -> None:
    predicate = ducktype.Boolean("is_active") & ducktype.Boolean.literal(True)
    assert predicate.render() == '("is_active" AND TRUE)'
    assert predicate.dependencies == {col_dep('is_active')}


def test_numeric_arithmetic_and_aliasing() -> None:
    expression = (ducktype.Numeric("subtotal") + 5).alias("order_total")
    assert expression.render() == '("subtotal" + 5) AS "order_total"'
    assert expression.dependencies == {col_dep('subtotal')}


def test_varchar_concatenation_with_literal() -> None:
    expression = ducktype.Varchar("first_name") + " "
    assert expression.render() == "(\"first_name\" || ' ')"
    assert expression.dependencies == {col_dep('first_name')}


def test_varchar_right_concatenation_literal() -> None:
    expression = "Hello, " + ducktype.Varchar("name")
    assert expression.render() == "('Hello, ' || \"name\")"
    assert expression.dependencies == {col_dep('name')}


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
    assert expression.dependencies == {col_dep('name')}
    assert expression.duck_type.render() == "BOOLEAN"

def test_function_catalog_numeric_pow_accepts_literal_exponent() -> None:
    expression = ducktype.Functions.Scalar.Numeric.pow(
        ducktype.Numeric("base"), 2
    )
    assert expression.render() == 'pow("base", 2)'
    assert expression.dependencies == {col_dep('base')}

def test_function_catalog_aggregate_numeric_sum_alias() -> None:
    expression = ducktype.Functions.Aggregate.Numeric.sum("revenue").alias("total")
    assert expression.render() == 'sum("revenue") AS "total"'
    assert expression.dependencies == {col_dep('revenue')}


def _build_overload(*parameter_types: str) -> DuckDBFunctionDefinition:
    return DuckDBFunctionDefinition(
        schema_name="main",
        function_name="test",
        function_type="scalar",
        return_type=None,
        parameter_types=tuple(parse_type(param) for param in parameter_types),
        parameters=tuple(f"arg{i}" for i in range(len(parameter_types))),
        varargs=None,
        description=None,
        comment=None,
        macro_definition=None,
    )


def test_function_argument_accepts_narrower_unsigned_integer() -> None:
    overload = _build_overload("UINTEGER")
    operand = ducktype.Numeric.literal(5)
    coerced = _coerce_operands_for_overload((operand,), overload)
    assert coerced == [operand]


def test_function_argument_accepts_generic_numeric_for_integer() -> None:
    overload = _build_overload("UTINYINT")
    operand = ducktype.Numeric("value")
    coerced = _coerce_operands_for_overload((operand,), overload)
    assert coerced == [operand]


def test_function_argument_rejects_wider_integer() -> None:
    overload = _build_overload("UTINYINT")
    operand = ducktype.Numeric.literal(512)
    with pytest.raises(TypeError):
        _coerce_operands_for_overload((operand,), overload)


def test_function_argument_accepts_narrower_signed_integer() -> None:
    overload = _build_overload("INTEGER")
    operand = ducktype.Numeric.literal(-5)
    coerced = _coerce_operands_for_overload((operand,), overload)
    assert coerced == [operand]


def test_numeric_expression_method_sum() -> None:
    aggregated = ducktype.Numeric("amount").sum()
    assert isinstance(aggregated, NumericExpression)
    assert aggregated.render() == 'sum("amount")'
    assert aggregated.dependencies == {col_dep('amount')}


def test_generic_expression_lacks_sum_method() -> None:
    customer = ducktype.Generic("customer")
    assert isinstance(customer, GenericExpression)
    with pytest.raises(AttributeError):
        customer.sum()  # type: ignore[attr-defined]


def test_generic_max_by_accepts_numeric() -> None:
    winner = ducktype.Generic("customer").max_by(ducktype.Numeric("score"))
    assert "max_by" in winner.render()
    assert winner.dependencies == {col_dep('customer'), col_dep('score')}


def test_window_over_renders_partition_and_order_clauses() -> None:
    base = ducktype.Numeric("amount").sum()
    windowed = base.over(
        partition_by=["customer"],
        order_by=[(ducktype.Numeric("order_date"), "DESC")],
    )
    assert (
        windowed.render()
        == '(sum("amount") OVER (PARTITION BY "customer" ORDER BY "order_date" DESC))'
    )
    assert windowed.dependencies == {col_dep('amount'), col_dep('customer'), col_dep('order_date')}


def test_window_over_supports_frame_clauses() -> None:
    windowed = ducktype.Numeric("amount").sum().over(
        order_by=["event_time"],
        frame="ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
    )
    assert (
        windowed.render()
        == '(sum("amount") OVER (ORDER BY "event_time" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW))'
    )
    assert windowed.dependencies == {col_dep('amount'), col_dep('event_time')}


def test_window_over_preserves_aliasing() -> None:
    windowed = (
        ducktype.Numeric("amount")
        .sum()
        .alias("running_total")
        .over(partition_by=["customer"])
    )
    assert (
        windowed.render()
        == '(sum("amount") OVER (PARTITION BY "customer")) AS "running_total"'
    )
    assert windowed.dependencies == {col_dep('amount'), col_dep('customer')}


def test_window_over_validates_direction() -> None:
    with pytest.raises(ValueError):
        ducktype.Numeric("amount").sum().over(order_by=[("order_date", "sideways")])


def test_window_over_rejects_empty_frame_clause() -> None:
    with pytest.raises(ValueError):
        ducktype.Numeric("amount").sum().over(frame="   ")


def test_numeric_case_expression_renders_sql() -> None:
    expression = (
        ducktype.Numeric.case()
        .when(ducktype.Varchar("status") == "active", 1)
        .when(ducktype.Varchar("status") == "inactive", 0)
        .else_(ducktype.Numeric.literal(-1))
        .end()
    )
    assert (
        expression.render()
        == "CASE WHEN (\"status\" = 'active') THEN 1 "
        "WHEN (\"status\" = 'inactive') THEN 0 ELSE -1 END"
    )
    assert expression.dependencies == {col_dep('status')}


def test_case_expression_supports_nested_builders() -> None:
    fallback = (
        ducktype.Varchar.case()
        .when(True, "fallback")
        .else_("unknown")
        .end()
    )
    expression = (
        ducktype.Varchar.case()
        .when(ducktype.Boolean("is_internal"), "internal")
        .else_(fallback)
        .end()
    )
    assert (
        expression.render()
        == "CASE WHEN \"is_internal\" THEN 'internal' ELSE "
        "CASE WHEN TRUE THEN 'fallback' ELSE 'unknown' END END"
    )
    assert expression.dependencies == {col_dep('is_internal')}


def test_case_expression_requires_when_clause() -> None:
    builder = ducktype.Numeric.case()
    with pytest.raises(ValueError):
        builder.end()


def test_case_expression_rejects_multiple_else_clauses() -> None:
    builder = ducktype.Numeric.case().when(True, 1).else_(0)
    with pytest.raises(ValueError):
        builder.else_(2)


def test_select_builder_renders_sql_statement() -> None:
    statement = (
        ducktype.select()
        .column(ducktype.Numeric("amount"))
        .column(ducktype.Numeric("amount").sum().alias("total"))
        .column("CURRENT_DATE", alias="today")
        .from_("orders")
        .build()
    )
    assert (
        statement
        == 'SELECT "amount", sum("amount") AS "total", CURRENT_DATE AS "today" '
        "FROM orders"
    )


def test_select_builder_allows_alias_override() -> None:
    expression = ducktype.Numeric("amount").sum().alias("total")
    statement = ducktype.select().column(expression, alias="override").build()
    assert statement == 'SELECT sum("amount") AS "override"'


def test_select_builder_requires_columns() -> None:
    builder = ducktype.select()
    with pytest.raises(ValueError):
        builder.build()


def test_select_builder_rejects_multiple_from_clauses() -> None:
    builder = ducktype.select().column("1")
    builder.from_("dual")
    with pytest.raises(ValueError):
        builder.from_("other")


def test_select_builder_supports_star_projection() -> None:
    statement = ducktype.select().star().build()
    assert statement == "SELECT *"


def test_select_builder_star_supports_exclude_and_replace() -> None:
    statement = (
        ducktype.select()
        .star(
            replace=[("renamed", '"value"')],
            exclude=["other"],
        )
        .build()
    )
    assert (
        statement
        == 'SELECT * REPLACE ("value" AS "renamed") EXCLUDE ("other")'
    )


def test_select_builder_star_accepts_aliased_expressions() -> None:
    expression = ducktype.Numeric("value").alias("renamed")
    statement = ducktype.select().star(replace=[expression]).build()
    assert statement == 'SELECT * REPLACE ("value" AS "renamed")'


def test_select_builder_build_select_list() -> None:
    builder = ducktype.select().column("1")
    select_list = builder.build_select_list()
    assert select_list == "1"
    with pytest.raises(RuntimeError):
        builder.column("2")


def test_select_builder_if_exists_requires_dependencies() -> None:
    builder = ducktype.select()
    with pytest.raises(TypeError):
        builder.column("1", if_exists=True)


def test_select_builder_if_exists_requires_available_columns() -> None:
    builder = ducktype.select()
    builder.column(ducktype.Numeric("value"), if_exists=True)
    with pytest.raises(RuntimeError):
        builder.build_select_list()


def test_select_builder_if_exists_skips_missing_columns() -> None:
    builder = ducktype.select()
    builder.column(ducktype.Numeric("present"))
    builder.column(ducktype.Numeric("missing"), if_exists=True)
    included = builder.build_select_list(available_columns=["present"])
    assert included == '"present"'


def test_select_builder_if_exists_includes_available_columns() -> None:
    builder = ducktype.select()
    builder.column(ducktype.Numeric("present"))
    builder.column(ducktype.Numeric("optional"), if_exists=True)
    included = builder.build_select_list(
        available_columns=["present", "optional"]
    )
    assert included == '"present", "optional"'


def test_select_builder_replace_if_exists_respects_dependencies() -> None:
    def make_builder():
        return ducktype.select().star(
            replace_if_exists={"alias": ducktype.Numeric("value")}
        )

    builder = make_builder()
    with pytest.raises(RuntimeError):
        builder.build_select_list()

    present = make_builder().build_select_list(available_columns=["value"])
    assert present == '* REPLACE ("value" AS "alias")'

    skipped = make_builder().build_select_list(available_columns=["other"])
    assert skipped == '*'


def test_select_builder_exclude_if_exists_skips_missing_columns() -> None:
    def make_builder():
        return ducktype.select().star(exclude_if_exists=["value"])

    builder = make_builder()
    with pytest.raises(RuntimeError):
        builder.build_select_list()

    present = make_builder().build_select_list(available_columns=["value"])
    assert present == '* EXCLUDE ("value")'

    skipped = make_builder().build_select_list(available_columns=["other"])
    assert skipped == '*'


def test_select_builder_if_exists_rejects_qualified_dependencies() -> None:
    builder = ducktype.select()
    with pytest.raises(ValueError):
        builder.column(ducktype.Numeric("value", table="orders"), if_exists=True)


def test_select_builder_if_exists_requires_column_dependencies() -> None:
    builder = ducktype.select()
    expression = ducktype.Numeric.raw("1", dependencies=[])
    with pytest.raises(ValueError):
        builder.column(expression, if_exists=True)
