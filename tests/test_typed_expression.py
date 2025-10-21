"""Unit tests for the typed expression sub-module."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from duckplus.static_typed import (
    AGGREGATE_FUNCTIONS,
    BooleanExpression,
    ExpressionDependency,
    GenericExpression,
    NumericExpression,
    VarcharExpression,
    SCALAR_FUNCTIONS,
    ducktype,
)
def col_dep(name: str, *, table: str | None = None) -> ExpressionDependency:
    return ExpressionDependency.column(name, table=table)


def test_numeric_column_carries_metadata() -> None:
    expression = ducktype.Numeric("total")
    assert isinstance(expression, NumericExpression)
    assert expression.render() == '"total"'
    assert expression.dependencies == {col_dep('total')}
    assert expression.duck_type.render() == "NUMERIC"


def test_decimal_column_uses_decimal_metadata() -> None:
    expression = ducktype.Decimal_10_2("amount")
    assert expression.render() == '"amount"'
    assert expression.dependencies == {col_dep('amount')}
    assert expression.duck_type.render() == "DECIMAL(10, 2)"


def test_decimal_literal_binary_operation_tracks_dependencies() -> None:
    literal = ducktype.Decimal_8_3.literal(Decimal("1.234"))
    column = ducktype.Decimal_8_3("discount")
    expression = literal + column

    assert expression.render() == '(1.234 + "discount")'
    assert expression.dependencies == {col_dep('discount')}
    assert expression.duck_type.render() == "DECIMAL(8, 3)"


def test_numeric_column_with_table_dependency() -> None:
    expression = ducktype.Numeric("total", table="orders")
    assert expression.render() == '"orders"."total"'
    assert expression.dependencies == {col_dep('total', table='orders')}


def test_numeric_aggregate_sum_uses_dependencies() -> None:
    expression = ducktype.Numeric.Aggregate.sum("sales")
    assert expression.render() == 'sum("sales")'
    assert expression.dependencies == {col_dep('sales')}


def test_integer_aggregate_avg_returns_double_type() -> None:
    expression = ducktype.Integer.Aggregate.avg("sales")
    assert expression.render() == 'avg("sales")'
    assert expression.dependencies == {col_dep('sales')}
    assert expression.duck_type.render() == "DOUBLE"


def test_numeric_aggregate_count_if_uses_predicate_dependencies() -> None:
    predicate = ducktype.Boolean("include")
    expression = ducktype.Numeric.Aggregate.count_if(predicate)
    assert expression.render() == 'count_if("include")'
    assert expression.dependencies == {col_dep('include')}


def test_generic_aggregate_max_tracks_dependencies() -> None:
    expression = ducktype.Generic.Aggregate.max("payload")
    assert expression.render() == 'max("payload")'
    assert expression.dependencies == {col_dep('payload')}


def test_scalar_namespace_lower_tracks_dependencies() -> None:
    expression = SCALAR_FUNCTIONS.Varchar.lower("customer")
    assert expression.render() == 'lower("customer")'
    assert expression.dependencies == {col_dep('customer')}


def test_scalar_namespace_numeric_literal() -> None:
    expression = SCALAR_FUNCTIONS.Numeric.abs(42)
    assert expression.render() == 'abs(42)'
    assert expression.dependencies == frozenset()


def test_scalar_symbolic_operator_renders_infix() -> None:
    left = ducktype.Varchar("name")
    right = ducktype.Varchar.literal("%foo%")
    expression = SCALAR_FUNCTIONS.Boolean.symbols['~~'](left, right)
    assert expression.render() == "(\"name\" ~~ '%foo%')"
    assert expression.dependencies == {col_dep('name')}


def test_aggregate_namespace_sum_tracks_dependencies() -> None:
    expression = AGGREGATE_FUNCTIONS.Numeric.sum("revenue")
    assert expression.render() == 'sum("revenue")'
    assert expression.dependencies == {col_dep('revenue')}


def test_aggregate_namespace_filter_merges_dependencies() -> None:
    predicate = ducktype.Boolean("is_active")
    expression = AGGREGATE_FUNCTIONS.Numeric.sum_filter(predicate, "revenue")
    assert expression.render() == 'sum("revenue") FILTER (WHERE "is_active")'
    assert expression.dependencies == {col_dep('revenue'), col_dep('is_active')}


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


def test_numeric_abs_method() -> None:
    expression = ducktype.Numeric.literal(-5).abs()
    assert expression.render() == "abs(-5)"
    assert expression.duck_type.category == "numeric"


def test_varchar_starts_with_method_tracks_dependencies() -> None:
    expression = ducktype.Varchar("name").starts_with("A")
    assert expression.render() == "starts_with(\"name\", 'A')"
    assert expression.dependencies == {col_dep('name')}


def test_varchar_trim_without_arguments() -> None:
    expression = ducktype.Varchar("name").trim()
    assert expression.render() == 'trim("name")'
    assert expression.dependencies == {col_dep('name')}


def test_varchar_trim_with_literal_characters() -> None:
    expression = ducktype.Varchar("name").trim("-")
    assert expression.render() == "trim(\"name\", '-')"
    assert expression.dependencies == {col_dep('name')}


def test_scalar_varchar_functions_include_macro_split_part() -> None:
    expression = SCALAR_FUNCTIONS.Varchar.split_part(
        ducktype.Varchar.literal("a-b"),
        ducktype.Varchar.literal("-"),
        1,
    )
    assert isinstance(expression, VarcharExpression)
    assert expression.render() == "split_part('a-b', '-', 1)"
    assert expression.dependencies == frozenset()


def test_scalar_generic_functions_include_array_append() -> None:
    expression = SCALAR_FUNCTIONS.Generic.array_append(
        ducktype.Generic("items"),
        ducktype.Varchar.literal("new"),
    )
    assert isinstance(expression, GenericExpression)
    assert expression.render() == "array_append(\"items\", 'new')"
    assert expression.dependencies == {col_dep('items')}


def test_try_cast_returns_numeric_expression() -> None:
    expression = ducktype.Varchar("value").try_cast("INTEGER")
    assert isinstance(expression, NumericExpression)
    assert expression.render() == 'TRY_CAST("value" AS INTEGER)'
    assert expression.dependencies == {col_dep('value')}
    assert expression.duck_type.render() == "INTEGER"


def test_cast_to_varchar_returns_varchar_expression() -> None:
    expression = ducktype.Numeric("value").cast("VARCHAR")
    assert isinstance(expression, VarcharExpression)
    assert expression.render() == 'CAST("value" AS VARCHAR)'
    assert expression.dependencies == {col_dep('value')}
    assert expression.duck_type.render() == "VARCHAR"


def test_cast_rejects_invalid_target_type() -> None:
    with pytest.raises(TypeError):
        ducktype.Numeric("value").cast(123)


def test_numeric_pow_accepts_literal_exponent() -> None:
    expression = ducktype.Numeric("base").pow(2)
    assert expression.render() == 'pow("base", 2)'
    assert expression.dependencies == {col_dep('base')}


def test_numeric_aggregate_sum_alias() -> None:
    expression = ducktype.Numeric.Aggregate.sum("revenue").alias("total")
    assert expression.render() == 'sum("revenue") AS "total"'
    assert expression.dependencies == {col_dep('revenue')}


def test_numeric_expression_method_sum() -> None:
    aggregated = ducktype.Numeric("amount").sum()
    assert isinstance(aggregated, NumericExpression)
    assert aggregated.render() == 'sum("amount")'
    assert aggregated.dependencies == {col_dep('amount')}


def test_integer_expression_avg_returns_double_type() -> None:
    aggregated = ducktype.Integer("amount").avg()
    assert aggregated.render() == 'avg("amount")'
    assert aggregated.dependencies == {col_dep('amount')}
    assert aggregated.duck_type.render() == "DOUBLE"


def test_integer_literal_preserves_type() -> None:
    literal = ducktype.Integer.literal(5)
    assert literal.render() == "5"
    assert literal.duck_type.render() == "INTEGER"


def test_integer_literal_rejects_float_literal() -> None:
    with pytest.raises(TypeError):
        ducktype.Integer.literal(5.5)


def test_float_literal_rejects_boolean() -> None:
    with pytest.raises(TypeError):
        ducktype.Float.literal(True)


def test_date_literal_from_python_date() -> None:
    literal = ducktype.Date.literal(date(2024, 1, 5))
    assert literal.render() == "DATE '2024-01-05'"
    assert literal.duck_type.render() == "DATE"


def test_timestamp_literal_from_python_datetime() -> None:
    stamp = ducktype.Timestamp.literal(datetime(2024, 1, 5, 14, 30, 15))
    assert stamp.render().startswith("TIMESTAMP '")
    assert "2024-01-05 14:30:15" in stamp.render()
    assert stamp.duck_type.render() == "TIMESTAMP"


def test_timestamp_literal_rejects_date_value() -> None:
    with pytest.raises(TypeError):
        ducktype.Timestamp.literal(date(2024, 1, 5))


def test_timestamp_precision_literal_tracks_type() -> None:
    literal = ducktype.Timestamp_ns.literal(datetime(2024, 1, 5, 14, 30, 15, 123456))
    assert literal.duck_type.render() == "TIMESTAMP_NS"


def test_timestamp_precision_coalesce_accepts_variant_literal() -> None:
    base = ducktype.Timestamp_ms("event_time")
    fallback = ducktype.Timestamp_ns.literal("2024-01-05 14:30:15.123456789")
    expression = base.coalesce(fallback)
    assert expression.render().startswith('COALESCE("event_time"')
    assert expression.duck_type.render() == "TIMESTAMP_MS"
    assert expression.dependencies == {col_dep('event_time')}


def test_timestamp_with_timezone_literal_tracks_type() -> None:
    literal = ducktype.Timestamp_tz.literal("2024-01-05 14:30:15+00")
    assert literal.duck_type.render() == "TIMESTAMP WITH TIME ZONE"


def test_date_coalesce_tracks_dependencies() -> None:
    fallback = ducktype.Date.literal("2024-01-01")
    expression = ducktype.Date("order_date").coalesce(fallback)
    assert expression.render() == "COALESCE(\"order_date\", DATE '2024-01-01')"
    assert expression.dependencies == {col_dep('order_date')}


def test_date_aggregate_max_tracks_dependencies() -> None:
    expression = ducktype.Date.Aggregate.max("order_date")
    assert expression.render() == 'max("order_date")'
    assert expression.dependencies == {col_dep('order_date')}


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


def test_case_expression_otherwise_alias() -> None:
    expression = (
        ducktype.Varchar.case().when(True, "present").otherwise("missing").end()
    )
    assert expression.render() == "CASE WHEN TRUE THEN 'present' ELSE 'missing' END"
    assert expression.dependencies == set()


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
    expression = ducktype.Numeric.literal(1)
    with pytest.raises(ValueError):
        builder.column(expression, if_exists=True)
