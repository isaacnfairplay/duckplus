"""Approximation-focused aggregate helpers exposed as direct Python methods."""

# pylint: disable=too-many-arguments,protected-access,import-outside-toplevel

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import NumericExpression, TypedExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
        AggregateNumericFunctions,
    )


_APPROX_COUNT_DISTINCT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_count_distinct",
        function_type="aggregate",
        return_type=parse_type("BIGINT"),
        parameter_types=(parse_type("ANY"),),
        parameters=("any",),
        varargs=None,
        description="Computes the approximate count of distinct elements using HyperLogLog.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("approx_count_distinct")
def approx_count_distinct(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``approx_count_distinct``.

    Computes the approximate count of distinct elements using HyperLogLog.

    Overloads:
    - main.approx_count_distinct(ANY any) -> BIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _APPROX_COUNT_DISTINCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("approx_count_distinct_filter")
def approx_count_distinct_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``approx_count_distinct`` with ``FILTER``.

    Computes the approximate count of distinct elements using HyperLogLog.

    Overloads:
    - main.approx_count_distinct(ANY any) -> BIGINT
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _APPROX_COUNT_DISTINCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_APPROX_QUANTILE_GENERIC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("DATE"),
        parameter_types=(parse_type("DATE"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIME"),
        parameter_types=(parse_type("TIME"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIME WITH TIME ZONE"),
        parameter_types=(parse_type("TIME WITH TIME ZONE"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIMESTAMP"),
        parameter_types=(parse_type("TIMESTAMP"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIMESTAMP WITH TIME ZONE"),
        parameter_types=(parse_type("TIMESTAMP WITH TIME ZONE"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("DATE[]"),
        parameter_types=(parse_type("DATE"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIME[]"),
        parameter_types=(parse_type("TIME"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIME WITH TIME ZONE[]"),
        parameter_types=(parse_type("TIME WITH TIME ZONE"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIMESTAMP[]"),
        parameter_types=(parse_type("TIMESTAMP"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TIMESTAMP WITH TIME ZONE[]"),
        parameter_types=(parse_type("TIMESTAMP WITH TIME ZONE"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("approx_quantile")
def approx_quantile_generic(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``approx_quantile``.

    Computes the approximate quantile using T-Digest.

    Overloads:
    - main.approx_quantile(DATE x, FLOAT pos) -> DATE
    - main.approx_quantile(TIME x, FLOAT pos) -> TIME
    - main.approx_quantile(TIME WITH TIME ZONE x, FLOAT pos) -> TIME WITH TIME ZONE
    - main.approx_quantile(TIMESTAMP x, FLOAT pos) -> TIMESTAMP
    - main.approx_quantile(TIMESTAMP WITH TIME ZONE x, FLOAT pos) -> TIMESTAMP WITH TIME ZONE
    - main.approx_quantile(DATE x, FLOAT[] pos) -> DATE[]
    - main.approx_quantile(TIME x, FLOAT[] pos) -> TIME[]
    - main.approx_quantile(TIME WITH TIME ZONE x, FLOAT[] pos) -> TIME WITH TIME ZONE[]
    - main.approx_quantile(TIMESTAMP x, FLOAT[] pos) -> TIMESTAMP[]
    - main.approx_quantile(TIMESTAMP WITH TIME ZONE x, FLOAT[] pos) -> TIMESTAMP WITH TIME ZONE[]
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _APPROX_QUANTILE_GENERIC_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("approx_quantile_filter")
def approx_quantile_generic_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``approx_quantile`` with ``FILTER``.

    Computes the approximate quantile using T-Digest.

    Overloads:
    - main.approx_quantile(DATE x, FLOAT pos) -> DATE
    - main.approx_quantile(TIME x, FLOAT pos) -> TIME
    - main.approx_quantile(TIME WITH TIME ZONE x, FLOAT pos) -> TIME WITH TIME ZONE
    - main.approx_quantile(TIMESTAMP x, FLOAT pos) -> TIMESTAMP
    - main.approx_quantile(TIMESTAMP WITH TIME ZONE x, FLOAT pos) -> TIMESTAMP WITH TIME ZONE
    - main.approx_quantile(DATE x, FLOAT[] pos) -> DATE[]
    - main.approx_quantile(TIME x, FLOAT[] pos) -> TIME[]
    - main.approx_quantile(TIME WITH TIME ZONE x, FLOAT[] pos) -> TIME WITH TIME ZONE[]
    - main.approx_quantile(TIMESTAMP x, FLOAT[] pos) -> TIMESTAMP[]
    - main.approx_quantile(TIMESTAMP WITH TIME ZONE x, FLOAT[] pos) -> TIMESTAMP WITH TIME ZONE[]
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _APPROX_QUANTILE_GENERIC_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_APPROX_QUANTILE_NUMERIC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("DECIMAL"),
        parameter_types=(parse_type("DECIMAL"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("SMALLINT"),
        parameter_types=(parse_type("SMALLINT"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("INTEGER"),
        parameter_types=(parse_type("INTEGER"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("BIGINT"),
        parameter_types=(parse_type("BIGINT"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("HUGEINT"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("DOUBLE"), parse_type("FLOAT")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("DECIMAL[]"),
        parameter_types=(parse_type("DECIMAL"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("TINYINT[]"),
        parameter_types=(parse_type("TINYINT"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("SMALLINT[]"),
        parameter_types=(parse_type("SMALLINT"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("INTEGER[]"),
        parameter_types=(parse_type("INTEGER"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("BIGINT[]"),
        parameter_types=(parse_type("BIGINT"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("HUGEINT[]"),
        parameter_types=(parse_type("HUGEINT"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("FLOAT[]"),
        parameter_types=(parse_type("FLOAT"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_quantile",
        function_type="aggregate",
        return_type=parse_type("DOUBLE[]"),
        parameter_types=(parse_type("DOUBLE"), parse_type("FLOAT[]")),
        parameters=("x", "pos"),
        varargs=None,
        description="Computes the approximate quantile using T-Digest.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("approx_quantile")
def approx_quantile_numeric(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``approx_quantile``.

    Computes the approximate quantile using T-Digest.

    Overloads:
    - main.approx_quantile(DECIMAL x, FLOAT pos) -> DECIMAL
    - main.approx_quantile(SMALLINT x, FLOAT pos) -> SMALLINT
    - main.approx_quantile(INTEGER x, FLOAT pos) -> INTEGER
    - main.approx_quantile(BIGINT x, FLOAT pos) -> BIGINT
    - main.approx_quantile(HUGEINT x, FLOAT pos) -> HUGEINT
    - main.approx_quantile(DOUBLE x, FLOAT pos) -> DOUBLE
    - main.approx_quantile(DECIMAL x, FLOAT[] pos) -> DECIMAL[]
    - main.approx_quantile(TINYINT x, FLOAT[] pos) -> TINYINT[]
    - main.approx_quantile(SMALLINT x, FLOAT[] pos) -> SMALLINT[]
    - main.approx_quantile(INTEGER x, FLOAT[] pos) -> INTEGER[]
    - main.approx_quantile(BIGINT x, FLOAT[] pos) -> BIGINT[]
    - main.approx_quantile(HUGEINT x, FLOAT[] pos) -> HUGEINT[]
    - main.approx_quantile(FLOAT x, FLOAT[] pos) -> FLOAT[]
    - main.approx_quantile(DOUBLE x, FLOAT[] pos) -> DOUBLE[]
    """

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _APPROX_QUANTILE_NUMERIC_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("approx_quantile_filter")
def approx_quantile_numeric_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``approx_quantile`` with ``FILTER``.

    Computes the approximate quantile using T-Digest.

    Overloads:
    - main.approx_quantile(DECIMAL x, FLOAT pos) -> DECIMAL
    - main.approx_quantile(SMALLINT x, FLOAT pos) -> SMALLINT
    - main.approx_quantile(INTEGER x, FLOAT pos) -> INTEGER
    - main.approx_quantile(BIGINT x, FLOAT pos) -> BIGINT
    - main.approx_quantile(HUGEINT x, FLOAT pos) -> HUGEINT
    - main.approx_quantile(DOUBLE x, FLOAT pos) -> DOUBLE
    - main.approx_quantile(DECIMAL x, FLOAT[] pos) -> DECIMAL[]
    - main.approx_quantile(TINYINT x, FLOAT[] pos) -> TINYINT[]
    - main.approx_quantile(SMALLINT x, FLOAT[] pos) -> SMALLINT[]
    - main.approx_quantile(INTEGER x, FLOAT[] pos) -> INTEGER[]
    - main.approx_quantile(BIGINT x, FLOAT[] pos) -> BIGINT[]
    - main.approx_quantile(HUGEINT x, FLOAT[] pos) -> HUGEINT[]
    - main.approx_quantile(FLOAT x, FLOAT[] pos) -> FLOAT[]
    - main.approx_quantile(DOUBLE x, FLOAT[] pos) -> DOUBLE[]
    """

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _APPROX_QUANTILE_NUMERIC_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_APPROX_TOP_K_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="approx_top_k",
        function_type="aggregate",
        return_type=parse_type("ANY[]"),
        parameter_types=(parse_type("ANY"), parse_type("BIGINT")),
        parameters=("val", "k"),
        varargs=None,
        description="Finds the k approximately most occurring values in the data set",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("approx_top_k")
def approx_top_k(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``approx_top_k``.

    Finds the k approximately most occurring values in the data set

    Overloads:
    - main.approx_top_k(ANY val, BIGINT k) -> ANY[]
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _APPROX_TOP_K_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("approx_top_k_filter")
def approx_top_k_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``approx_top_k`` with ``FILTER``.

    Finds the k approximately most occurring values in the data set

    Overloads:
    - main.approx_top_k(ANY val, BIGINT k) -> ANY[]
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _APPROX_TOP_K_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_HISTOGRAM_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="histogram",
        function_type="aggregate",
        return_type=parse_type("MAP"),
        parameter_types=(parse_type("ANY"),),
        parameters=("arg",),
        varargs=None,
        description="Returns a LIST of STRUCTs with the fields bucket and count.",
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="histogram",
        function_type="aggregate",
        return_type=parse_type("MAP"),
        parameter_types=(parse_type("ANY"), parse_type("ANY[]")),
        parameters=("arg", "col1"),
        varargs=None,
        description="Returns a LIST of STRUCTs with the fields bucket and count.",
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("histogram")
def histogram(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``histogram``.

    Returns a LIST of STRUCTs with the fields bucket and count.

    Overloads:
    - main.histogram(ANY arg) -> MAP
    - main.histogram(ANY arg, ANY[] col1) -> MAP
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _HISTOGRAM_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("histogram_filter")
def histogram_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``histogram`` with ``FILTER``.

    Returns a LIST of STRUCTs with the fields bucket and count.

    Overloads:
    - main.histogram(ANY arg) -> MAP
    - main.histogram(ANY arg, ANY[] col1) -> MAP
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _HISTOGRAM_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


_HISTOGRAM_EXACT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="histogram_exact",
        function_type="aggregate",
        return_type=parse_type("MAP"),
        parameter_types=(parse_type("ANY"), parse_type("ANY[]")),
        parameters=("arg", "bins"),
        varargs=None,
        description=(
            "Returns a LIST of STRUCTs with the fields bucket and count matching the "
            "buckets exactly."
        ),
        comment=None,
        macro_definition=None,
    ),
)


@register_duckdb_function("histogram_exact")
def histogram_exact(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``histogram_exact``.

    Returns a LIST of STRUCTs with the fields bucket and count matching the buckets exactly.

    Overloads:
    - main.histogram_exact(ANY arg, ANY[] bins) -> MAP
    """

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _HISTOGRAM_EXACT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("histogram_exact_filter")
def histogram_exact_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``histogram_exact`` with ``FILTER``.

    Returns a LIST of STRUCTs with the fields bucket and count matching the buckets exactly.

    Overloads:
    - main.histogram_exact(ANY arg, ANY[] bins) -> MAP
    """

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _HISTOGRAM_EXACT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


def _register() -> None:
    """Attach approximation helpers onto the aggregate namespaces."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
        AggregateNumericFunctions,
    )

    generic_namespace: Any = AggregateGenericFunctions
    numeric_namespace: Any = AggregateNumericFunctions

    numeric_namespace._APPROX_COUNT_DISTINCT_SIGNATURES = _APPROX_COUNT_DISTINCT_SIGNATURES
    numeric_namespace.approx_count_distinct = approx_count_distinct  # type: ignore[assignment]
    numeric_namespace.approx_count_distinct_filter = (
        approx_count_distinct_filter  # type: ignore[assignment]
    )
    numeric_namespace._register_function(
        "approx_count_distinct",
        names=getattr(approx_count_distinct, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_count_distinct, "__duckdb_symbols__", ()),
    )
    numeric_namespace._register_function(
        "approx_count_distinct_filter",
        names=getattr(approx_count_distinct_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_count_distinct_filter, "__duckdb_symbols__", ()),
    )

    generic_namespace._APPROX_QUANTILE_SIGNATURES = _APPROX_QUANTILE_GENERIC_SIGNATURES
    generic_namespace.approx_quantile = approx_quantile_generic  # type: ignore[assignment]
    generic_namespace.approx_quantile_filter = (
        approx_quantile_generic_filter  # type: ignore[assignment]
    )
    generic_namespace._register_function(
        "approx_quantile",
        names=getattr(approx_quantile_generic, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_quantile_generic, "__duckdb_symbols__", ()),
    )
    generic_namespace._register_function(
        "approx_quantile_filter",
        names=getattr(approx_quantile_generic_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_quantile_generic_filter, "__duckdb_symbols__", ()),
    )

    numeric_namespace._APPROX_QUANTILE_SIGNATURES = _APPROX_QUANTILE_NUMERIC_SIGNATURES
    numeric_namespace.approx_quantile = approx_quantile_numeric  # type: ignore[assignment]
    numeric_namespace.approx_quantile_filter = (
        approx_quantile_numeric_filter  # type: ignore[assignment]
    )
    numeric_namespace._register_function(
        "approx_quantile",
        names=getattr(approx_quantile_numeric, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_quantile_numeric, "__duckdb_symbols__", ()),
    )
    numeric_namespace._register_function(
        "approx_quantile_filter",
        names=getattr(approx_quantile_numeric_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_quantile_numeric_filter, "__duckdb_symbols__", ()),
    )

    generic_namespace._APPROX_TOP_K_SIGNATURES = _APPROX_TOP_K_SIGNATURES
    generic_namespace.approx_top_k = approx_top_k  # type: ignore[assignment]
    generic_namespace.approx_top_k_filter = approx_top_k_filter  # type: ignore[assignment]
    generic_namespace._register_function(
        "approx_top_k",
        names=getattr(approx_top_k, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_top_k, "__duckdb_symbols__", ()),
    )
    generic_namespace._register_function(
        "approx_top_k_filter",
        names=getattr(approx_top_k_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(approx_top_k_filter, "__duckdb_symbols__", ()),
    )

    generic_namespace._HISTOGRAM_SIGNATURES = _HISTOGRAM_SIGNATURES
    generic_namespace.histogram = histogram  # type: ignore[assignment]
    generic_namespace.histogram_filter = histogram_filter  # type: ignore[assignment]
    generic_namespace._register_function(
        "histogram",
        names=getattr(histogram, "__duckdb_identifiers__", ()),
        symbols=getattr(histogram, "__duckdb_symbols__", ()),
    )
    generic_namespace._register_function(
        "histogram_filter",
        names=getattr(histogram_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(histogram_filter, "__duckdb_symbols__", ()),
    )

    generic_namespace._HISTOGRAM_EXACT_SIGNATURES = _HISTOGRAM_EXACT_SIGNATURES
    generic_namespace.histogram_exact = histogram_exact  # type: ignore[assignment]
    generic_namespace.histogram_exact_filter = (
        histogram_exact_filter  # type: ignore[assignment]
    )
    generic_namespace._register_function(
        "histogram_exact",
        names=getattr(histogram_exact, "__duckdb_identifiers__", ()),
        symbols=getattr(histogram_exact, "__duckdb_symbols__", ()),
    )
    generic_namespace._register_function(
        "histogram_exact_filter",
        names=getattr(histogram_exact_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(histogram_exact_filter, "__duckdb_symbols__", ()),
    )


_register()

__all__ = [
    "approx_count_distinct",
    "approx_count_distinct_filter",
    "approx_quantile_generic",
    "approx_quantile_generic_filter",
    "approx_quantile_numeric",
    "approx_quantile_numeric_filter",
    "approx_top_k",
    "approx_top_k_filter",
    "histogram",
    "histogram_filter",
    "histogram_exact",
    "histogram_exact_filter",
]
