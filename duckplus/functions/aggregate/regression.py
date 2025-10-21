"""Regression and covariance aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Sequence, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import TypedExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateNumericFunctions,
    )


@dataclass(frozen=True)
class _HelperConfig:
    """Configuration for regression and covariance helpers."""

    function_name: str
    description: str | None
    return_type: str


_HELPER_CONFIGS: tuple[_HelperConfig, ...] = (
    _HelperConfig(
        function_name="covar_pop",
        description="Returns the population covariance of input values.",
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="covar_samp",
        description=(
            "Returns the sample covariance for non-NULL pairs in a group."
        ),
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_avgx",
        description=(
            "Returns the average of the independent variable for non-NULL pairs "
            "in a group, where x is the independent variable and y is the "
            "dependent variable."
        ),
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_avgy",
        description=(
            "Returns the average of the dependent variable for non-NULL pairs in "
            "a group, where x is the independent variable and y is the dependent "
            "variable."
        ),
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_count",
        description="Returns the number of non-NULL number pairs in a group.",
        return_type="UINTEGER",
    ),
    _HelperConfig(
        function_name="regr_intercept",
        description=(
            "Returns the intercept of the univariate linear regression line for "
            "non-NULL pairs in a group."
        ),
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_r2",
        description=(
            "Returns the coefficient of determination for non-NULL pairs in a "
            "group."
        ),
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_slope",
        description=(
            "Returns the slope of the linear regression line for non-NULL pairs "
            "in a group."
        ),
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_sxx",
        description=None,
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_sxy",
        description="Returns the population covariance of input values.",
        return_type="DOUBLE",
    ),
    _HelperConfig(
        function_name="regr_syy",
        description=None,
        return_type="DOUBLE",
    ),
)


def _make_signatures(config: _HelperConfig) -> tuple[DuckDBFunctionDefinition, ...]:
    return (
        DuckDBFunctionDefinition(
            schema_name="main",
            function_name=config.function_name,
            function_type="aggregate",
            return_type=parse_type(config.return_type),
            parameter_types=(parse_type("DOUBLE"), parse_type("DOUBLE")),
            parameters=("y", "x"),
            varargs=None,
            description=config.description,
            comment=None,
            macro_definition=None,
        ),
    )


def _build_docstring(
    config: _HelperConfig, *, filter_variant: bool = False
) -> str:
    header = f"Call DuckDB function ``{config.function_name}``"
    if filter_variant:
        header += " with ``FILTER``"
    header += "."

    lines = [header, ""]
    if config.description:
        lines.extend([config.description, ""])
    lines.extend(
        [
            "Overloads:",
            (
                f"- main.{config.function_name}(DOUBLE y, DOUBLE x) -> "
                f"{config.return_type}"
            ),
        ]
    )
    return "\n".join(lines)


def _invoke_registered_function(
    self: Any,
    attr_name: str,
    *,
    operands: tuple[object, ...],
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = cast(
        tuple[DuckDBFunctionDefinition, ...], getattr(self, attr_name)
    )
    return cast(
        TypedExpression,
        invoke_duckdb_function(
            signatures,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


def _invoke_registered_filter_function(
    self: Any,
    attr_name: str,
    predicate: object,
    *,
    operands: tuple[object, ...],
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = cast(
        tuple[DuckDBFunctionDefinition, ...], getattr(self, attr_name)
    )
    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            signatures,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("covar_pop")
def covar_pop(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``covar_pop``."""

    return _invoke_registered_function(
        self,
        "_COVAR_POP_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("covar_pop_filter")
def covar_pop_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``covar_pop`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_COVAR_POP_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("covar_samp")
def covar_samp(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``covar_samp``."""

    return _invoke_registered_function(
        self,
        "_COVAR_SAMP_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("covar_samp_filter")
def covar_samp_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``covar_samp`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_COVAR_SAMP_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_avgx")
def regr_avgx(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_avgx``."""

    return _invoke_registered_function(
        self,
        "_REGR_AVGX_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_avgx_filter")
def regr_avgx_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_avgx`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_AVGX_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_avgy")
def regr_avgy(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_avgy``."""

    return _invoke_registered_function(
        self,
        "_REGR_AVGY_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_avgy_filter")
def regr_avgy_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_avgy`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_AVGY_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_count")
def regr_count(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_count``."""

    return _invoke_registered_function(
        self,
        "_REGR_COUNT_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_count_filter")
def regr_count_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_count`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_COUNT_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_intercept")
def regr_intercept(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_intercept``."""

    return _invoke_registered_function(
        self,
        "_REGR_INTERCEPT_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_intercept_filter")
def regr_intercept_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_intercept`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_INTERCEPT_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_r2")
def regr_r2(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_r2``."""

    return _invoke_registered_function(
        self,
        "_REGR_R2_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_r2_filter")
def regr_r2_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_r2`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_R2_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_slope")
def regr_slope(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_slope``."""

    return _invoke_registered_function(
        self,
        "_REGR_SLOPE_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_slope_filter")
def regr_slope_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_slope`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_SLOPE_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_sxx")
def regr_sxx(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_sxx``."""

    return _invoke_registered_function(
        self,
        "_REGR_SXX_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_sxx_filter")
def regr_sxx_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_sxx`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_SXX_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_sxy")
def regr_sxy(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_sxy``."""

    return _invoke_registered_function(
        self,
        "_REGR_SXY_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_sxy_filter")
def regr_sxy_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_sxy`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_SXY_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_syy")
def regr_syy(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_syy``."""

    return _invoke_registered_function(
        self,
        "_REGR_SYY_SIGNATURES",
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


@register_duckdb_function("regr_syy_filter")
def regr_syy_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``regr_syy`` with ``FILTER``."""

    return _invoke_registered_filter_function(
        self,
        "_REGR_SYY_SIGNATURES",
        predicate,
        operands=operands,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )


def _register() -> None:
    """Attach regression helpers onto the numeric aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateNumericFunctions,
    )

    namespace_lookup: dict[str, Any] = {
        "AggregateNumericFunctions": AggregateNumericFunctions,
    }

    for config in _HELPER_CONFIGS:
        attr_name = f"_{config.function_name.upper()}_SIGNATURES"
        signatures = _make_signatures(config)
        namespace = namespace_lookup["AggregateNumericFunctions"]
        setattr(namespace, attr_name, signatures)

        function = globals()[config.function_name]
        setattr(namespace, config.function_name, function)  # type: ignore[assignment]
        namespace._register_function(
            config.function_name,
            names=getattr(function, "__duckdb_identifiers__", ()),
            symbols=getattr(function, "__duckdb_symbols__", ()),
        )

        filter_name = f"{config.function_name}_filter"
        filter_function = globals()[filter_name]
        setattr(namespace, filter_name, filter_function)  # type: ignore[assignment]
        namespace._register_function(
            filter_name,
            names=getattr(filter_function, "__duckdb_identifiers__", ()),
            symbols=getattr(filter_function, "__duckdb_symbols__", ()),
        )


for helper_config in _HELPER_CONFIGS:
    function = globals()[helper_config.function_name]
    filter_function = globals()[f"{helper_config.function_name}_filter"]
    function.__doc__ = _build_docstring(helper_config)
    filter_function.__doc__ = _build_docstring(
        helper_config, filter_variant=True
    )


_register()


__all__ = [
    "covar_pop",
    "covar_pop_filter",
    "covar_samp",
    "covar_samp_filter",
    "regr_avgx",
    "regr_avgx_filter",
    "regr_avgy",
    "regr_avgy_filter",
    "regr_count",
    "regr_count_filter",
    "regr_intercept",
    "regr_intercept_filter",
    "regr_r2",
    "regr_r2_filter",
    "regr_slope",
    "regr_slope_filter",
    "regr_sxx",
    "regr_sxx_filter",
    "regr_sxy",
    "regr_sxy_filter",
    "regr_syy",
    "regr_syy_filter",
]
