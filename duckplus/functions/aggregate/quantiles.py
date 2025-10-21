"""Decorator-backed quantile aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
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
        AggregateGenericFunctions,
    )


_QUANTILE_DESCRIPTION = (
    "Returns the exact quantile number between 0 and 1. "
    "If pos is a LIST of FLOATs, then the result is a LIST of the corresponding "
    "exact quantiles."
)

_QUANTILE_CONT_DESCRIPTION = (
    "Returns the interpolated quantile number between 0 and 1. "
    "If pos is a LIST of FLOATs, then the result is a LIST of the corresponding "
    "interpolated quantiles."
)


def _build_definition(
    *,
    function_name: str,
    return_type: str,
    parameter_types: Sequence[str],
    parameters: Sequence[str],
    description: str,
) -> DuckDBFunctionDefinition:
    return DuckDBFunctionDefinition(
        schema_name="main",
        function_name=function_name,
        function_type="aggregate",
        return_type=parse_type(return_type),
        parameter_types=tuple(parse_type(type_name) for type_name in parameter_types),
        parameters=tuple(parameters),
        varargs=None,
        description=description,
        comment=None,
        macro_definition=None,
    )


def _format_overloads(
    signatures: Sequence[DuckDBFunctionDefinition],
) -> tuple[str, ...]:
    formatted: list[str] = []
    for signature in signatures:
        params = ", ".join(
            f"{param_type} {name}"
            for param_type, name in zip(signature.parameter_types, signature.parameters)
        )
        if params:
            formatted.append(
                f"- main.{signature.function_name}({params}) -> {signature.return_type}"
            )
        else:
            formatted.append(
                f"- main.{signature.function_name}() -> {signature.return_type}"
            )
    return tuple(formatted)


def _build_docstring(
    function_name: str,
    description: str,
    signatures: Sequence[DuckDBFunctionDefinition],
    *,
    filter_variant: bool = False,
) -> str:
    header = f"Call DuckDB function ``{function_name}``"
    if filter_variant:
        header += " with ``FILTER``"
    header += "."
    overloads = "\n".join(_format_overloads(signatures))
    return f"{header}\n\n{description}\n\nOverloads:\n{overloads}\n"


_QUANTILE_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        function_name="quantile",
        return_type="ANY",
        parameter_types=("ANY",),
        parameters=("x",),
        description=_QUANTILE_DESCRIPTION,
    ),
    _build_definition(
        function_name="quantile",
        return_type="ANY",
        parameter_types=("ANY", "DOUBLE"),
        parameters=("x", "pos"),
        description=_QUANTILE_DESCRIPTION,
    ),
    _build_definition(
        function_name="quantile",
        return_type="ANY",
        parameter_types=("ANY", "DOUBLE[]"),
        parameters=("x", "pos"),
        description=_QUANTILE_DESCRIPTION,
    ),
)

_QUANTILE_CONT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = tuple(
    _build_definition(
        function_name="quantile_cont",
        return_type=value_type,
        parameter_types=(value_type, position_type),
        parameters=("x", "pos"),
        description=_QUANTILE_CONT_DESCRIPTION,
    )
    for value_type in (
        "DATE",
        "TIMESTAMP",
        "TIME",
        "TIMESTAMP WITH TIME ZONE",
        "TIME WITH TIME ZONE",
    )
    for position_type in ("DOUBLE", "DOUBLE[]")
)

_QUANTILE_DISC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        function_name="quantile_disc",
        return_type="ANY",
        parameter_types=("ANY",),
        parameters=("x",),
        description=_QUANTILE_DESCRIPTION,
    ),
    _build_definition(
        function_name="quantile_disc",
        return_type="ANY",
        parameter_types=("ANY", "DOUBLE"),
        parameters=("x", "pos"),
        description=_QUANTILE_DESCRIPTION,
    ),
    _build_definition(
        function_name="quantile_disc",
        return_type="ANY",
        parameter_types=("ANY", "DOUBLE[]"),
        parameters=("x", "pos"),
        description=_QUANTILE_DESCRIPTION,
    ),
)


@register_duckdb_function("quantile")
def quantile(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``quantile``."""

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _QUANTILE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


quantile.__doc__ = _build_docstring(
    "quantile",
    _QUANTILE_DESCRIPTION,
    _QUANTILE_SIGNATURES,
)


@register_duckdb_function("quantile_filter")
def quantile_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``quantile`` with ``FILTER``."""

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _QUANTILE_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


quantile_filter.__doc__ = _build_docstring(
    "quantile",
    _QUANTILE_DESCRIPTION,
    _QUANTILE_SIGNATURES,
    filter_variant=True,
)


@register_duckdb_function("quantile_cont")
def quantile_cont(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``quantile_cont``."""

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _QUANTILE_CONT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


quantile_cont.__doc__ = _build_docstring(
    "quantile_cont",
    _QUANTILE_CONT_DESCRIPTION,
    _QUANTILE_CONT_SIGNATURES,
)


@register_duckdb_function("quantile_cont_filter")
def quantile_cont_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``quantile_cont`` with ``FILTER``."""

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _QUANTILE_CONT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


quantile_cont_filter.__doc__ = _build_docstring(
    "quantile_cont",
    _QUANTILE_CONT_DESCRIPTION,
    _QUANTILE_CONT_SIGNATURES,
    filter_variant=True,
)


@register_duckdb_function("quantile_disc")
def quantile_disc(
    self: "AggregateGenericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``quantile_disc``."""

    return cast(
        TypedExpression,
        invoke_duckdb_function(
            _QUANTILE_DISC_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


quantile_disc.__doc__ = _build_docstring(
    "quantile_disc",
    _QUANTILE_DESCRIPTION,
    _QUANTILE_DISC_SIGNATURES,
)


@register_duckdb_function("quantile_disc_filter")
def quantile_disc_filter(
    self: "AggregateGenericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``quantile_disc`` with ``FILTER``."""

    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            _QUANTILE_DISC_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


quantile_disc_filter.__doc__ = _build_docstring(
    "quantile_disc",
    _QUANTILE_DESCRIPTION,
    _QUANTILE_DISC_SIGNATURES,
    filter_variant=True,
)


def _register() -> None:
    """Attach quantile helpers onto the aggregate namespace."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
    )

    namespace: Any = AggregateGenericFunctions

    namespace._QUANTILE_SIGNATURES = _QUANTILE_SIGNATURES
    namespace._QUANTILE_CONT_SIGNATURES = _QUANTILE_CONT_SIGNATURES
    namespace._QUANTILE_DISC_SIGNATURES = _QUANTILE_DISC_SIGNATURES
    namespace.quantile = quantile  # type: ignore[assignment]
    namespace.quantile_filter = quantile_filter  # type: ignore[assignment]
    namespace.quantile_cont = quantile_cont  # type: ignore[assignment]
    namespace.quantile_cont_filter = quantile_cont_filter  # type: ignore[assignment]
    namespace.quantile_disc = quantile_disc  # type: ignore[assignment]
    namespace.quantile_disc_filter = quantile_disc_filter  # type: ignore[assignment]
    namespace._register_function(
        "quantile",
        names=getattr(quantile, "__duckdb_identifiers__", ()),
        symbols=getattr(quantile, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "quantile_filter",
        names=getattr(quantile_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(quantile_filter, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "quantile_cont",
        names=getattr(quantile_cont, "__duckdb_identifiers__", ()),
        symbols=getattr(quantile_cont, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "quantile_cont_filter",
        names=getattr(quantile_cont_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(quantile_cont_filter, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "quantile_disc",
        names=getattr(quantile_disc, "__duckdb_identifiers__", ()),
        symbols=getattr(quantile_disc, "__duckdb_symbols__", ()),
    )
    namespace._register_function(
        "quantile_disc_filter",
        names=getattr(quantile_disc_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(quantile_disc_filter, "__duckdb_symbols__", ()),
    )


_register()


__all__ = [
    "quantile",
    "quantile_filter",
    "quantile_cont",
    "quantile_cont_filter",
    "quantile_disc",
    "quantile_disc_filter",
]
