# pylint: disable=redefined-builtin

"""Summation and product aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from duckplus.functions._base import (
    invoke_duckdb_filter_function,
    invoke_duckdb_function,
    register_duckdb_function,
)
from duckplus.static_typed.expression import NumericExpression
from duckplus.static_typed.functions import DuckDBFunctionDefinition
from duckplus.static_typed.types import parse_type

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
        AggregateNumericFunctions,
    )


_SUM_DESCRIPTION = "Calculates the sum value for all tuples in arg."
_PRODUCT_DESCRIPTION = "Calculates the product of all tuples in arg."


_SUM_GENERIC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("BIGNUM"),
        parameter_types=(parse_type("BIGNUM"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
)

_SUM_NUMERIC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("DECIMAL"),
        parameter_types=(parse_type("DECIMAL"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("BOOLEAN"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("SMALLINT"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("INTEGER"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("BIGINT"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("HUGEINT"),
        parameter_types=(parse_type("HUGEINT"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="sum",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("DOUBLE"),),
        parameters=("arg",),
        varargs=None,
        description=_SUM_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
)

_PRODUCT_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="product",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("DOUBLE"),),
        parameters=("arg",),
        varargs=None,
        description=_PRODUCT_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
)


def _format_overloads(
    signatures: Iterable[DuckDBFunctionDefinition],
) -> tuple[str, ...]:
    formatted: list[str] = []
    for signature in signatures:
        params = ", ".join(
            f"{param_type} {name}"
            for param_type, name in zip(signature.parameter_types, signature.parameters)
        )
        formatted.append(
            f"- main.{signature.function_name}({params}) -> {signature.return_type}"
        )
    return tuple(formatted)


_SUM_OVERLOADS = _format_overloads(
    (*_SUM_GENERIC_SIGNATURES, *_SUM_NUMERIC_SIGNATURES)
)
_PRODUCT_OVERLOADS = _format_overloads(_PRODUCT_SIGNATURES)


def _build_docstring(
    function_name: str,
    description: str,
    overloads: Iterable[str],
    *,
    filter_variant: bool = False,
) -> str:
    header = f"Call DuckDB function ``{function_name}``"
    if filter_variant:
        header += " with ``FILTER``"
    header += "."
    formatted_overloads = "\n".join(overloads)
    return (
        f"{header}\n\n"
        f"{description}\n\n"
        "Overloads:\n"
        f"{formatted_overloads}\n"
    )


@register_duckdb_function("sum")
def sum(  # noqa: A002 - matching DuckDB helper name.
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``sum``."""

    signatures = getattr(type(self), "_SUM_SIGNATURES")
    return cast(
        NumericExpression,
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


@register_duckdb_function("sum_filter")
def sum_filter(  # noqa: A002 - matching DuckDB helper name.
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``sum`` with ``FILTER``."""

    signatures = getattr(type(self), "_SUM_SIGNATURES")
    return cast(
        NumericExpression,
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


@register_duckdb_function("product")
def product(
    self: "AggregateNumericFunctions",
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``product``."""

    return cast(
        NumericExpression,
        invoke_duckdb_function(
            _PRODUCT_SIGNATURES,
            return_category=self.return_category,
            operands=operands,
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("product_filter")
def product_filter(
    self: "AggregateNumericFunctions",
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> NumericExpression:
    """Call DuckDB function ``product`` with ``FILTER``."""

    return cast(
        NumericExpression,
        invoke_duckdb_filter_function(
            predicate,
            _PRODUCT_SIGNATURES,
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
    """Attach summation helpers onto aggregate namespaces."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
        AggregateNumericFunctions,
    )

    namespace_lookup: dict[str, Any] = {
        "AggregateGenericFunctions": AggregateGenericFunctions,
        "AggregateNumericFunctions": AggregateNumericFunctions,
    }

    for class_name, signatures in (
        ("AggregateGenericFunctions", _SUM_GENERIC_SIGNATURES),
        ("AggregateNumericFunctions", _SUM_NUMERIC_SIGNATURES),
    ):
        namespace = namespace_lookup[class_name]
        namespace._SUM_SIGNATURES = signatures
        namespace.sum = sum  # type: ignore[assignment]
        namespace.sum_filter = sum_filter  # type: ignore[assignment]
        namespace._register_function(
            "sum",
            names=getattr(sum, "__duckdb_identifiers__", ()),
            symbols=getattr(sum, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "sum_filter",
            names=getattr(sum_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(sum_filter, "__duckdb_symbols__", ()),
        )

    namespace_lookup["AggregateNumericFunctions"]._PRODUCT_SIGNATURES = (
        _PRODUCT_SIGNATURES
    )
    namespace_lookup["AggregateNumericFunctions"].product = product  # type: ignore[assignment]
    namespace_lookup["AggregateNumericFunctions"].product_filter = (
        product_filter  # type: ignore[assignment]
    )
    namespace_lookup["AggregateNumericFunctions"]._register_function(
        "product",
        names=getattr(product, "__duckdb_identifiers__", ()),
        symbols=getattr(product, "__duckdb_symbols__", ()),
    )
    namespace_lookup["AggregateNumericFunctions"]._register_function(
        "product_filter",
        names=getattr(product_filter, "__duckdb_identifiers__", ()),
        symbols=getattr(product_filter, "__duckdb_symbols__", ()),
    )


sum.__doc__ = _build_docstring("sum", _SUM_DESCRIPTION, _SUM_OVERLOADS)
sum_filter.__doc__ = _build_docstring(
    "sum",
    _SUM_DESCRIPTION,
    _SUM_OVERLOADS,
    filter_variant=True,
)
product.__doc__ = _build_docstring("product", _PRODUCT_DESCRIPTION, _PRODUCT_OVERLOADS)
product_filter.__doc__ = _build_docstring(
    "product",
    _PRODUCT_DESCRIPTION,
    _PRODUCT_OVERLOADS,
    filter_variant=True,
)


_register()


__all__ = [
    "sum",
    "sum_filter",
    "product",
    "product_filter",
]
