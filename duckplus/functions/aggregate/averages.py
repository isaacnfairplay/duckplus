"""Average aggregate helpers."""

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
        AggregateNumericFunctions,
    )


_AVERAGE_DESCRIPTION = "Calculates the average value for all tuples in x."


_AVERAGE_GENERIC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("TIMESTAMP"),
        parameter_types=(parse_type("TIMESTAMP"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("TIMESTAMP WITH TIME ZONE"),
        parameter_types=(parse_type("TIMESTAMP WITH TIME ZONE"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("TIME"),
        parameter_types=(parse_type("TIME"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("TIME WITH TIME ZONE"),
        parameter_types=(parse_type("TIME WITH TIME ZONE"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
)


_AVERAGE_NUMERIC_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("DECIMAL"),
        parameter_types=(parse_type("DECIMAL"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("SMALLINT"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("INTEGER"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("BIGINT"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("HUGEINT"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("INTERVAL"),
        parameter_types=(parse_type("INTERVAL"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
    DuckDBFunctionDefinition(
        schema_name="main",
        function_name="avg",
        function_type="aggregate",
        return_type=parse_type("DOUBLE"),
        parameter_types=(parse_type("DOUBLE"),),
        parameters=("x",),
        varargs=None,
        description=_AVERAGE_DESCRIPTION,
        comment=None,
        macro_definition=None,
    ),
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
        formatted.append(
            f"- main.{signature.function_name}({params}) -> {signature.return_type}"
        )
    return tuple(formatted)


_AVG_OVERLOADS = _format_overloads(
    (*_AVERAGE_GENERIC_SIGNATURES, *_AVERAGE_NUMERIC_SIGNATURES)
)

def _build_docstring(function_name: str, *, filter_variant: bool = False) -> str:
    header = f"Call DuckDB function ``{function_name}``"
    if filter_variant:
        header += " with ``FILTER``"
    header += "."
    return (
        f"{header}\n\n"
        f"{_AVERAGE_DESCRIPTION}\n\n"
        "Overloads:\n"
        + "\n".join(_AVG_OVERLOADS)
        + "\n"
    )


@register_duckdb_function("avg")
def avg(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``avg``."""

    signatures = cast(
        tuple[DuckDBFunctionDefinition, ...],
        getattr(self, "_AVG_SIGNATURES"),
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


@register_duckdb_function("avg_filter")
def avg_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``avg`` with ``FILTER``."""

    signatures = cast(
        tuple[DuckDBFunctionDefinition, ...],
        getattr(self, "_AVG_SIGNATURES"),
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


@register_duckdb_function("mean")
def mean(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``mean``."""

    signatures = cast(
        tuple[DuckDBFunctionDefinition, ...],
        getattr(self, "_MEAN_SIGNATURES"),
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


@register_duckdb_function("mean_filter")
def mean_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``mean`` with ``FILTER``."""

    signatures = cast(
        tuple[DuckDBFunctionDefinition, ...],
        getattr(self, "_MEAN_SIGNATURES"),
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


def _register() -> None:
    """Attach average helpers onto the aggregate namespaces."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateGenericFunctions,
        AggregateNumericFunctions,
    )

    namespace_lookup: dict[str, Any] = {
        "AggregateGenericFunctions": AggregateGenericFunctions,
        "AggregateNumericFunctions": AggregateNumericFunctions,
    }

    for class_name, signatures in (
        ("AggregateGenericFunctions", _AVERAGE_GENERIC_SIGNATURES),
        ("AggregateNumericFunctions", _AVERAGE_NUMERIC_SIGNATURES),
    ):
        namespace = namespace_lookup[class_name]
        namespace._AVG_SIGNATURES = signatures
        namespace.avg = avg  # type: ignore[assignment]
        namespace.avg_filter = avg_filter  # type: ignore[assignment]
        namespace._register_function(
            "avg",
            names=getattr(avg, "__duckdb_identifiers__", ()),
            symbols=getattr(avg, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "avg_filter",
            names=getattr(avg_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(avg_filter, "__duckdb_symbols__", ()),
        )

    for class_name, signatures in (
        ("AggregateGenericFunctions", _AVERAGE_GENERIC_SIGNATURES),
        ("AggregateNumericFunctions", _AVERAGE_NUMERIC_SIGNATURES),
    ):
        namespace = namespace_lookup[class_name]
        namespace._MEAN_SIGNATURES = signatures
        namespace.mean = mean  # type: ignore[assignment]
        namespace.mean_filter = mean_filter  # type: ignore[assignment]
        namespace._register_function(
            "mean",
            names=getattr(mean, "__duckdb_identifiers__", ()),
            symbols=getattr(mean, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "mean_filter",
            names=getattr(mean_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(mean_filter, "__duckdb_symbols__", ()),
        )


avg.__doc__ = _build_docstring("avg")
avg_filter.__doc__ = _build_docstring("avg", filter_variant=True)
mean.__doc__ = _build_docstring("mean")
mean_filter.__doc__ = _build_docstring("mean", filter_variant=True)

_register()


__all__ = [
    "avg",
    "avg_filter",
    "mean",
    "mean_filter",
]
