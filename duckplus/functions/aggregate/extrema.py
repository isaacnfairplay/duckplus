"""Decorator-backed helpers for ``min`` and ``max`` aggregates."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

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
        AggregateBlobFunctions,
        AggregateBooleanFunctions,
        AggregateGenericFunctions,
        AggregateNumericFunctions,
        AggregateVarcharFunctions,
    )


_MIN_DESCRIPTION = "Returns the minimum value present in arg."
_MAX_DESCRIPTION = "Returns the maximum value present in arg."

_NUMERIC_TYPE_NAMES: tuple[str, ...] = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
)


def _build_definition(
    function_name: str,
    return_type: str,
    parameter_types: tuple[str, ...],
    *,
    description: str,
    parameters: tuple[str, ...] | None = None,
) -> DuckDBFunctionDefinition:
    """Construct a DuckDB function definition for a single-namespace helper."""

    parsed_parameters = tuple(parse_type(type_name) for type_name in parameter_types)
    resolved_parameters = parameters or ("arg",) * len(parameter_types)
    return DuckDBFunctionDefinition(
        schema_name="main",
        function_name=function_name,
        function_type="aggregate",
        return_type=parse_type(return_type),
        parameter_types=parsed_parameters,
        parameters=resolved_parameters,
        varargs=None,
        description=description,
        comment=None,
        macro_definition=None,
    )


_GENERIC_MIN_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "min",
        "ANY",
        ("ANY",),
        description=_MIN_DESCRIPTION,
    ),
    _build_definition(
        "min",
        "ANY[]",
        ("ANY", "BIGINT"),
        parameters=("arg", "col1"),
        description=_MIN_DESCRIPTION,
    ),
)

_GENERIC_MAX_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "max",
        "ANY",
        ("ANY",),
        description=_MAX_DESCRIPTION,
    ),
    _build_definition(
        "max",
        "ANY[]",
        ("ANY", "BIGINT"),
        parameters=("arg", "col1"),
        description=_MAX_DESCRIPTION,
    ),
)

_BOOLEAN_MIN_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "min",
        "BOOLEAN",
        ("BOOLEAN",),
        description=_MIN_DESCRIPTION,
    ),
)

_BOOLEAN_MAX_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "max",
        "BOOLEAN",
        ("BOOLEAN",),
        description=_MAX_DESCRIPTION,
    ),
)

_VARCHAR_MIN_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "min",
        "VARCHAR",
        ("VARCHAR",),
        description=_MIN_DESCRIPTION,
    ),
)

_VARCHAR_MAX_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "max",
        "VARCHAR",
        ("VARCHAR",),
        description=_MAX_DESCRIPTION,
    ),
)

_BLOB_MIN_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "min",
        "BLOB",
        ("BLOB",),
        description=_MIN_DESCRIPTION,
    ),
)

_BLOB_MAX_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = (
    _build_definition(
        "max",
        "BLOB",
        ("BLOB",),
        description=_MAX_DESCRIPTION,
    ),
)

_NUMERIC_MIN_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = tuple(
    _build_definition("min", type_name, (type_name,), description=_MIN_DESCRIPTION)
    for type_name in _NUMERIC_TYPE_NAMES
)

_NUMERIC_MAX_SIGNATURES: tuple[DuckDBFunctionDefinition, ...] = tuple(
    _build_definition("max", type_name, (type_name,), description=_MAX_DESCRIPTION)
    for type_name in _NUMERIC_TYPE_NAMES
)


def _format_type(duck_type: object) -> str:
    type_name = str(duck_type)
    if type_name.startswith("ARRAY(") and type_name.endswith(")"):
        return f"{type_name[6:-1]}[]"
    return type_name


def _format_overloads(
    signatures: Iterable[DuckDBFunctionDefinition],
) -> tuple[str, ...]:
    formatted: list[str] = []
    for signature in signatures:
        params = ", ".join(
            f"{_format_type(param_type)} {name}"
            for param_type, name in zip(
                signature.parameter_types, signature.parameters
            )
        )
        formatted.append(
            f"- main.{signature.function_name}({params}) -> {_format_type(signature.return_type)}"
        )
    return tuple(formatted)


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


_MIN_OVERLOADS = _format_overloads(
    (
        *_GENERIC_MIN_SIGNATURES,
        *_BOOLEAN_MIN_SIGNATURES,
        *_NUMERIC_MIN_SIGNATURES,
        *_VARCHAR_MIN_SIGNATURES,
        *_BLOB_MIN_SIGNATURES,
    )
)

_MAX_OVERLOADS = _format_overloads(
    (
        *_GENERIC_MAX_SIGNATURES,
        *_BOOLEAN_MAX_SIGNATURES,
        *_NUMERIC_MAX_SIGNATURES,
        *_VARCHAR_MAX_SIGNATURES,
        *_BLOB_MAX_SIGNATURES,
    )
)


@register_duckdb_function("max")
def max(  # noqa: A002 - matching DuckDB helper name.
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``max``."""

    signatures = getattr(type(self), "_MAX_SIGNATURES")
    return cast(
        TypedExpression,
        invoke_duckdb_function(
            signatures,
            return_category=self.return_category,
            operands=tuple(operands),
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("max_filter")
def max_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``max`` with ``FILTER``."""

    signatures = getattr(type(self), "_MAX_SIGNATURES")
    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            signatures,
            return_category=self.return_category,
            operands=tuple(operands),
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("min")
def min(  # noqa: A002 - matching DuckDB helper name.
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``min``."""

    signatures = getattr(type(self), "_MIN_SIGNATURES")
    return cast(
        TypedExpression,
        invoke_duckdb_function(
            signatures,
            return_category=self.return_category,
            operands=tuple(operands),
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


@register_duckdb_function("min_filter")
def min_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    """Call DuckDB function ``min`` with ``FILTER``."""

    signatures = getattr(type(self), "_MIN_SIGNATURES")
    return cast(
        TypedExpression,
        invoke_duckdb_filter_function(
            predicate,
            signatures,
            return_category=self.return_category,
            operands=tuple(operands),
            order_by=order_by,
            within_group=within_group,
            partition_by=partition_by,
            over_order_by=over_order_by,
            frame=frame,
        ),
    )


max.__doc__ = _build_docstring("max", _MAX_DESCRIPTION, _MAX_OVERLOADS)
max_filter.__doc__ = _build_docstring(
    "max",
    _MAX_DESCRIPTION,
    _MAX_OVERLOADS,
    filter_variant=True,
)
min.__doc__ = _build_docstring("min", _MIN_DESCRIPTION, _MIN_OVERLOADS)
min_filter.__doc__ = _build_docstring(
    "min",
    _MIN_DESCRIPTION,
    _MIN_OVERLOADS,
    filter_variant=True,
)


def _register() -> None:
    """Attach ``min``/``max`` helpers onto aggregate namespaces."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateBlobFunctions,
        AggregateBooleanFunctions,
        AggregateGenericFunctions,
        AggregateNumericFunctions,
        AggregateVarcharFunctions,
    )

    namespace_lookup: dict[str, Any] = {
        "AggregateGenericFunctions": AggregateGenericFunctions,
        "AggregateNumericFunctions": AggregateNumericFunctions,
        "AggregateVarcharFunctions": AggregateVarcharFunctions,
        "AggregateBlobFunctions": AggregateBlobFunctions,
        "AggregateBooleanFunctions": AggregateBooleanFunctions,
    }

    signature_lookup: dict[str, tuple[
        tuple[DuckDBFunctionDefinition, ...],
        tuple[DuckDBFunctionDefinition, ...],
    ]] = {
        "AggregateGenericFunctions": (
            _GENERIC_MIN_SIGNATURES,
            _GENERIC_MAX_SIGNATURES,
        ),
        "AggregateNumericFunctions": (
            _NUMERIC_MIN_SIGNATURES,
            _NUMERIC_MAX_SIGNATURES,
        ),
        "AggregateVarcharFunctions": (
            _VARCHAR_MIN_SIGNATURES,
            _VARCHAR_MAX_SIGNATURES,
        ),
        "AggregateBlobFunctions": (
            _BLOB_MIN_SIGNATURES,
            _BLOB_MAX_SIGNATURES,
        ),
        "AggregateBooleanFunctions": (
            _BOOLEAN_MIN_SIGNATURES,
            _BOOLEAN_MAX_SIGNATURES,
        ),
    }

    for class_name, (min_signatures, max_signatures) in signature_lookup.items():
        namespace = namespace_lookup[class_name]
        namespace._MIN_SIGNATURES = min_signatures
        namespace._MAX_SIGNATURES = max_signatures
        namespace.min = min  # type: ignore[assignment]
        namespace.min_filter = min_filter  # type: ignore[assignment]
        namespace.max = max  # type: ignore[assignment]
        namespace.max_filter = max_filter  # type: ignore[assignment]
        for attribute_name in ("min", "min_filter", "max", "max_filter"):
            helper = globals()[attribute_name]
            namespace._register_function(
                attribute_name,
                names=getattr(helper, "__duckdb_identifiers__", ()),
                symbols=getattr(helper, "__duckdb_symbols__", ()),
            )


_register()


__all__ = [
    "max",
    "max_filter",
    "min",
    "min_filter",
]
