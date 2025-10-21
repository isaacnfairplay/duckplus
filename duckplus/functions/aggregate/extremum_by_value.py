"""Decorator-backed helpers for DuckDB's ``max_by``/``min_by`` aggregates."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
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
        AggregateGenericFunctions,
        AggregateNumericFunctions,
        AggregateVarcharFunctions,
    )


_ARG_VALUE_TYPES: tuple[str, ...] = (
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "DOUBLE",
    "VARCHAR",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "BLOB",
)


@dataclass(frozen=True)
class ReturnSignatureConfig:
    """Configuration describing the overloads for a return category."""

    return_type: str
    arg_type: str | None = None
    value_types: tuple[str, ...] = _ARG_VALUE_TYPES
    extra_parameter_types: tuple[str, ...] = ()
    extra_parameter_names: tuple[str, ...] | None = None

    def parameter_names(self) -> tuple[str, ...]:
        names: tuple[str, ...] = ("arg", "val")
        if not self.extra_parameter_types:
            return names
        if self.extra_parameter_names is not None:
            return names + self.extra_parameter_names
        extras = tuple(
            f"col{index}" for index in range(2, 2 + len(self.extra_parameter_types))
        )
        return names + extras


@dataclass(frozen=True)
class NamespaceConfig:
    """Configuration tying overloads to a generated namespace."""

    class_name: str
    return_configs: tuple[ReturnSignatureConfig, ...]


_GENERIC_RETURN_CONFIGS: tuple[ReturnSignatureConfig, ...] = (
    ReturnSignatureConfig(return_type="DATE"),
    ReturnSignatureConfig(return_type="TIMESTAMP"),
    ReturnSignatureConfig(return_type="TIMESTAMP WITH TIME ZONE"),
    ReturnSignatureConfig(
        return_type="ANY",
        value_types=_ARG_VALUE_TYPES + ("ANY",),
    ),
    ReturnSignatureConfig(
        return_type="ARRAY(ANY)",
        arg_type="ANY",
        value_types=("ANY",),
        extra_parameter_types=("BIGINT",),
        extra_parameter_names=("col2",),
    ),
)

_NAMESPACE_CONFIGS: tuple[NamespaceConfig, ...] = (
    NamespaceConfig(
        "AggregateBlobFunctions",
        return_configs=(ReturnSignatureConfig(return_type="BLOB"),),
    ),
    NamespaceConfig(
        "AggregateVarcharFunctions",
        return_configs=(ReturnSignatureConfig(return_type="VARCHAR"),),
    ),
    NamespaceConfig(
        "AggregateNumericFunctions",
        return_configs=(
            ReturnSignatureConfig(return_type="INTEGER"),
            ReturnSignatureConfig(return_type="BIGINT"),
            ReturnSignatureConfig(return_type="DOUBLE"),
            ReturnSignatureConfig(return_type="DECIMAL"),
        ),
    ),
    NamespaceConfig(
        "AggregateGenericFunctions",
        return_configs=_GENERIC_RETURN_CONFIGS,
    ),
)


def _format_type(type_name: str) -> str:
    if type_name.startswith("ARRAY(") and type_name.endswith(")"):
        return f"{type_name[6:-1]}[]"
    return type_name


def _make_signatures(
    function_name: str,
    description: str,
    *,
    config: ReturnSignatureConfig,
) -> tuple[DuckDBFunctionDefinition, ...]:
    arg_type = config.arg_type or config.return_type
    parsed_return_type = parse_type(config.return_type)
    parsed_arg_type = parse_type(arg_type)
    parsed_extra_types = tuple(parse_type(t) for t in config.extra_parameter_types)
    parameter_names = config.parameter_names()

    definitions: list[DuckDBFunctionDefinition] = []
    for val_type in config.value_types:
        parameter_types = (
            parsed_arg_type,
            parse_type(val_type),
            *parsed_extra_types,
        )
        definitions.append(
            DuckDBFunctionDefinition(
                schema_name="main",
                function_name=function_name,
                function_type="aggregate",
                return_type=parsed_return_type,
                parameter_types=parameter_types,
                parameters=parameter_names,
                varargs=None,
                description=description,
                comment=None,
                macro_definition=None,
            )
        )
    return tuple(definitions)


def _format_overloads(function_name: str) -> str:
    lines: list[str] = []
    for namespace_config in _NAMESPACE_CONFIGS:
        for return_config in namespace_config.return_configs:
            arg_type = return_config.arg_type or return_config.return_type
            parameter_names = return_config.parameter_names()
            extra_types = return_config.extra_parameter_types
            extra_names = parameter_names[2:]
            for val_type in return_config.value_types:
                parameters = [
                    f"{_format_type(arg_type)} {parameter_names[0]}",
                    f"{_format_type(val_type)} {parameter_names[1]}",
                ]
                parameters.extend(
                    f"{_format_type(extra_type)} {extra_name}"
                    for extra_name, extra_type in zip(extra_names, extra_types)
                )
                lines.append(
                    f"    - main.{function_name}({', '.join(parameters)}) -> "
                    f"{_format_type(return_config.return_type)}"
                )
    return "\n".join(lines)


_MAX_BY_DESCRIPTION = (
    "Finds the row with the maximum val. Calculates the non-NULL arg "
    "expression at that row."
)
_MIN_BY_DESCRIPTION = (
    "Finds the row with the minimum val. Calculates the non-NULL arg "
    "expression at that row."
)


_MAX_BY_OVERLOADS = _format_overloads("max_by")
_MIN_BY_OVERLOADS = _format_overloads("min_by")


def _build_docstring(
    function_name: str,
    description: str,
    overloads: str,
    *,
    filter_variant: bool = False,
) -> str:
    header = (
        f"Call DuckDB function ``{function_name}`` with ``FILTER``."
        if filter_variant
        else f"Call DuckDB function ``{function_name}``."
    )
    return (
        f"{header}\n\n"
        f"{description}\n\n"
        "Overloads:\n"
        f"{overloads}"
    )


def _build_signature_map(
    function_name: str,
    description: str,
) -> dict[str, tuple[DuckDBFunctionDefinition, ...]]:
    signatures: dict[str, tuple[DuckDBFunctionDefinition, ...]] = {}
    for namespace_config in _NAMESPACE_CONFIGS:
        namespace_signatures: list[DuckDBFunctionDefinition] = []
        for return_config in namespace_config.return_configs:
            namespace_signatures.extend(
                _make_signatures(function_name, description, config=return_config)
            )
        signatures[namespace_config.class_name] = tuple(namespace_signatures)
    return signatures


_MAX_BY_SIGNATURES = _build_signature_map("max_by", _MAX_BY_DESCRIPTION)
_MIN_BY_SIGNATURES = _build_signature_map("min_by", _MIN_BY_DESCRIPTION)


@register_duckdb_function("max_by")
def max_by(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_MAX_BY_SIGNATURES")
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


@register_duckdb_function("max_by_filter")
def max_by_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_MAX_BY_SIGNATURES")
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


@register_duckdb_function("min_by")
def min_by(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_MIN_BY_SIGNATURES")
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


@register_duckdb_function("min_by_filter")
def min_by_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_MIN_BY_SIGNATURES")
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
    """Attach ``max_by``/``min_by`` helpers onto aggregate namespaces."""

    from duckplus.static_typed._generated_function_namespaces import (
        AggregateBlobFunctions,
        AggregateGenericFunctions,
        AggregateNumericFunctions,
        AggregateVarcharFunctions,
    )

    namespace_lookup: dict[str, Any] = {
        "AggregateBlobFunctions": AggregateBlobFunctions,
        "AggregateVarcharFunctions": AggregateVarcharFunctions,
        "AggregateNumericFunctions": AggregateNumericFunctions,
        "AggregateGenericFunctions": AggregateGenericFunctions,
    }

    for namespace_config in _NAMESPACE_CONFIGS:
        namespace = namespace_lookup[namespace_config.class_name]
        namespace._MAX_BY_SIGNATURES = _MAX_BY_SIGNATURES[namespace_config.class_name]
        namespace._MIN_BY_SIGNATURES = _MIN_BY_SIGNATURES[namespace_config.class_name]
        namespace.max_by = max_by  # type: ignore[assignment]
        namespace.max_by_filter = max_by_filter  # type: ignore[assignment]
        namespace.min_by = min_by  # type: ignore[assignment]
        namespace.min_by_filter = min_by_filter  # type: ignore[assignment]
        namespace._register_function(
            "max_by",
            names=getattr(max_by, "__duckdb_identifiers__", ()),
            symbols=getattr(max_by, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "max_by_filter",
            names=getattr(max_by_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(max_by_filter, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "min_by",
            names=getattr(min_by, "__duckdb_identifiers__", ()),
            symbols=getattr(min_by, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "min_by_filter",
            names=getattr(min_by_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(min_by_filter, "__duckdb_symbols__", ()),
        )


max_by.__doc__ = _build_docstring(
    "max_by", _MAX_BY_DESCRIPTION, _MAX_BY_OVERLOADS
)
max_by_filter.__doc__ = _build_docstring(
    "max_by",
    _MAX_BY_DESCRIPTION,
    _MAX_BY_OVERLOADS,
    filter_variant=True,
)
min_by.__doc__ = _build_docstring("min_by", _MIN_BY_DESCRIPTION, _MIN_BY_OVERLOADS)
min_by_filter.__doc__ = _build_docstring(
    "min_by",
    _MIN_BY_DESCRIPTION,
    _MIN_BY_OVERLOADS,
    filter_variant=True,
)


_register()


__all__ = [
    "max_by",
    "max_by_filter",
    "min_by",
    "min_by_filter",
]
