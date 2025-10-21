"""Decorator-backed arg-extrema aggregate helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from importlib import import_module
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
    class_name: str
    base_configs: tuple[ReturnSignatureConfig, ...]
    null_configs: tuple[ReturnSignatureConfig, ...] | None = None

    def configs(self, *, null_variant: bool = False) -> tuple[ReturnSignatureConfig, ...]:
        if null_variant and self.null_configs is not None:
            return self.null_configs
        return self.base_configs


_GENERIC_BASE_CONFIGS: tuple[ReturnSignatureConfig, ...] = (
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
        base_configs=(ReturnSignatureConfig(return_type="BLOB"),),
    ),
    NamespaceConfig(
        "AggregateVarcharFunctions",
        base_configs=(ReturnSignatureConfig(return_type="VARCHAR"),),
    ),
    NamespaceConfig(
        "AggregateNumericFunctions",
        base_configs=(
            ReturnSignatureConfig(return_type="INTEGER"),
            ReturnSignatureConfig(return_type="BIGINT"),
            ReturnSignatureConfig(return_type="DOUBLE"),
            ReturnSignatureConfig(return_type="DECIMAL"),
        ),
    ),
    NamespaceConfig(
        "AggregateGenericFunctions",
        base_configs=_GENERIC_BASE_CONFIGS,
        null_configs=_GENERIC_BASE_CONFIGS[:-1],
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


def _format_overloads(function_name: str, *, null_variant: bool = False) -> str:
    lines: list[str] = []
    for namespace_config in _NAMESPACE_CONFIGS:
        for return_config in namespace_config.configs(null_variant=null_variant):
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
                    f"    - main.{function_name}({', '.join(parameters)}) -> {_format_type(return_config.return_type)}"
                )
    return "\n".join(lines)


_ARG_MAX_DESCRIPTION = (
    "Finds the row with the maximum val. Calculates the non-NULL arg expression"
    " at that row."
)
_ARG_MAX_NULL_DESCRIPTION = (
    "Finds the row with the maximum val. Calculates the arg expression at that"
    " row."
)
_ARG_MIN_DESCRIPTION = (
    "Finds the row with the minimum val. Calculates the non-NULL arg expression"
    " at that row."
)
_ARG_MIN_NULL_DESCRIPTION = (
    "Finds the row with the minimum val. Calculates the arg expression at that"
    " row."
)


_ARG_MAX_OVERLOADS = _format_overloads("arg_max")
_ARG_MAX_NULL_OVERLOADS = _format_overloads("arg_max_null", null_variant=True)
_ARG_MIN_OVERLOADS = _format_overloads("arg_min")
_ARG_MIN_NULL_OVERLOADS = _format_overloads("arg_min_null", null_variant=True)


def _build_docstring(
    function_name: str,
    description: str,
    overloads: str,
    *,
    filter_variant: bool = False,
) -> str:
    header = f"Call DuckDB function ``{function_name}``"
    if filter_variant:
        header += " with ``FILTER``"
    header += "."
    return (
        f"{header}\n\n"
        f"{description}\n\n"
        "Overloads:\n"
        f"{overloads}\n"
    )


@register_duckdb_function("arg_max")
def arg_max(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MAX_SIGNATURES")
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


@register_duckdb_function("arg_max_filter")
def arg_max_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MAX_SIGNATURES")
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


@register_duckdb_function("arg_max_null")
def arg_max_null(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MAX_NULL_SIGNATURES")
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


@register_duckdb_function("arg_max_null_filter")
def arg_max_null_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MAX_NULL_SIGNATURES")
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


@register_duckdb_function("arg_min")
def arg_min(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MIN_SIGNATURES")
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


@register_duckdb_function("arg_min_filter")
def arg_min_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MIN_SIGNATURES")
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


@register_duckdb_function("arg_min_null")
def arg_min_null(
    self: Any,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MIN_NULL_SIGNATURES")
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


@register_duckdb_function("arg_min_null_filter")
def arg_min_null_filter(
    self: Any,
    predicate: object,
    *operands: object,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    signatures = getattr(type(self), "_ARG_MIN_NULL_SIGNATURES")
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


arg_max.__doc__ = _build_docstring("arg_max", _ARG_MAX_DESCRIPTION, _ARG_MAX_OVERLOADS)
arg_max_filter.__doc__ = _build_docstring(
    "arg_max",
    _ARG_MAX_DESCRIPTION,
    _ARG_MAX_OVERLOADS,
    filter_variant=True,
)
arg_max_null.__doc__ = _build_docstring(
    "arg_max_null",
    _ARG_MAX_NULL_DESCRIPTION,
    _ARG_MAX_NULL_OVERLOADS,
)
arg_max_null_filter.__doc__ = _build_docstring(
    "arg_max_null",
    _ARG_MAX_NULL_DESCRIPTION,
    _ARG_MAX_NULL_OVERLOADS,
    filter_variant=True,
)
arg_min.__doc__ = _build_docstring("arg_min", _ARG_MIN_DESCRIPTION, _ARG_MIN_OVERLOADS)
arg_min_filter.__doc__ = _build_docstring(
    "arg_min",
    _ARG_MIN_DESCRIPTION,
    _ARG_MIN_OVERLOADS,
    filter_variant=True,
)
arg_min_null.__doc__ = _build_docstring(
    "arg_min_null",
    _ARG_MIN_NULL_DESCRIPTION,
    _ARG_MIN_NULL_OVERLOADS,
)
arg_min_null_filter.__doc__ = _build_docstring(
    "arg_min_null",
    _ARG_MIN_NULL_DESCRIPTION,
    _ARG_MIN_NULL_OVERLOADS,
    filter_variant=True,
)


def _collect_signatures(
    *,
    function_name: str,
    description: str,
    configs: tuple[ReturnSignatureConfig, ...],
) -> tuple[DuckDBFunctionDefinition, ...]:
    return tuple(
        definition
        for config in configs
        for definition in _make_signatures(
            function_name,
            description,
            config=config,
        )
    )


def _register() -> None:
    """Attach arg-extrema helpers onto all aggregate namespaces."""

    namespace_module = import_module(
        "duckplus.static_typed._generated_function_namespaces"
    )

    for namespace_config in _NAMESPACE_CONFIGS:
        namespace: Any = getattr(namespace_module, namespace_config.class_name)

        namespace._ARG_MAX_SIGNATURES = _collect_signatures(
            function_name="arg_max",
            description=_ARG_MAX_DESCRIPTION,
            configs=namespace_config.configs(),
        )
        namespace._ARG_MAX_NULL_SIGNATURES = _collect_signatures(
            function_name="arg_max_null",
            description=_ARG_MAX_NULL_DESCRIPTION,
            configs=namespace_config.configs(null_variant=True),
        )
        namespace._ARG_MIN_SIGNATURES = _collect_signatures(
            function_name="arg_min",
            description=_ARG_MIN_DESCRIPTION,
            configs=namespace_config.configs(),
        )
        namespace._ARG_MIN_NULL_SIGNATURES = _collect_signatures(
            function_name="arg_min_null",
            description=_ARG_MIN_NULL_DESCRIPTION,
            configs=namespace_config.configs(null_variant=True),
        )

        namespace.arg_max = arg_max  # type: ignore[assignment]
        namespace.arg_max_filter = arg_max_filter  # type: ignore[assignment]
        namespace.arg_max_null = arg_max_null  # type: ignore[assignment]
        namespace.arg_max_null_filter = arg_max_null_filter  # type: ignore[assignment]
        namespace.arg_min = arg_min  # type: ignore[assignment]
        namespace.arg_min_filter = arg_min_filter  # type: ignore[assignment]
        namespace.arg_min_null = arg_min_null  # type: ignore[assignment]
        namespace.arg_min_null_filter = arg_min_null_filter  # type: ignore[assignment]

        namespace._register_function(
            "arg_max",
            names=getattr(arg_max, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_max, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_max_filter",
            names=getattr(arg_max_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_max_filter, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_max_null",
            names=getattr(arg_max_null, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_max_null, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_max_null_filter",
            names=getattr(arg_max_null_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_max_null_filter, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_min",
            names=getattr(arg_min, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_min, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_min_filter",
            names=getattr(arg_min_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_min_filter, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_min_null",
            names=getattr(arg_min_null, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_min_null, "__duckdb_symbols__", ()),
        )
        namespace._register_function(
            "arg_min_null_filter",
            names=getattr(arg_min_null_filter, "__duckdb_identifiers__", ()),
            symbols=getattr(arg_min_null_filter, "__duckdb_symbols__", ()),
        )


_register()


__all__ = [
    "arg_max",
    "arg_max_filter",
    "arg_max_null",
    "arg_max_null_filter",
    "arg_min",
    "arg_min_filter",
    "arg_min_null",
    "arg_min_null_filter",
]
