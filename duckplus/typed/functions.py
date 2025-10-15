"""Static DuckDB function catalog to power typed expression helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, Iterable, Mapping, Sequence, Tuple

from .expression import (
    BlobExpression,
    BooleanExpression,
    GenericExpression,
    NumericExpression,
    TypedExpression,
    VarcharExpression,
)
from .expression import _quote_identifier  # pylint: disable=protected-access


@dataclass(frozen=True)
class DuckDBFunctionDefinition:
    """Captured metadata describing a DuckDB function overload."""

    schema_name: str
    function_name: str
    function_type: str
    return_type: str | None
    parameter_types: tuple[str, ...]
    varargs: str | None

    def matches_arity(self, argument_count: int) -> bool:
        """Return whether this overload accepts the provided argument count."""

        required = len(self.parameter_types)
        if self.varargs is None:
            return argument_count == required
        return argument_count >= required


@dataclass(frozen=True)
class DuckDBFunctionSignature:
    """Structured view of a DuckDB function's callable surface."""

    schema_name: str
    function_name: str
    function_type: str
    return_type: str | None
    parameter_types: tuple[str, ...]
    varargs: str | None
    sql_name: str

    def call_syntax(self) -> str:
        """Render the SQL call including argument placeholders."""

        arguments = list(self.parameter_types)
        if self.varargs:
            arguments.append(f"{self.varargs}...")
        joined_arguments = ", ".join(arguments)
        return f"{self.sql_name}({joined_arguments})"

    def return_annotation(self) -> str:
        """Return the upper-cased DuckDB return annotation."""

        return (self.return_type or "UNKNOWN").upper()

    def __str__(self) -> str:  # pragma: no cover - trivial wrapper
        return f"{self.call_syntax()} -> {self.return_annotation()}"


class _DuckDBFunctionCall:
    """Callable wrapper that renders a function invocation."""

    def __init__(
        self,
        overloads: Sequence[DuckDBFunctionDefinition],
        *,
        return_category: str,
    ) -> None:
        if not overloads:
            msg = "Function call requires at least one overload"
            raise ValueError(msg)
        self._overloads = tuple(overloads)
        self._return_category = return_category
        self._function_type = overloads[0].function_type
        self._signatures = tuple(
            _definition_to_signature(overload) for overload in overloads
        )
        self.__doc__ = _format_function_docstring(
            self._signatures,
            return_category=self._return_category,
        )

    def __call__(self, *operands: object) -> TypedExpression:
        arguments = [_coerce_function_operand(operand) for operand in operands]
        dependencies = _merge_dependencies(arguments)
        overload = self._select_overload(len(arguments))
        sql_name = _render_function_name(overload)
        rendered_args = ", ".join(argument.render() for argument in arguments)
        sql = f"{sql_name}({rendered_args})" if arguments else f"{sql_name}()"
        return _construct_expression(
            sql,
            return_type=overload.return_type,
            dependencies=dependencies,
            category=self._return_category,
        )

    def _select_overload(self, argument_count: int) -> DuckDBFunctionDefinition:
        for overload in self._overloads:
            if overload.matches_arity(argument_count):
                return overload
        return self._overloads[0]

    @property
    def signatures(self) -> Tuple[DuckDBFunctionSignature, ...]:
        """Return structured signatures for all overloads."""

        return self._signatures

    @property
    def function_type(self) -> str:
        """Expose the DuckDB function type (scalar/aggregate/window)."""

        return self._function_type


class _StaticFunctionNamespace:
    """Base class for generated DuckDB function namespaces."""

    __slots__ = ()
    function_type: ClassVar[str]
    return_category: ClassVar[str]
    _IDENTIFIER_FUNCTIONS: ClassVar[Mapping[str, _DuckDBFunctionCall]] = {}
    _SYMBOLIC_FUNCTIONS: ClassVar[Mapping[str, _DuckDBFunctionCall]] = {}

    def __getitem__(self, name: str) -> _DuckDBFunctionCall:
        try:
            return self._IDENTIFIER_FUNCTIONS[name]
        except KeyError:
            try:
                return self._SYMBOLIC_FUNCTIONS[name]
            except KeyError as error:
                raise KeyError(name) from error

    def get(
        self,
        name: str,
        default: _DuckDBFunctionCall | None = None,
    ) -> _DuckDBFunctionCall | None:
        """Return the function call for ``name`` if present, else ``default``."""

        try:
            return self[name]
        except KeyError:
            return default

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        return name in self._IDENTIFIER_FUNCTIONS or name in self._SYMBOLIC_FUNCTIONS

    def __dir__(self) -> list[str]:
        return sorted(self._IDENTIFIER_FUNCTIONS)

    @property
    def symbols(self) -> Mapping[str, _DuckDBFunctionCall]:
        """Mapping of non-identifier function names (operators)."""

        return self._SYMBOLIC_FUNCTIONS


def _coerce_function_operand(value: object) -> TypedExpression:
    if isinstance(value, TypedExpression):
        return value
    if isinstance(value, bool):
        return BooleanExpression("TRUE" if value else "FALSE")
    if isinstance(value, (int, float, Decimal)):
        return NumericExpression.literal(value)
    if isinstance(value, bytes):
        return BlobExpression.literal(value)
    if isinstance(value, str):
        return GenericExpression(
            _quote_identifier(value),
            type_annotation="IDENTIFIER",
            dependencies=(value,),
        )
    if value is None:
        return GenericExpression("NULL")
    msg = "DuckDB function arguments must be typed expressions or supported literals"
    raise TypeError(msg)


def _merge_dependencies(expressions: Iterable[TypedExpression]) -> frozenset[str]:
    dependencies: set[str] = set()
    for expression in expressions:
        dependencies.update(expression.dependencies)
    return frozenset(dependencies)


def _render_function_name(definition: DuckDBFunctionDefinition) -> str:
    schema = definition.schema_name
    name = definition.function_name
    if schema in ("main", "pg_catalog"):
        return _quote_identifier(name) if name != name.lower() else name
    return f"{_quote_identifier(schema)}.{_quote_identifier(name)}"


def _definition_to_signature(
    definition: DuckDBFunctionDefinition,
) -> DuckDBFunctionSignature:
    return DuckDBFunctionSignature(
        schema_name=definition.schema_name,
        function_name=definition.function_name,
        function_type=definition.function_type,
        return_type=definition.return_type,
        parameter_types=definition.parameter_types,
        varargs=definition.varargs,
        sql_name=_render_function_name(definition),
    )


def _format_function_docstring(
    signatures: Sequence[DuckDBFunctionSignature],
    *,
    return_category: str,
) -> str:
    overload_count = len(signatures)
    overload_text = "overload" if overload_count == 1 else "overloads"
    header = (
        f"DuckDB {signatures[0].function_type} function returning {return_category} "
        f"results with {overload_count} {overload_text}."
    )
    lines = [header, "", "Overloads:"]
    lines.extend(f"- {signature}" for signature in signatures)
    return "\n".join(lines)


def _construct_expression(
    sql: str,
    *,
    return_type: str | None,
    dependencies: frozenset[str],
    category: str,
) -> TypedExpression:
    annotation = (return_type or "UNKNOWN").upper()
    if category == "numeric":
        return NumericExpression(sql, dependencies=dependencies, type_annotation=annotation)
    if category == "boolean":
        return BooleanExpression(sql, dependencies=dependencies, type_annotation=annotation)
    if category == "varchar":
        return VarcharExpression(sql, dependencies=dependencies, type_annotation=annotation)
    if category == "blob":
        return BlobExpression(sql, dependencies=dependencies, type_annotation=annotation)
    return GenericExpression(sql, dependencies=dependencies, type_annotation=annotation)


from ._generated_function_namespaces import (  # noqa: E402  pylint: disable=wrong-import-position
    AggregateFunctionNamespace,
    ScalarFunctionNamespace,
    WindowFunctionNamespace,
)


# pylint: disable=invalid-name
class DuckDBFunctionNamespace:  # pylint: disable=too-few-public-methods
    """Static access to DuckDB's scalar, aggregate, and window functions."""

    Scalar: ScalarFunctionNamespace  # pylint: disable=invalid-name
    Aggregate: AggregateFunctionNamespace  # pylint: disable=invalid-name
    Window: WindowFunctionNamespace  # pylint: disable=invalid-name

    def __init__(self) -> None:
        self.Scalar = ScalarFunctionNamespace()
        self.Aggregate = AggregateFunctionNamespace()
        self.Window = WindowFunctionNamespace()

    def __dir__(self) -> list[str]:
        return ["Aggregate", "Scalar", "Window"]


# pylint: enable=invalid-name
SCALAR_FUNCTIONS = ScalarFunctionNamespace()
AGGREGATE_FUNCTIONS = AggregateFunctionNamespace()
WINDOW_FUNCTIONS = WindowFunctionNamespace()


# pylint: disable=duplicate-code
__all__ = [
    "DuckDBFunctionDefinition",
    "DuckDBFunctionNamespace",
    "DuckDBFunctionSignature",
    "SCALAR_FUNCTIONS",
    "AGGREGATE_FUNCTIONS",
    "WINDOW_FUNCTIONS",
]
