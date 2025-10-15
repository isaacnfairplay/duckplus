"""Static DuckDB function catalog to power typed expression helpers."""

# pylint: disable=too-many-arguments,too-few-public-methods,invalid-name,too-many-return-statements

from __future__ import annotations

from decimal import Decimal
from typing import ClassVar, Iterable, Mapping, Sequence, Tuple

from .dependencies import ExpressionDependency
from .expression import (
    BlobExpression,
    BooleanExpression,
    GenericExpression,
    NumericExpression,
    TypedExpression,
    VarcharExpression,
)
from .expressions.utils import quote_identifier, quote_qualified_identifier
from .types import DuckDBType, GenericType, IdentifierType, UnknownType


class DuckDBFunctionDefinition:
    """Captured metadata describing a DuckDB function overload."""

    __slots__ = (
        "schema_name",
        "function_name",
        "function_type",
        "return_type",
        "parameter_types",
        "varargs",
    )

    def __init__(
        self,
        *,
        schema_name: str,
        function_name: str,
        function_type: str,
        return_type: DuckDBType | None,
        parameter_types: tuple[DuckDBType, ...],
        varargs: DuckDBType | None,
    ) -> None:
        self.schema_name = schema_name
        self.function_name = function_name
        self.function_type = function_type
        self.return_type = return_type
        self.parameter_types = parameter_types
        self.varargs = varargs

    def matches_arity(self, argument_count: int) -> bool:
        """Return whether this overload accepts the provided argument count."""

        required = len(self.parameter_types)
        if self.varargs is None:
            return argument_count == required
        return argument_count >= required


class DuckDBFunctionSignature:
    """Structured view of a DuckDB function's callable surface."""

    __slots__ = (
        "schema_name",
        "function_name",
        "function_type",
        "return_type",
        "parameter_types",
        "varargs",
        "sql_name",
    )

    def __init__(
        self,
        *,
        schema_name: str,
        function_name: str,
        function_type: str,
        return_type: DuckDBType | None,
        parameter_types: tuple[DuckDBType, ...],
        varargs: DuckDBType | None,
        sql_name: str,
    ) -> None:
        self.schema_name = schema_name
        self.function_name = function_name
        self.function_type = function_type
        self.return_type = return_type
        self.parameter_types = parameter_types
        self.varargs = varargs
        self.sql_name = sql_name

    def call_syntax(self) -> str:
        """Render the SQL call including argument placeholders."""

        arguments = [parameter.render() for parameter in self.parameter_types]
        if self.varargs is not None:
            arguments.append(f"{self.varargs.render()}...")
        joined_arguments = ", ".join(arguments)
        return f"{self.sql_name}({joined_arguments})"

    def return_annotation(self) -> str:
        """Return the DuckDB return annotation."""

        return (self.return_type or UnknownType()).render()

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
        arity = len(operands)
        for overload in self._overloads:
            if not overload.matches_arity(arity):
                continue
            try:
                arguments = _coerce_operands_for_overload(operands, overload)
            except TypeError:
                continue
            dependencies = _merge_dependencies(arguments)
            sql_name = _render_function_name(overload)
            rendered_args = ", ".join(argument.render() for argument in arguments)
            sql = f"{sql_name}({rendered_args})" if arguments else f"{sql_name}()"
            return _construct_expression(
                sql,
                return_type=overload.return_type,
                dependencies=dependencies,
                category=self._return_category,
            )
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


def _validate_function_operand_type(
    operand: TypedExpression,
    expected_type: DuckDBType | None,
) -> TypedExpression:
    if expected_type is None:
        return operand
    operand_type = operand.duck_type
    if isinstance(operand_type, DuckDBType) and not expected_type.accepts(operand_type):
        msg = (
            "DuckDB function arguments must match expected types: "
            f"expected {expected_type.render()}, got {operand_type.render()}"
        )
        raise TypeError(msg)
    return operand


def _coerce_function_operand(
    value: object,
    expected_type: DuckDBType | None = None,
) -> TypedExpression:
    if isinstance(value, TypedExpression):
        return _validate_function_operand_type(value, expected_type)
    if expected_type is not None:
        coerced = _coerce_by_duck_type(value, expected_type)
        if coerced is not None:
            return _validate_function_operand_type(coerced, expected_type)
    coerced_default = _coerce_function_operand_default(value)
    return _validate_function_operand_type(coerced_default, expected_type)


def _coerce_function_operand_default(value: object) -> TypedExpression:
    if isinstance(value, TypedExpression):
        return value
    if isinstance(value, bool):
        return BooleanExpression.literal(value)
    if isinstance(value, (int, float, Decimal)):
        return NumericExpression.literal(value)
    if isinstance(value, bytes):
        return BlobExpression.literal(value)
    if isinstance(value, tuple) and len(value) == 2:
        table, column = value
        if isinstance(table, str) and isinstance(column, str):
            dependency = ExpressionDependency.column(column, table=table)
            return GenericExpression(
                quote_qualified_identifier(column, table=table),
                duck_type=IdentifierType("IDENTIFIER"),
                dependencies=(dependency,),
            )
    if isinstance(value, str):
        dependency = ExpressionDependency.column(value)
        return GenericExpression(
            quote_identifier(value),
            duck_type=IdentifierType("IDENTIFIER"),
            dependencies=(dependency,),
        )
    if value is None:
        return GenericExpression("NULL", duck_type=UnknownType())
    msg = "DuckDB function arguments must be typed expressions or supported literals"
    raise TypeError(msg)


def _merge_dependencies(
    expressions: Iterable[TypedExpression],
) -> frozenset[ExpressionDependency]:
    dependencies: set[ExpressionDependency] = set()
    for expression in expressions:
        dependencies.update(expression.dependencies)
    return frozenset(dependencies)


def _coerce_operands_for_overload(
    operands: tuple[object, ...],
    overload: DuckDBFunctionDefinition,
) -> list[TypedExpression]:
    required = len(overload.parameter_types)
    varargs_type = overload.varargs
    if varargs_type is None and len(operands) != required:
        msg = "Incorrect argument count"
        raise TypeError(msg)
    if varargs_type is not None and len(operands) < required:
        msg = "Insufficient arguments for varargs overload"
        raise TypeError(msg)
    coerced: list[TypedExpression] = []
    for index, operand in enumerate(operands):
        expected_type: DuckDBType | None
        if index < required:
            expected_type = overload.parameter_types[index]
        else:
            expected_type = varargs_type
        coerced.append(_coerce_function_operand(operand, expected_type))
    return coerced


def _coerce_by_duck_type(
    value: object,
    expected_type: DuckDBType,
) -> TypedExpression | None:
    category = getattr(expected_type, "category", "generic")
    if category == "boolean" and isinstance(value, bool):
        return BooleanExpression.literal(value)
    if category == "numeric" and isinstance(value, (int, float, Decimal)):
        return NumericExpression.literal(value)
    if category == "varchar" and isinstance(value, str):
        return VarcharExpression.literal(value)
    if category == "blob" and isinstance(value, bytes):
        return BlobExpression.literal(value)
    if category == "identifier":
        if isinstance(value, tuple) and len(value) == 2:
            table, column = value
            if isinstance(table, str) and isinstance(column, str):
                dependency = ExpressionDependency.column(column, table=table)
                return GenericExpression(
                    quote_qualified_identifier(column, table=table),
                    duck_type=expected_type,
                    dependencies=(dependency,),
                )
        if isinstance(value, str):
            dependency = ExpressionDependency.column(value)
            return GenericExpression(
                quote_identifier(value),
                duck_type=expected_type,
                dependencies=(dependency,),
            )
    if value is None and category != "identifier":
        return GenericExpression("NULL", duck_type=UnknownType())
    return None


def _render_function_name(definition: DuckDBFunctionDefinition) -> str:
    schema = definition.schema_name
    name = definition.function_name
    if schema in ("main", "pg_catalog"):
        return quote_identifier(name) if name != name.lower() else name
    return f"{quote_identifier(schema)}.{quote_identifier(name)}"


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
    return_type: DuckDBType | None,
    dependencies: frozenset[ExpressionDependency],
    category: str,
) -> TypedExpression:
    duck_type = return_type or GenericType("UNKNOWN")
    if category == "numeric":
        return NumericExpression(sql, dependencies=dependencies, duck_type=duck_type)
    if category == "boolean":
        return BooleanExpression(sql, dependencies=dependencies, duck_type=duck_type)
    if category == "varchar":
        return VarcharExpression(sql, dependencies=dependencies, duck_type=duck_type)
    if category == "blob":
        return BlobExpression(sql, dependencies=dependencies, duck_type=duck_type)
    return GenericExpression(sql, dependencies=dependencies, duck_type=duck_type)


from ._generated_function_namespaces import (  # noqa: E402  pylint: disable=wrong-import-position
    AggregateFunctionNamespace,
    ScalarFunctionNamespace,
    WindowFunctionNamespace,
)


class DuckDBFunctionNamespace:  # pylint: disable=too-few-public-methods
    """Static access to DuckDB's scalar, aggregate, and window functions."""

    Scalar: ScalarFunctionNamespace
    Aggregate: AggregateFunctionNamespace
    Window: WindowFunctionNamespace

    def __init__(self) -> None:
        self.Scalar = ScalarFunctionNamespace()
        self.Aggregate = AggregateFunctionNamespace()
        self.Window = WindowFunctionNamespace()

    def __dir__(self) -> list[str]:
        return ["Aggregate", "Scalar", "Window"]


SCALAR_FUNCTIONS = ScalarFunctionNamespace()
AGGREGATE_FUNCTIONS = AggregateFunctionNamespace()
WINDOW_FUNCTIONS = WindowFunctionNamespace()


__all__ = [
    "DuckDBFunctionDefinition",
    "DuckDBFunctionNamespace",
    "DuckDBFunctionSignature",
    "SCALAR_FUNCTIONS",
    "AGGREGATE_FUNCTIONS",
    "WINDOW_FUNCTIONS",
]
