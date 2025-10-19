"""Runtime DuckDB function namespace primitives."""

# pylint: disable=too-many-return-statements,too-many-branches,
# pylint: disable=too-many-instance-attributes,protected-access,
# pylint: disable=too-few-public-methods,line-too-long

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    Mapping,
    Sequence,
    Tuple,
    cast,
)

from .dependencies import ExpressionDependency
from .expression import (
    BlobExpression,
    BooleanExpression,
    DateExpression,
    GenericExpression,
    NumericExpression,
    TimestampExpression,
    TimestampMillisecondsExpression,
    TimestampMicrosecondsExpression,
    TimestampNanosecondsExpression,
    TimestampSecondsExpression,
    TimestampWithTimezoneExpression,
    TypedExpression,
    VarcharExpression,
)
from .expressions.temporal import TemporalExpression
from .types import (
    BlobType,
    BooleanType,
    DecimalType,
    DuckDBType,
    FloatingType,
    GenericType,
    IntegerType,
    IntervalType,
    NumericType,
    TemporalType,
    UnknownType,
    VarcharType,
    infer_numeric_literal_type,
)

@dataclass(frozen=True)
class DuckDBFunctionSignature:
    """Typed representation of a DuckDB function overload."""

    schema_name: str
    function_name: str
    function_type: str
    return_type: DuckDBType | None
    parameter_types: Tuple[DuckDBType | None, ...]
    parameters: Tuple[str, ...]
    varargs: DuckDBType | None
    description: str | None
    comment: str | None
    macro_definition: str | None


@dataclass(frozen=True)
class DuckDBFunctionDefinition(DuckDBFunctionSignature):
    """Alias for backwards compatibility with the generator API."""


class _StaticFunctionNamespace:
    """Registry exposing DuckDB functions for a single return category."""

    function_type: ClassVar[str]
    return_category: ClassVar[str]
    _IDENTIFIER_FUNCTIONS: ClassVar[Mapping[str, str]]
    _SYMBOLIC_FUNCTIONS: ClassVar[Mapping[str, str]]

    def __getitem__(self, name: str) -> Callable[..., TypedExpression]:
        method = self.get(name)
        if method is None:  # pragma: no cover - defensive guard
            raise KeyError(name)
        return method

    def get(
        self,
        name: str,
        default: Callable[..., TypedExpression] | None = None,
    ) -> Callable[..., TypedExpression] | None:
        method_name = self._IDENTIFIER_FUNCTIONS.get(name)
        if method_name is None:
            method_name = self._SYMBOLIC_FUNCTIONS.get(name)
        if method_name is None:
            return default
        return cast(Callable[..., TypedExpression], getattr(self, method_name))

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):  # pragma: no cover - defensive guard
            return False
        return name in self._IDENTIFIER_FUNCTIONS or name in self._SYMBOLIC_FUNCTIONS

    @property
    def symbols(self) -> Mapping[str, Callable[..., TypedExpression]]:
        return {
            symbol: cast(Callable[..., TypedExpression], getattr(self, method_name))
            for symbol, method_name in self._SYMBOLIC_FUNCTIONS.items()
        }

    def __dir__(self) -> list[str]:
        members = set(self._IDENTIFIER_FUNCTIONS)
        members.update(self._SYMBOLIC_FUNCTIONS)
        return sorted(members)


_TEMPORAL_EXPRESSION_BY_NAME: Mapping[str, type[TemporalExpression]] = {
    "DATE": DateExpression,
    "TIMESTAMP": TimestampExpression,
    "TIMESTAMP_S": TimestampSecondsExpression,
    "TIMESTAMP_MS": TimestampMillisecondsExpression,
    "TIMESTAMP_US": TimestampMicrosecondsExpression,
    "TIMESTAMP_NS": TimestampNanosecondsExpression,
    "TIMESTAMP WITH TIME ZONE": TimestampWithTimezoneExpression,
}


def _resolve_temporal_expression(duck_type: DuckDBType | None) -> type[TypedExpression]:
    if isinstance(duck_type, TemporalType):
        expression_type = _TEMPORAL_EXPRESSION_BY_NAME.get(duck_type.render())
        if expression_type is not None:
            return expression_type
    return GenericExpression


def _expression_type_for(duck_type: DuckDBType | None) -> type[TypedExpression]:
    if duck_type is None or isinstance(duck_type, (GenericType, UnknownType)):
        return GenericExpression
    if isinstance(duck_type, BooleanType):
        return BooleanExpression
    if isinstance(duck_type, (NumericType, IntegerType, FloatingType, DecimalType, IntervalType)):
        return NumericExpression
    if isinstance(duck_type, VarcharType):
        return VarcharExpression
    if isinstance(duck_type, BlobType):
        return BlobExpression
    if isinstance(duck_type, TemporalType):
        resolved = _resolve_temporal_expression(duck_type)
        if resolved is not GenericExpression:
            return resolved
        return GenericExpression
    return GenericExpression


def _split_identifier(identifier: str) -> tuple[str | None, str]:
    trimmed = identifier.strip()
    if trimmed.startswith("\"") or trimmed.startswith("'"):
        return None, identifier
    if "." in trimmed:
        table, column = trimmed.split(".", 1)
        if table and column:
            return table, column
    return None, identifier


def _instantiate_expression(
    expression_type: type[TypedExpression],
    sql: str,
    *,
    duck_type: DuckDBType | None,
    dependencies: Iterable[ExpressionDependency],
) -> TypedExpression:
    deps = frozenset(dependencies)
    if expression_type is GenericExpression:
        return GenericExpression(sql, duck_type=duck_type or GenericType("UNKNOWN"), dependencies=deps)
    if expression_type is BooleanExpression:
        return cast(Any, expression_type)._raw(sql, dependencies=deps)
    if expression_type is VarcharExpression:
        return cast(Any, expression_type)._raw(sql, dependencies=deps, duck_type=duck_type)
    if expression_type is BlobExpression:
        return cast(Any, expression_type)._raw(sql, dependencies=deps, duck_type=duck_type)
    if expression_type is NumericExpression:
        return cast(Any, expression_type)._raw(sql, dependencies=deps, duck_type=duck_type)
    if issubclass(expression_type, TemporalExpression):
        return cast(Any, expression_type)._raw(sql, dependencies=deps, duck_type=duck_type)
    return cast(Any, expression_type)._raw(sql, dependencies=deps)


def _coerce_literal(
    expression_type: type[TypedExpression],
    value: object,
    duck_type: DuckDBType | None,
) -> TypedExpression:
    if expression_type is BooleanExpression and isinstance(value, bool):
        return BooleanExpression.literal(value)
    if expression_type is NumericExpression and isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return NumericExpression.literal(value)
    if expression_type is VarcharExpression and isinstance(value, str):
        return VarcharExpression.literal(value)
    if expression_type is BlobExpression and isinstance(value, bytes):
        return BlobExpression.literal(value)
    if issubclass(expression_type, TemporalExpression):
        if isinstance(value, (date, datetime)):
            return expression_type.literal(value)
        if isinstance(value, str):
            return expression_type.literal(value)
    if value is None:
        return GenericExpression("NULL", duck_type=GenericType("UNKNOWN"))
    if isinstance(value, bool):
        return BooleanExpression.literal(value)
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return NumericExpression.literal(value)
    if isinstance(value, str):
        return VarcharExpression.literal(value)
    if isinstance(value, bytes):
        return BlobExpression.literal(value)
    if isinstance(value, (date, datetime)):
        temporal_type = _resolve_temporal_expression(duck_type)
        if issubclass(temporal_type, TemporalExpression):
            return temporal_type.literal(value)
    if isinstance(value, time):
        return VarcharExpression.literal(value.isoformat())
    if isinstance(value, TypedExpression):  # pragma: no cover - defensive guard
        return value
    msg = f"Unsupported literal value {value!r} for DuckDB function argument"
    raise TypeError(msg)


def _coerce_operand(
    operand: object,
    expected_type: DuckDBType | None,
) -> TypedExpression:
    if isinstance(operand, TypedExpression):
        return operand
    if isinstance(operand, tuple) and len(operand) == 2 and all(isinstance(part, str) for part in operand):
        table, column = operand
        expression_type = _expression_type_for(expected_type)
        column_factory = getattr(expression_type, "column", None)
        if callable(column_factory):
            return column_factory(column, table=table)
    if isinstance(operand, str):
        if operand.strip() == "*":
            return GenericExpression._raw("*")
        table, column = _split_identifier(operand)
        expression_type = _expression_type_for(expected_type)
        column_factory = getattr(expression_type, "column", None)
        if callable(column_factory):
            return column_factory(column, table=table)
    expression_type = _expression_type_for(expected_type)
    return _coerce_literal(expression_type, operand, expected_type)


def _infer_operand_type(operand: object) -> DuckDBType | None:
    if isinstance(operand, TypedExpression):
        return operand.duck_type
    if isinstance(operand, bool):
        return BooleanType("BOOLEAN")
    if isinstance(operand, (int, float, Decimal)) and not isinstance(operand, bool):
        try:
            return infer_numeric_literal_type(operand)
        except TypeError:  # pragma: no cover - defensive guard
            return NumericType("NUMERIC")
    if isinstance(operand, datetime):
        return TemporalType("TIMESTAMP")
    if isinstance(operand, date):
        return TemporalType("DATE")
    if isinstance(operand, time):
        return TemporalType("TIME")
    return None


def _select_signature(
    signatures: Sequence[DuckDBFunctionSignature],
    operands: Sequence[object],
) -> DuckDBFunctionSignature:
    argument_count = len(operands)
    operand_types = [_infer_operand_type(operand) for operand in operands]
    best_signature: DuckDBFunctionSignature | None = None
    best_score: tuple[int, int] | None = None

    def _evaluate_signature(
        signature: DuckDBFunctionSignature,
    ) -> tuple[int, int] | None:
        required = len(signature.parameter_types)
        if signature.varargs is not None:
            if argument_count < required:
                return None
        elif argument_count != required:
            return None

        expected_types = list(signature.parameter_types)
        if argument_count > len(expected_types):
            if signature.varargs is None:
                return None
            expected_types.extend([signature.varargs] * (argument_count - len(expected_types)))

        typed_matches = 0
        typed_fallbacks = 0
        for expected, actual in zip(expected_types, operand_types, strict=False):
            if expected is None or actual is None:
                if actual is not None and expected is None:
                    typed_fallbacks += 1
                continue
            if expected.accepts(actual):
                typed_matches += 1
                continue
            return None

        return (typed_matches, -typed_fallbacks)

    for signature in signatures:
        score = _evaluate_signature(signature)
        if score is None:
            continue
        if best_score is None or score > best_score:
            best_signature = signature
            best_score = score

    if best_signature is not None:
        return best_signature

    function_name = signatures[0].function_name if signatures else "<unknown>"
    msg = (
        f"No DuckDB overload found for {function_name} with {argument_count} "
        "argument(s)"
    )
    raise TypeError(msg)


def _build_arguments(
    signature: DuckDBFunctionSignature,
    operands: Sequence[object],
) -> tuple[list[str], frozenset[ExpressionDependency]]:
    dependencies: set[ExpressionDependency] = set()
    expected_types = list(signature.parameter_types)
    if signature.varargs is not None and len(operands) > len(expected_types):
        extra = len(operands) - len(expected_types)
        expected_types.extend([signature.varargs] * extra)
    coerced = []
    for expected, operand in zip(expected_types, operands, strict=False):
        expression = _coerce_operand(operand, expected)
        coerced.append(expression)
        dependencies.update(expression.dependencies)
    return [expression.render() for expression in coerced], frozenset(dependencies)


def _render_call(function_name: str, arguments: list[str]) -> str:
    if not arguments:
        return f"{function_name}()"
    return f"{function_name}({', '.join(arguments)})"


def _render_symbolic(function_name: str, arguments: list[str]) -> str:
    if not arguments:
        return function_name
    if len(arguments) == 1:
        return f"({function_name} {arguments[0]})"
    joined = f" {function_name} ".join(arguments)
    return f"({joined})"


def _render_sql(signature: DuckDBFunctionSignature, arguments: list[str]) -> str:
    if signature.function_name.isidentifier():
        return _render_call(signature.function_name, arguments)
    return _render_symbolic(signature.function_name, arguments)


def _expression_type_for_signature(
    signature: DuckDBFunctionSignature,
    return_category: str,
) -> type[TypedExpression]:
    if signature.return_type is not None:
        resolved = _expression_type_for(signature.return_type)
        if resolved is not GenericExpression:
            return resolved
        temporal_candidate = _resolve_temporal_expression(signature.return_type)
        if temporal_candidate is not GenericExpression:
            return temporal_candidate
    category_map = {
        "numeric": NumericExpression,
        "boolean": BooleanExpression,
        "varchar": VarcharExpression,
        "blob": BlobExpression,
    }
    return category_map.get(return_category, GenericExpression)


def call_duckdb_function(
    signatures: Sequence[DuckDBFunctionDefinition],
    *,
    return_category: str,
    operands: Sequence[object],
) -> TypedExpression:
    if not signatures:
        msg = "Function call requires at least one signature"
        raise ValueError(msg)
    signature = _select_signature(
        cast(Sequence[DuckDBFunctionSignature], signatures),
        operands,
    )
    arguments, dependencies = _build_arguments(signature, operands)
    sql = _render_sql(signature, arguments)
    expression_type = _expression_type_for_signature(signature, return_category)
    return _instantiate_expression(
        expression_type,
        sql,
        duck_type=signature.return_type,
        dependencies=dependencies,
    )


def call_duckdb_filter_function(
    predicate: object,
    signatures: Sequence[DuckDBFunctionDefinition],
    *,
    return_category: str,
    operands: Sequence[object],
) -> TypedExpression:
    if not signatures:
        msg = "Function call requires at least one signature"
        raise ValueError(msg)
    condition = _coerce_operand(predicate, BooleanType("BOOLEAN"))
    signature = _select_signature(
        cast(Sequence[DuckDBFunctionSignature], signatures),
        operands,
    )
    arguments, dependencies = _build_arguments(signature, operands)
    sql = _render_sql(signature, arguments)
    clause = f"{sql} FILTER (WHERE {condition.render()})"
    expression_type = _expression_type_for_signature(signature, return_category)
    merged = frozenset((*dependencies, *condition.dependencies))
    return _instantiate_expression(
        expression_type,
        clause,
        duck_type=signature.return_type,
        dependencies=merged,
    )


__all__ = [
    "DuckDBFunctionDefinition",
    "DuckDBFunctionSignature",
    "_StaticFunctionNamespace",
    "call_duckdb_filter_function",
    "call_duckdb_function",
]
