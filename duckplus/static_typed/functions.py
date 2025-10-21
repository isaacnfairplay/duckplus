"""Runtime DuckDB function namespace primitives."""

# pylint: disable=too-many-return-statements,too-many-branches,
# pylint: disable=too-many-instance-attributes,protected-access,
# pylint: disable=too-few-public-methods,line-too-long

from __future__ import annotations

from collections.abc import Iterable as IterableABC
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Mapping,
    Sequence,
    Tuple,
    TypeVar,
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


_NamespaceExprT = TypeVar("_NamespaceExprT", bound=TypedExpression)


class _StaticFunctionNamespace(Generic[_NamespaceExprT]):
    """Registry exposing DuckDB functions for a single return category."""

    function_type: ClassVar[str]
    return_category: ClassVar[str]
    _IDENTIFIER_FUNCTIONS: ClassVar[dict[str, str]]
    _SYMBOLIC_FUNCTIONS: ClassVar[dict[str, str]]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        inherited_identifiers = dict(getattr(cls, "_IDENTIFIER_FUNCTIONS", ()))
        inherited_symbols = dict(getattr(cls, "_SYMBOLIC_FUNCTIONS", ()))
        cls._IDENTIFIER_FUNCTIONS = inherited_identifiers
        cls._SYMBOLIC_FUNCTIONS = inherited_symbols
        for attribute_name, attribute in cls.__dict__.items():
            identifiers = cast(
                tuple[str, ...] | None,
                getattr(attribute, "__duckdb_identifiers__", None),
            )
            symbols = cast(
                tuple[str, ...] | None,
                getattr(attribute, "__duckdb_symbols__", None),
            )
            if identifiers or symbols:
                cls._register_function(
                    attribute_name,
                    names=identifiers or (),
                    symbols=symbols or (),
                )

    @classmethod
    def _register_function(
        cls,
        attribute_name: str,
        *,
        names: tuple[str, ...],
        symbols: tuple[str, ...],
    ) -> None:
        if "_IDENTIFIER_FUNCTIONS" not in cls.__dict__:
            cls._IDENTIFIER_FUNCTIONS = {}
        if "_SYMBOLIC_FUNCTIONS" not in cls.__dict__:
            cls._SYMBOLIC_FUNCTIONS = {}
        for identifier in names:
            existing = cls._IDENTIFIER_FUNCTIONS.get(identifier)
            if existing is not None and existing != attribute_name:
                # Transitional compatibility: prefer the class-defined mapping when
                # legacy namespaces still declare `_IDENTIFIER_FUNCTIONS` manually.
                continue
            cls._IDENTIFIER_FUNCTIONS[identifier] = attribute_name
        for symbol in symbols:
            existing = cls._SYMBOLIC_FUNCTIONS.get(symbol)
            if existing is not None and existing != attribute_name:
                # Transitional compatibility: keep the original symbol mapping when
                # older namespaces pre-populated `_SYMBOLIC_FUNCTIONS`.
                continue
            cls._SYMBOLIC_FUNCTIONS[symbol] = attribute_name

    def __getitem__(self, name: str) -> Callable[..., _NamespaceExprT]:
        method = self.get(name)
        if method is None:  # pragma: no cover - defensive guard
            raise KeyError(name)
        return method

    def get(
        self,
        name: str,
        default: Callable[..., _NamespaceExprT] | None = None,
    ) -> Callable[..., _NamespaceExprT] | None:
        method_name = self._IDENTIFIER_FUNCTIONS.get(name)
        if method_name is None:
            method_name = self._SYMBOLIC_FUNCTIONS.get(name)
        if method_name is None:
            return default
        return cast(Callable[..., _NamespaceExprT], getattr(self, method_name))

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):  # pragma: no cover - defensive guard
            return False
        return name in self._IDENTIFIER_FUNCTIONS or name in self._SYMBOLIC_FUNCTIONS

    @property
    def symbols(self) -> Mapping[str, Callable[..., _NamespaceExprT]]:
        return {
            symbol: cast(Callable[..., _NamespaceExprT], getattr(self, method_name))
            for symbol, method_name in self._SYMBOLIC_FUNCTIONS.items()
        }

    def __dir__(self) -> list[str]:
        members = set(self._IDENTIFIER_FUNCTIONS)
        members.update(self._SYMBOLIC_FUNCTIONS)
        return sorted(members)


def duckdb_function(
    *names: str,
    symbols: Iterable[str] = (),
) -> Callable[[Callable[..., _NamespaceExprT]], Callable[..., _NamespaceExprT]]:
    """Decorator registering functions on typed namespaces at definition time."""

    alias_names = tuple(dict.fromkeys(names))
    normalized_symbols = tuple(dict.fromkeys(symbols))

    def decorator(
        func: Callable[..., _NamespaceExprT],
    ) -> Callable[..., _NamespaceExprT]:
        normalized_names = alias_names
        if not normalized_names and not normalized_symbols:
            normalized_names = (func.__name__,)
        setattr(func, "__duckdb_identifiers__", normalized_names)
        setattr(func, "__duckdb_symbols__", normalized_symbols)
        return func

    return decorator


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

    for signature in signatures:
        required = len(signature.parameter_types)
        if signature.varargs is not None:
            if argument_count < required:
                continue
        elif argument_count != required:
            continue

        expected_types = list(signature.parameter_types)
        if argument_count > len(expected_types):
            if signature.varargs is None:
                continue
            expected_types.extend([signature.varargs] * (argument_count - len(expected_types)))

        typed_matches = 0
        typed_fallbacks = 0
        compatible = True
        for expected, actual in zip(expected_types, operand_types, strict=False):
            if expected is None or actual is None:
                if actual is not None and expected is None:
                    typed_fallbacks += 1
                continue
            if expected.accepts(actual):
                typed_matches += 1
            else:
                compatible = False
                break

        if not compatible:
            continue

        score = (typed_matches, -typed_fallbacks)
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


def _render_call(
    function_name: str,
    arguments: list[str],
    *,
    order_clause: str | None = None,
) -> str:
    body = ", ".join(arguments)
    if order_clause:
        if body:
            body = f"{body} {order_clause}"
        else:
            body = order_clause
    if not body:
        return f"{function_name}()"
    return f"{function_name}({body})"


def _render_symbolic(function_name: str, arguments: list[str]) -> str:
    if not arguments:
        return function_name
    if len(arguments) == 1:
        return f"({function_name} {arguments[0]})"
    joined = f" {function_name} ".join(arguments)
    return f"({joined})"


def _render_sql(
    signature: DuckDBFunctionSignature,
    arguments: list[str],
    *,
    order_clause: str | None = None,
) -> str:
    if signature.function_name.isidentifier():
        return _render_call(
            signature.function_name,
            arguments,
            order_clause=order_clause,
        )
    if order_clause is not None:
        msg = "ORDER BY clause is not supported for symbolic DuckDB functions"
        raise ValueError(msg)
    return _render_symbolic(signature.function_name, arguments)


def _normalise_clause_operands(
    operands: Iterable[object] | object | None,
) -> list[object]:
    if operands is None:
        return []
    if isinstance(operands, (list, tuple, set)):
        return list(operands)
    if isinstance(operands, (TypedExpression, str)):
        return [operands]
    if isinstance(operands, IterableABC):
        return list(operands)
    return [operands]


def _normalise_sort_direction(direction: object) -> str:
    if not isinstance(direction, str):
        msg = "Order direction must be a string"
        raise TypeError(msg)
    normalised = direction.strip().upper()
    if normalised not in {"ASC", "DESC"}:
        msg = "Order direction must be 'ASC' or 'DESC'"
        raise ValueError(msg)
    return normalised


def _coerce_order_clause_operand(
    operand: object,
) -> tuple[str, frozenset[ExpressionDependency]]:
    if (
        isinstance(operand, Mapping)
        and "expression" in operand
        and len(operand) <= 2
    ):
        expression_operand = operand["expression"]
        direction = operand.get("direction")
        direction_sql = (
            _normalise_sort_direction(direction)
            if direction is not None
            else None
        )
        expression = _coerce_operand(expression_operand, None)
        sql = expression.render()
        if direction_sql is not None:
            sql = f"{sql} {direction_sql}"
        return sql, expression.dependencies
    if (
        isinstance(operand, tuple)
        and len(operand) == 2
        and isinstance(operand[1], str)
        and operand[1].strip().upper() in {"ASC", "DESC"}
    ):
        expression_operand, direction = operand
        direction_sql = _normalise_sort_direction(direction)
        expression = _coerce_operand(expression_operand, None)
        return f"{expression.render()} {direction_sql}", expression.dependencies
    expression = _coerce_operand(operand, None)
    return expression.render(), expression.dependencies


def _build_order_clause(
    operands: Iterable[object] | object | None,
) -> tuple[str | None, frozenset[ExpressionDependency]]:
    normalised = _normalise_clause_operands(operands)
    if not normalised:
        return None, frozenset()
    clause_parts: list[str] = []
    dependencies: set[ExpressionDependency] = set()
    for operand in normalised:
        sql, operand_dependencies = _coerce_order_clause_operand(operand)
        clause_parts.append(sql)
        dependencies.update(operand_dependencies)
    clause_sql = ", ".join(clause_parts)
    return f"ORDER BY {clause_sql}", frozenset(dependencies)


def _build_window_clause(
    partition_by: Iterable[object] | object | None,
    order_by: Iterable[object] | object | None,
    frame: str | None,
) -> tuple[str | None, frozenset[ExpressionDependency]]:
    partition_operands = TypedExpression._normalise_window_operands(partition_by)
    order_operands = TypedExpression._normalise_window_operands(order_by)
    dependencies: set[ExpressionDependency] = set()
    components: list[str] = []

    if partition_operands:
        partition_sql: list[str] = []
        for operand in partition_operands:
            sql, operand_dependencies = TypedExpression._coerce_window_operand(operand)
            partition_sql.append(sql)
            dependencies.update(operand_dependencies)
        components.append(f"PARTITION BY {', '.join(partition_sql)}")

    if order_operands:
        order_sql: list[str] = []
        for operand in order_operands:
            sql, operand_dependencies = TypedExpression._coerce_window_order_operand(operand)
            order_sql.append(sql)
            dependencies.update(operand_dependencies)
        components.append(f"ORDER BY {', '.join(order_sql)}")

    if frame is not None:
        frame_sql = frame.strip()
        if not frame_sql:
            msg = "Window frame clause cannot be empty"
            raise ValueError(msg)
        components.append(frame_sql)

    if not components:
        return None, frozenset()

    window_spec = " ".join(components)
    return f"({window_spec})", frozenset(dependencies)


def _compose_function_sql(
    signature: DuckDBFunctionSignature,
    arguments: list[str],
    *,
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> tuple[str, str | None, frozenset[ExpressionDependency]]:
    order_clause, order_dependencies = _build_order_clause(order_by)
    sql = _render_sql(signature, arguments, order_clause=order_clause)

    dependencies: set[ExpressionDependency] = set(order_dependencies)

    within_clause, within_dependencies = _build_order_clause(within_group)
    dependencies.update(within_dependencies)
    if within_clause is not None:
        sql = f"{sql} WITHIN GROUP ({within_clause})"

    window_clause, window_dependencies = _build_window_clause(
        partition_by,
        over_order_by,
        frame,
    )
    dependencies.update(window_dependencies)

    return sql, window_clause, frozenset(dependencies)


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
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
) -> TypedExpression:
    if not signatures:
        msg = "Function call requires at least one signature"
        raise ValueError(msg)
    signature = _select_signature(
        cast(Sequence[DuckDBFunctionSignature], signatures),
        operands,
    )
    arguments, dependencies = _build_arguments(signature, operands)
    sql, window_clause, clause_dependencies = _compose_function_sql(
        signature,
        arguments,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )
    dependencies = frozenset((*dependencies, *clause_dependencies))
    if window_clause is not None:
        sql = f"{sql} OVER {window_clause}"
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
    order_by: Iterable[object] | object | None = None,
    within_group: Iterable[object] | object | None = None,
    partition_by: Iterable[object] | object | None = None,
    over_order_by: Iterable[object] | object | None = None,
    frame: str | None = None,
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
    sql, window_clause, clause_dependencies = _compose_function_sql(
        signature,
        arguments,
        order_by=order_by,
        within_group=within_group,
        partition_by=partition_by,
        over_order_by=over_order_by,
        frame=frame,
    )
    dependencies = frozenset((*dependencies, *clause_dependencies))
    clause = f"{sql} FILTER (WHERE {condition.render()})"
    if window_clause is not None:
        clause = f"{clause} OVER {window_clause}"
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
    "duckdb_function",
    "call_duckdb_filter_function",
    "call_duckdb_function",
]
