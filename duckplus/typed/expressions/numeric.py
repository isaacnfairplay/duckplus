"""Numeric expression primitives and factories."""

from __future__ import annotations

from decimal import Decimal
from typing import Callable, Iterable, TYPE_CHECKING

from ..dependencies import DependencyLike, ExpressionDependency
from ..types import DuckDBType, NumericType, infer_numeric_literal_type
from .base import TypedExpression
from .boolean import BooleanFactory
from .case import CaseExpressionBuilder
from .utils import format_numeric, quote_qualified_identifier

if TYPE_CHECKING:  # pragma: no cover - type checking helper
    from ..functions import DuckDBFunctionNamespace

NumericOperand = int | float | Decimal


class NumericExpression(TypedExpression):
    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> None:
        super().__init__(
            sql,
            duck_type=duck_type or NumericType("NUMERIC"),
            dependencies=dependencies,
        )

    @classmethod
    def column(
        cls,
        name: str,
        *,
        table: str | None = None,
    ) -> "NumericExpression":
        dependency = ExpressionDependency.column(name, table=table)
        sql = quote_qualified_identifier(name, table=table)
        return cls(sql, dependencies=(dependency,))

    @classmethod
    def literal(
        cls,
        value: NumericOperand,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "NumericExpression":
        inferred_type = duck_type or infer_numeric_literal_type(value)
        return cls(
            format_numeric(value),
            duck_type=inferred_type,
        )

    @classmethod
    def raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> "NumericExpression":
        return cls(sql, dependencies=dependencies, duck_type=duck_type)

    def _coerce_operand(self, other: object) -> "NumericExpression":
        if isinstance(other, NumericExpression):
            return other
        if isinstance(other, (int, float, Decimal)):
            return NumericExpression.literal(other)
        msg = "Numeric expressions only accept numeric operands"
        raise TypeError(msg)

    def _binary(self, operator: str, other: object) -> "NumericExpression":
        operand = self._coerce_operand(other)
        sql = f"({self.render()} {operator} {operand.render()})"
        dependencies = self.dependencies.union(operand.dependencies)
        return NumericExpression(sql, dependencies=dependencies)

    def __add__(self, other: object) -> "NumericExpression":
        return self._binary("+", other)

    def __sub__(self, other: object) -> "NumericExpression":
        return self._binary("-", other)

    def __mul__(self, other: object) -> "NumericExpression":
        return self._binary("*", other)

    def __truediv__(self, other: object) -> "NumericExpression":
        return self._binary("/", other)

    def __mod__(self, other: object) -> "NumericExpression":
        return self._binary("%", other)

    def __pow__(self, other: object) -> "NumericExpression":
        return self._binary("^", other)

    # Aggregation -----------------------------------------------------
    def sum(self) -> "NumericExpression":
        from ..functions import (  # pylint: disable=import-outside-toplevel
            AGGREGATE_FUNCTIONS,
        )

        return AGGREGATE_FUNCTIONS.Numeric.sum(self)

    def avg(self) -> "NumericExpression":
        from ..functions import (  # pylint: disable=import-outside-toplevel
            AGGREGATE_FUNCTIONS,
        )

        return AGGREGATE_FUNCTIONS.Numeric.avg(self)


class NumericFactory:
    """Factory for creating numeric expressions."""

    def __init__(self, function_namespace: "DuckDBFunctionNamespace | None" = None) -> None:
        self._function_namespace = function_namespace

    def __call__(
        self,
        column: str,
        *,
        table: str | None = None,
    ) -> NumericExpression:
        return NumericExpression.column(column, table=table)

    def literal(self, value: NumericOperand) -> NumericExpression:
        return NumericExpression.literal(value)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[DependencyLike] = (),
        duck_type: DuckDBType | None = None,
    ) -> NumericExpression:
        return NumericExpression.raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )

    def coerce(self, operand: object) -> NumericExpression:
        if isinstance(operand, NumericExpression):
            return operand
        if isinstance(operand, str):
            return self(operand)
        if isinstance(operand, tuple) and len(operand) == 2:
            table, column = operand
            if isinstance(table, str) and isinstance(column, str):
                return NumericExpression.column(column, table=table)
        if isinstance(operand, (int, float, Decimal)):
            return self.literal(operand)
        msg = "Unsupported operand for numeric expression"
        raise TypeError(msg)

    def case(self) -> CaseExpressionBuilder[NumericExpression]:
        boolean_factory = BooleanFactory()
        return CaseExpressionBuilder(
            result_coercer=self.coerce,
            condition_coercer=boolean_factory.coerce,
        )

    @property
    def Aggregate(self) -> "NumericAggregateFactory":  # pylint: disable=invalid-name
        return NumericAggregateFactory(
            self,
            function_namespace=self._function_namespace,
        )


class NumericAggregateFactory:
    def __init__(
        self,
        factory: NumericFactory,
        *,
        function_namespace: "DuckDBFunctionNamespace | None" = None,
    ) -> None:
        self._factory = factory
        self._function_namespace = function_namespace

    def _from_function_namespace(
        self, name: str
    ) -> Callable[..., NumericExpression] | None:
        if self._function_namespace is None:
            return None
        try:
            accessor = self._function_namespace.Aggregate.Numeric
        except RuntimeError:
            return None
        function = getattr(accessor, name, None)
        if not callable(function):
            return None
        return function

    def __getattr__(self, name: str):
        function = self._from_function_namespace(name)
        if function is None:
            raise AttributeError(f"Aggregate function '{name}' is not available") from None

        def wrapper(*operands: object) -> NumericExpression:
            return function(*operands)

        return wrapper

    def sum(self, operand: object) -> NumericExpression:
        function = self._from_function_namespace("sum")
        if function is not None:
            return function(operand)
        expression = self._factory.coerce(operand)
        sql = f"sum({expression.render()})"
        return NumericExpression(sql, dependencies=expression.dependencies)
