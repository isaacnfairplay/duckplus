"""Aggregate expression helpers for :class:`duckplus.duckrel.DuckRel`."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from . import util
from .filters import ColumnReference, FilterExpression

__all__ = [
    "AggregateArgument",
    "AggregateExpression",
    "AggregateOrder",
]


class AggregateArgument:
    """Represent an argument used within an aggregate function call."""

    __slots__ = ("_kind", "_value")
    _kind: Literal["column", "literal", "raw"]
    _value: Any

    def __init__(self, kind: Literal["column", "literal", "raw"], value: Any) -> None:
        self._kind = kind
        self._value = value

    @classmethod
    def column(cls, reference: str | ColumnReference) -> "AggregateArgument":
        """Return an argument referencing a projected column."""

        if isinstance(reference, ColumnReference):
            reference = reference.name
        if not isinstance(reference, str):
            raise TypeError(
                "Aggregate column reference must be provided as a string; "
                f"received {type(reference).__name__}."
            )
        if not reference:
            raise ValueError("Aggregate column reference must not be empty.")
        return cls("column", reference)

    @classmethod
    def literal(cls, value: Any) -> "AggregateArgument":
        """Return an argument using a SQL literal representation of *value*."""

        coerced = util.coerce_scalar(value)
        return cls("literal", coerced)

    @classmethod
    def raw(cls, expression: str) -> "AggregateArgument":
        """Return an argument using the provided SQL *expression* verbatim."""

        if not isinstance(expression, str):
            raise TypeError(
                "Raw aggregate arguments must be strings; "
                f"received {type(expression).__name__}."
            )
        if not expression.strip():
            raise ValueError("Raw aggregate argument must not be empty.")
        return cls("raw", expression)

    def render(self, available_columns: Sequence[str]) -> str:
        """Return the SQL fragment for the argument validating column usage."""

        if self._kind == "column":
            resolved = util.resolve_columns([self._value], available_columns)[0]
            return util.quote_identifier(resolved)
        if self._kind == "literal":
            return util.format_sql_literal(self._value)
        return cast(str, self._value)

    def columns(self) -> tuple[str, ...]:
        """Return column names referenced by the argument."""

        if self._kind == "column":
            return (self._value,)
        return ()


@dataclass(frozen=True)
class AggregateOrder:
    """Ordering expression applied within aggregate function calls."""

    expression: AggregateArgument
    direction: Literal["ASC", "DESC"] = "ASC"

    def __post_init__(self) -> None:
        if not isinstance(self.expression, AggregateArgument):
            raise TypeError(
                "AggregateOrder.expression must be an AggregateArgument instance; "
                f"received {type(self.expression).__name__}."
            )

        normalized_direction = _normalize_direction(self.direction)
        object.__setattr__(self, "direction", normalized_direction)

    @classmethod
    def by_column(
        cls, column: str | ColumnReference, direction: Literal["asc", "desc"] = "asc"
    ) -> "AggregateOrder":
        """Return an order specification referencing a column."""

        normalized_direction = _normalize_direction(direction)
        return cls(AggregateArgument.column(column), normalized_direction)

    @classmethod
    def by_expression(
        cls, expression: str | AggregateArgument, direction: Literal["asc", "desc"] = "asc"
    ) -> "AggregateOrder":
        """Return an order specification using a raw SQL expression."""

        if isinstance(expression, AggregateArgument):
            argument = expression
        else:
            argument = AggregateArgument.raw(expression)
        normalized_direction = _normalize_direction(direction)
        return cls(argument, normalized_direction)

    def render(self, available_columns: Sequence[str]) -> str:
        """Return the rendered ``ORDER BY`` fragment."""

        rendered_expression = self.expression.render(available_columns)
        return f"{rendered_expression} {self.direction}"


class AggregateExpression:
    """Structured aggregate expression referencing relation columns."""

    __slots__ = ("_function", "_arguments", "_distinct", "_filter", "_order_by", "_star")

    def __init__(
        self,
        function: str,
        arguments: Sequence[AggregateArgument] | None = None,
        *,
        distinct: bool = False,
        filter: str | FilterExpression | None = None,
        order_by: Sequence[AggregateOrder] | None = None,
        _star: bool = False,
    ) -> None:
        if not isinstance(function, str) or not function.strip():
            raise ValueError("Aggregate function name must be a non-empty string.")

        normalized_function = function.strip().upper()

        normalized_arguments: tuple[AggregateArgument, ...]
        if arguments is None:
            normalized_arguments = ()
        else:
            normalized_arguments = tuple(arguments)
            for argument in normalized_arguments:
                if not isinstance(argument, AggregateArgument):
                    raise TypeError(
                        "AggregateExpression arguments must be AggregateArgument instances; "
                        f"received {type(argument).__name__}."
                    )

        normalized_orders: tuple[AggregateOrder, ...]
        if order_by is None:
            normalized_orders = ()
        else:
            normalized_orders = tuple(order_by)
            for order in normalized_orders:
                if not isinstance(order, AggregateOrder):
                    raise TypeError(
                        "order_by entries must be AggregateOrder instances; "
                        f"received {type(order).__name__}."
                    )

        if _star:
            if normalized_function != "COUNT":
                raise ValueError("Only COUNT aggregates may reference all rows via '*'.")
            if normalized_arguments:
                raise ValueError("COUNT(*) aggregates do not accept additional arguments.")
            if normalized_orders:
                raise ValueError("COUNT(*) aggregates do not support ORDER BY clauses.")
            if distinct:
                raise ValueError("COUNT(DISTINCT ...) requires a column reference.")

        if distinct and not normalized_arguments:
            raise ValueError("DISTINCT aggregates require at least one argument.")

        if normalized_orders and not normalized_arguments:
            raise ValueError("ORDER BY requires at least one aggregate argument to order.")

        if filter is not None and not isinstance(filter, (str, FilterExpression)):
            raise TypeError(
                "filter must be a SQL string or FilterExpression; "
                f"received {type(filter).__name__}."
            )

        if isinstance(filter, str) and not filter.strip():
            raise ValueError("filter expression must not be empty.")

        self._function = normalized_function
        self._arguments = normalized_arguments
        self._distinct = bool(distinct)
        self._filter = filter
        self._order_by = normalized_orders
        self._star = bool(_star)

    @classmethod
    def function(
        cls,
        name: str,
        *arguments: AggregateArgument | str | ColumnReference,
        distinct: bool = False,
        filter: str | FilterExpression | None = None,
        order_by: Sequence[
            AggregateOrder | str | ColumnReference | Sequence[object]
        ] | None = None,
    ) -> "AggregateExpression":
        """Return a generic aggregate expression for *name*."""

        normalized_arguments = tuple(_normalize_argument(argument) for argument in arguments)
        normalized_orders = _normalize_orders(order_by)
        return cls(
            name,
            normalized_arguments,
            distinct=distinct,
            filter=filter,
            order_by=normalized_orders,
        )

    @classmethod
    def count(
        cls,
        column: AggregateArgument | str | ColumnReference | None = None,
        *,
        distinct: bool = False,
        filter: str | FilterExpression | None = None,
    ) -> "AggregateExpression":
        """Return a ``COUNT`` aggregate expression."""

        if column is None:
            return cls("COUNT", (), distinct=distinct, filter=filter, _star=True)
        argument = _normalize_argument(column)
        return cls("COUNT", (argument,), distinct=distinct, filter=filter)

    @classmethod
    def sum(
        cls,
        column: AggregateArgument | str | ColumnReference,
        *,
        distinct: bool = False,
        filter: str | FilterExpression | None = None,
    ) -> "AggregateExpression":
        """Return a ``SUM`` aggregate expression."""

        argument = _normalize_argument(column)
        return cls("SUM", (argument,), distinct=distinct, filter=filter)

    @classmethod
    def avg(
        cls,
        column: AggregateArgument | str | ColumnReference,
        *,
        distinct: bool = False,
        filter: str | FilterExpression | None = None,
    ) -> "AggregateExpression":
        """Return an ``AVG`` aggregate expression."""

        argument = _normalize_argument(column)
        return cls("AVG", (argument,), distinct=distinct, filter=filter)

    @classmethod
    def min(
        cls,
        column: AggregateArgument | str | ColumnReference,
        *,
        filter: str | FilterExpression | None = None,
    ) -> "AggregateExpression":
        """Return a ``MIN`` aggregate expression."""

        argument = _normalize_argument(column)
        return cls("MIN", (argument,), filter=filter)

    @classmethod
    def max(
        cls,
        column: AggregateArgument | str | ColumnReference,
        *,
        filter: str | FilterExpression | None = None,
    ) -> "AggregateExpression":
        """Return a ``MAX`` aggregate expression."""

        argument = _normalize_argument(column)
        return cls("MAX", (argument,), filter=filter)

    @staticmethod
    def column(reference: str | ColumnReference) -> AggregateArgument:
        """Return an :class:`AggregateArgument` referencing *reference*."""

        return AggregateArgument.column(reference)

    @staticmethod
    def literal(value: Any) -> AggregateArgument:
        """Return an :class:`AggregateArgument` representing *value*."""

        return AggregateArgument.literal(value)

    @staticmethod
    def raw(expression: str) -> AggregateArgument:
        """Return an :class:`AggregateArgument` using a raw SQL fragment."""

        return AggregateArgument.raw(expression)

    def with_filter(self, filter: str | FilterExpression | None) -> "AggregateExpression":
        """Return a copy of the aggregate expression with a ``FILTER`` clause."""

        return type(self)(
            self._function,
            self._arguments,
            distinct=self._distinct,
            filter=filter,
            order_by=self._order_by,
            _star=self._star,
        )

    def with_order_by(self, *orders: AggregateOrder | str | ColumnReference | Sequence[object]) -> "AggregateExpression":
        """Return a copy of the aggregate expression with ``ORDER BY`` clauses."""

        normalized = _normalize_orders(orders)
        return type(self)(
            self._function,
            self._arguments,
            distinct=self._distinct,
            filter=self._filter,
            order_by=normalized,
            _star=self._star,
        )

    def distinct(self) -> "AggregateExpression":
        """Return a copy of the aggregate expression applying ``DISTINCT``."""

        return type(self)(
            self._function,
            self._arguments,
            distinct=True,
            filter=self._filter,
            order_by=self._order_by,
            _star=self._star,
        )

    def render(self, available_columns: Sequence[str]) -> str:
        """Return the aggregate SQL expression validating column references."""

        if self._star:
            invocation = f"{self._function}(*)"
        else:
            arguments = [argument.render(available_columns) for argument in self._arguments]
            if self._distinct:
                arguments[0] = f"DISTINCT {arguments[0]}"

            if self._order_by:
                order_sql = ", ".join(order.render(available_columns) for order in self._order_by)
                if not arguments:
                    raise ValueError("ORDER BY requires at least one aggregate argument to order.")
                arguments_sql = ", ".join(arguments)
                invocation = f"{self._function}({arguments_sql} ORDER BY {order_sql})"
            else:
                arguments_sql = ", ".join(arguments)
                invocation = f"{self._function}({arguments_sql})"

        if self._filter is None:
            return invocation

        if isinstance(self._filter, FilterExpression):
            filter_sql = self._filter.render(available_columns)
        else:
            filter_sql = self._filter
        return f"{invocation} FILTER (WHERE {filter_sql})"


def _normalize_argument(argument: AggregateArgument | str | ColumnReference) -> AggregateArgument:
    if isinstance(argument, AggregateArgument):
        return argument
    if isinstance(argument, ColumnReference):
        return AggregateArgument.column(argument)
    if isinstance(argument, str):
        return AggregateArgument.column(argument)
    raise TypeError(
        "AggregateExpression arguments must be column names or AggregateArgument instances; "
        f"received {type(argument).__name__}."
    )


def _normalize_orders(
    orders: Sequence[AggregateOrder | str | ColumnReference | Sequence[object]] | None,
) -> tuple[AggregateOrder, ...]:
    if orders is None:
        return ()

    normalized: list[AggregateOrder] = []
    for order_obj in orders:
        if isinstance(order_obj, AggregateOrder):
            normalized.append(order_obj)
            continue

        if isinstance(order_obj, (str, ColumnReference)):
            normalized.append(AggregateOrder.by_column(order_obj))
            continue

        if isinstance(order_obj, Sequence):
            sequence = tuple(order_obj)
            if not sequence:
                raise ValueError("ORDER BY tuples must contain an expression.")
            if len(sequence) > 2:
                raise ValueError(
                    "ORDER BY tuples accept up to two values: expression and optional direction."
                )
            expression_obj = sequence[0]
            if len(sequence) == 2:
                direction_obj = sequence[1]
            else:
                direction_obj = "asc"

            if isinstance(expression_obj, AggregateArgument):
                argument = expression_obj
            elif isinstance(expression_obj, ColumnReference):
                argument = AggregateArgument.column(expression_obj)
            elif isinstance(expression_obj, str):
                argument = AggregateArgument.column(expression_obj)
            else:
                raise TypeError(
                    "ORDER BY expressions must be column names or AggregateArgument instances; "
                    f"received {type(expression_obj).__name__}."
                )

            if not isinstance(direction_obj, str):
                raise TypeError(
                    "ORDER BY direction must be a string literal 'asc' or 'desc'; "
                    f"received {type(direction_obj).__name__}."
                )
            normalized_direction = _normalize_direction(direction_obj)
            normalized.append(AggregateOrder(argument, normalized_direction))
            continue

        raise TypeError(
            "ORDER BY entries must be AggregateOrder instances, column names, or tuples; "
            f"received {type(order_obj).__name__}."
        )

    return tuple(normalized)


def _normalize_direction(value: str) -> Literal["ASC", "DESC"]:
    if not isinstance(value, str):
        raise TypeError(
            "Ordering direction must be provided as a string literal 'asc' or 'desc'; "
            f"received {type(value).__name__}."
        )

    normalized = value.upper()
    if normalized not in {"ASC", "DESC"}:
        raise ValueError(
            "Ordering direction must be 'asc' or 'desc'; "
            f"received {value!r}."
        )
    return cast(Literal["ASC", "DESC"], normalized)
