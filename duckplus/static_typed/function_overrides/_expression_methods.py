"""Utilities for binding typed namespace helpers onto expression instances."""

from __future__ import annotations

from functools import wraps
from typing import Callable, Tuple, TypeVar

from duckplus.static_typed.expression import TypedExpression

_ExprT = TypeVar("_ExprT", bound=TypedExpression)
_ResultExprT = TypeVar("_ResultExprT", bound=TypedExpression)


def attach_expression_method(
    expression_type: type[_ExprT],
    namespace: object,
    helper: Callable[..., _ResultExprT],
    *,
    insert_self_at: int = 0,
    operands_transform: Callable[[Tuple[object, ...]], Tuple[object, ...]] | None = None,
) -> None:
    """Expose ``helper`` as a bound method on ``expression_type`` instances.

    Parameters
    ----------
    expression_type:
        The expression class that should gain a method forwarding into the
        typed namespace helper.
    namespace:
        The typed namespace instance where ``helper`` is registered.
    helper:
        The decorated helper function attached to ``namespace``.
    insert_self_at:
        Position within the helper call where the expression instance should be
        inserted. Defaults to ``0`` which treats the expression as the first
        operand.
    """

    method_name = helper.__name__
    if hasattr(expression_type, method_name):
        return

    namespace_method = getattr(namespace, method_name)

    @wraps(helper)
    def method(self: _ExprT, *operands: object) -> _ResultExprT:
        adapted = operands
        if operands_transform is not None:
            adapted = operands_transform(tuple(operands))
        if insert_self_at <= 0:
            return namespace_method(self, *adapted)
        args = list(adapted)
        args.insert(insert_self_at, self)
        return namespace_method(*args)

    setattr(expression_type, method_name, method)
