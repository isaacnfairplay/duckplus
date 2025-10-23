"""Concrete boolean expression classes."""

from __future__ import annotations

from typing import Any, Mapping

from spec import boolean_spec

from .base import TypedExpression


class BooleanExpr(TypedExpression):  # pylint: disable=too-few-public-methods
    """Boolean expression supporting logical helpers."""

    __slots__ = ()
    __duckplus_spec__: Mapping[str, Mapping[str, Any]] = boolean_spec.SPEC

    def _invoke(self, name: str, *args: Any, **kwargs: Any) -> "BooleanExpr":
        rendered_args = ", ".join(
            [*(repr(arg) for arg in args), *[f"{key}={value!r}" for key, value in kwargs.items()]]
        )
        expression = f"{self._expression}.{name}({rendered_args})"
        return self.__class__(expression)


class PredicateExpr(BooleanExpr):  # pylint: disable=too-few-public-methods
    """Named subtype for boolean predicates bound via factories."""
