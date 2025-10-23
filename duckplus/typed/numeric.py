"""Concrete numeric expression classes."""

from __future__ import annotations

from typing import Any, Mapping

from spec import numeric_spec

from .base import TypedExpression


class NumericExpr(TypedExpression):  # pylint: disable=too-few-public-methods
    """Base class for numeric expressions."""

    __slots__ = ()
    __duckplus_spec__: Mapping[str, Mapping[str, Any]] = numeric_spec.SPEC

    def _invoke(self, name: str, *args: Any, **kwargs: Any) -> "NumericExpr":
        rendered_args = ", ".join(
            [*(repr(arg) for arg in args), *[f"{key}={value!r}" for key, value in kwargs.items()]]
        )
        expression = f"{self._expression}.{name}({rendered_args})"
        return self.__class__(expression)


class IntegerExpr(NumericExpr):  # pylint: disable=too-few-public-methods
    """Integer subtype for numeric expressions."""
    __slots__ = ()
