"""Concrete temporal expression classes."""

from __future__ import annotations

from typing import Any, Mapping

from spec import temporal_spec

from .base import TypedExpression
from .boolean import BooleanExpr
from .numeric import NumericExpr
from .string import StringExpr


class TemporalExpr(TypedExpression):  # pylint: disable=too-few-public-methods
    """Base class for timestamp-like expressions."""

    __slots__ = ()
    __duckplus_spec__: Mapping[str, Mapping[str, Any]] = temporal_spec.SPEC

    def _invoke(self, name: str, *args: Any, **kwargs: Any) -> TypedExpression:
        rendered_args = ", ".join(
            [*(repr(arg) for arg in args), *[f"{key}={value!r}" for key, value in kwargs.items()]]
        )
        expression = f"{self._expression}.{name}({rendered_args})"
        return_hint = self.__duckplus_spec__[name]["return"]
        if return_hint == "TemporalExpr":
            return self.__class__(expression)
        if return_hint == "NumericExpr":
            return NumericExpr(expression)
        if return_hint == "StringExpr":
            return StringExpr(expression)
        if return_hint == "BooleanExpr":
            return BooleanExpr(expression)
        raise ValueError(f"Unsupported return type: {return_hint}")


class TimestampExpr(TemporalExpr):  # pylint: disable=too-few-public-methods
    """Concrete timestamp expression bound via factories."""
    __slots__ = ()
