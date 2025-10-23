"""Protocol definitions for numeric expressions."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class NumericExprProto(Protocol):
    """Static interface for numeric expression helpers."""

    def abs(self) -> "NumericExprProto":
        ...

    def round(self, digits: int | None = None) -> "NumericExprProto":
        ...


@runtime_checkable
class IntegerExprProto(NumericExprProto, Protocol):
    """Protocol for integer-returning expressions."""
