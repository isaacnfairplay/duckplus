"""Protocol describing boolean expression helpers."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class BooleanExprProto(Protocol):
    """Static surface for boolean expressions installed via ExprMeta."""

    def and_(self, other: "BooleanExprProto") -> "BooleanExprProto":
        """Return the logical conjunction of two boolean expressions."""

    def or_(self, other: "BooleanExprProto") -> "BooleanExprProto":
        """Return the logical disjunction of two boolean expressions."""

    def negate(self) -> "BooleanExprProto":
        """Return the logical negation of the boolean expression."""


__all__ = ["BooleanExprProto"]
