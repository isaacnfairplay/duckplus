"""Protocol definitions for string expressions."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .boolean import BooleanExprProto
from .numeric import IntegerExprProto


@runtime_checkable
class StringExprProto(Protocol):
    """Static interface for string expression helpers."""

    def strip(self, chars: str | None = None) -> "StringExprProto":
        ...

    def split_part(self, separator: str, field: int) -> "StringExprProto":
        ...

    def length(self) -> IntegerExprProto:
        ...

    def like(self, pattern: str) -> BooleanExprProto:
        ...


__all__ = ["StringExprProto"]
