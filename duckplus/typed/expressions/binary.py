"""Binary (BLOB) expression primitives and factories."""

from __future__ import annotations

from typing import Iterable

from ..types import BlobType, DuckDBType
from .base import TypedExpression
from .utils import quote_identifier


class BlobExpression(TypedExpression):
    __slots__ = ()

    def __init__(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        duck_type: DuckDBType | None = None,
    ) -> None:
        super().__init__(
            sql,
            duck_type=duck_type or BlobType("BLOB"),
            dependencies=dependencies,
        )

    @classmethod
    def column(cls, name: str) -> "BlobExpression":
        return cls(quote_identifier(name), dependencies=(name,))

    @classmethod
    def literal(
        cls,
        value: bytes,
        *,
        duck_type: DuckDBType | None = None,
    ) -> "BlobExpression":
        hex_literal = value.hex()
        return cls(
            f"BLOB '\\x{hex_literal}'",
            duck_type=duck_type or BlobType("BLOB"),
        )

    @classmethod
    def raw(
        cls,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        duck_type: DuckDBType | None = None,
    ) -> "BlobExpression":
        return cls(sql, dependencies=dependencies, duck_type=duck_type)

    def _coerce_operand(self, other: object) -> "BlobExpression":
        if isinstance(other, BlobExpression):
            return other
        if isinstance(other, bytes):
            return BlobExpression.literal(other)
        msg = "Blob expressions only accept bytes operands"
        raise TypeError(msg)


class BlobFactory:
    def __call__(self, column: str) -> BlobExpression:
        return BlobExpression.column(column)

    def literal(self, value: bytes) -> BlobExpression:
        return BlobExpression.literal(value)

    def raw(
        self,
        sql: str,
        *,
        dependencies: Iterable[str] = (),
        duck_type: DuckDBType | None = None,
    ) -> BlobExpression:
        return BlobExpression.raw(
            sql,
            dependencies=dependencies,
            duck_type=duck_type,
        )

    def coerce(self, operand: object) -> BlobExpression:
        if isinstance(operand, BlobExpression):
            return operand
        if isinstance(operand, bytes):
            return self.literal(operand)
        msg = "Unsupported operand for blob expression"
        raise TypeError(msg)
