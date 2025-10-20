"""Decimal expression factories registered onto :class:`DuckTypeNamespace`."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from .numeric import NumericExpression, NumericFactory, NumericOperand
from ..types import DecimalType, DuckDBType

if TYPE_CHECKING:  # pragma: no cover - typing support only
    from ..expression import DuckTypeNamespace


def _create_decimal_expression(precision: int, scale: int) -> type[NumericExpression]:
    class DecimalExpression(NumericExpression):  # type: ignore[misc]
        __slots__ = ()

        @classmethod
        def default_type(cls) -> DuckDBType:  # type: ignore[override]
            return DecimalType(precision, scale)

        @classmethod
        def default_literal_type(
            cls, value: NumericOperand
        ) -> DuckDBType:  # type: ignore[override]
            return DecimalType(precision, scale)

    DecimalExpression.__name__ = f"Decimal{precision}_{scale}Expression"
    DecimalExpression.__qualname__ = DecimalExpression.__name__
    return DecimalExpression


_DECIMAL_FACTORY_ITEMS = tuple(
    (
        f"Decimal_{precision}_{scale}",
        NumericFactory(_create_decimal_expression(precision, scale)),
    )
    for precision in range(1, 39)
    for scale in range(0, precision + 1)
)

DECIMAL_FACTORY_NAMES: tuple[str, ...] = tuple(name for name, _ in _DECIMAL_FACTORY_ITEMS)

for _name, _factory in _DECIMAL_FACTORY_ITEMS:
    globals()[_name] = _factory

_DuckTypeNamespaceT = TypeVar("_DuckTypeNamespaceT", bound="DuckTypeNamespace")


def register_decimal_factories(
    namespace: type[_DuckTypeNamespaceT],
) -> type[_DuckTypeNamespaceT]:
    """Attach decimal factories to ``DuckTypeNamespace`` at class definition time."""

    for name, factory in _DECIMAL_FACTORY_ITEMS:
        setattr(namespace, name, factory)
    return namespace


__all__ = [
    "DECIMAL_FACTORY_NAMES",
    "register_decimal_factories",
    *DECIMAL_FACTORY_NAMES,
]
