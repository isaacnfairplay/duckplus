"""Decimal expression factories registered onto :class:`DuckTypeNamespace`."""

from __future__ import annotations

from decimal import Decimal
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

    seen: set[str] = set()
    for name, factory in _DECIMAL_FACTORY_ITEMS:
        if name in seen:
            msg = f"Duplicate decimal factory name detected: {name}"
            raise ValueError(msg)
        seen.add(name)

        if hasattr(namespace, name):
            msg = f"Decimal factory '{name}' already defined on {namespace.__name__}"
            raise ValueError(msg)

        _, precision_str, scale_str = name.split("_")
        precision = int(precision_str)
        scale = int(scale_str)

        expression_type = factory.expression_type
        default_type = expression_type.default_type()
        literal_type = expression_type.default_literal_type(Decimal("0"))

        for metadata in (default_type, literal_type):
            if not isinstance(metadata, DecimalType):
                msg = f"Decimal factory '{name}' must expose DecimalType metadata"
                raise ValueError(msg)
            if metadata.precision != precision or metadata.scale != scale:
                msg = (
                    f"Decimal factory '{name}' metadata mismatch: expected "
                    f"DECIMAL({precision}, {scale}), "
                    f"got DECIMAL({metadata.precision}, {metadata.scale})"
                )
                raise ValueError(msg)

        setattr(namespace, name, factory)
    return namespace


__all__ = [
    "DECIMAL_FACTORY_NAMES",
    "register_decimal_factories",
    *DECIMAL_FACTORY_NAMES,
]
