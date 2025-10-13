"""Shared relational specification types for Duck+."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, cast

from .filters import FilterExpression


@dataclass(frozen=True)
class ExpressionPredicate:
    """Arbitrary SQL predicate fragment for joins."""

    expression: str

    def __post_init__(self) -> None:
        if not isinstance(self.expression, str) or not self.expression.strip():
            raise ValueError(
                "Join expression predicates must be provided as a non-empty string; "
                f"received {type(self.expression).__name__} with value {self.expression!r}."
            )


JoinPredicate = FilterExpression | ExpressionPredicate


@dataclass(frozen=True)
class JoinSpec:
    """Structured join specification with equality keys and optional predicates."""

    equal_keys: Sequence[tuple[str, str]]
    predicates: Sequence[JoinPredicate] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not isinstance(self.equal_keys, Sequence):
            raise TypeError(
                "JoinSpec.equal_keys must be a sequence of column pairs; "
                f"received {type(self.equal_keys).__name__}."
            )
        if not isinstance(self.predicates, Sequence):
            raise TypeError(
                "JoinSpec.predicates must be a sequence of predicates; "
                f"received {type(self.predicates).__name__}."
            )

        normalized_keys: list[tuple[str, str]] = []
        for pair_obj in cast(Sequence[object], self.equal_keys):
            if isinstance(pair_obj, (str, bytes)):
                raise TypeError(
                    "JoinSpec.equal_keys must contain pairs of column names; "
                    f"found single value {pair_obj!r}."
                )
            if not isinstance(pair_obj, Sequence):
                raise TypeError(
                    "JoinSpec.equal_keys must contain pairs of column names; "
                    f"found {type(pair_obj).__name__}."
                )
            pair = tuple(pair_obj)
            if len(pair) != 2:
                raise ValueError(
                    "JoinSpec.equal_keys must contain column name pairs; "
                    f"received {len(pair)} values in {pair!r}."
                )
            left, right = pair
            if not isinstance(left, str) or not isinstance(right, str):
                raise TypeError(
                    "JoinSpec.equal_keys must contain string column names; "
                    f"received {left!r} and {right!r}."
                )
            normalized_keys.append((left, right))

        normalized_predicates: list[JoinPredicate] = []
        for predicate_obj in cast(Sequence[object], self.predicates):
            if not isinstance(predicate_obj, (FilterExpression, ExpressionPredicate)):
                raise TypeError(
                    "JoinSpec.predicates must contain JoinPredicate instances; "
                    f"received {type(predicate_obj).__name__}."
                )
            normalized_predicates.append(predicate_obj)

        if not normalized_keys and not normalized_predicates:
            raise ValueError(
                "JoinSpec requires at least one equality key or predicate; both inputs were empty."
            )

        object.__setattr__(self, "equal_keys", tuple(normalized_keys))
        object.__setattr__(self, "predicates", tuple(normalized_predicates))


@dataclass(frozen=True)
class AsofOrder:
    """Pair of columns describing ASOF ordering."""

    left: str
    right: str


@dataclass(frozen=True)
class PartitionSpec(JoinSpec):
    """Equality-only specification describing partition columns for joins."""

    def __post_init__(self) -> None:
        if self.predicates:
            raise ValueError("PartitionSpec does not accept predicates; only equality keys are allowed.")
        super().__post_init__()

    @classmethod
    def of_columns(cls, *columns: str) -> "PartitionSpec":
        """Return a :class:`PartitionSpec` pairing identically named columns."""

        normalized: list[tuple[str, str]] = []
        for column in columns:
            if not isinstance(column, str):
                raise TypeError(
                    "PartitionSpec.of_columns() expects string column names; "
                    f"received {type(column).__name__}."
                )
            normalized.append((column, column))
        if not normalized:
            raise ValueError("PartitionSpec.of_columns() requires at least one column name.")
        return cls(equal_keys=tuple(normalized))

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, str]) -> "PartitionSpec":
        """Return a :class:`PartitionSpec` from a mapping of left-to-right column names."""

        if not isinstance(mapping, Mapping):
            raise TypeError(
                "PartitionSpec.from_mapping() expects a mapping of left-to-right column names; "
                f"received {type(mapping).__name__}."
            )
        pairs = [(left, right) for left, right in mapping.items()]
        if not pairs:
            raise ValueError("PartitionSpec.from_mapping() requires at least one mapping entry.")
        return cls(equal_keys=tuple(pairs))


class AsofSpec(JoinSpec):
    """Structured ASOF join specification."""

    __slots__ = ("order", "direction", "tolerance")

    order: AsofOrder
    direction: Literal["backward", "forward", "nearest"]
    tolerance: str | None

    def __init__(
        self,
        *,
        equal_keys: Sequence[tuple[str, str]],
        order: AsofOrder,
        predicates: Sequence[JoinPredicate] = (),
        direction: Literal["backward", "forward", "nearest"] = "backward",
        tolerance: str | None = None,
    ) -> None:
        super().__init__(equal_keys=equal_keys, predicates=predicates)
        object.__setattr__(self, "order", order)
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "tolerance", tolerance)
        if direction not in {"backward", "forward", "nearest"}:
            raise ValueError(
                "ASOF direction must be 'backward', 'forward', or 'nearest'; "
                f"received {direction!r}."
            )
        if direction == "nearest" and tolerance is None:
            raise ValueError(
                "ASOF joins with direction 'nearest' require a tolerance expression."
            )


@dataclass(frozen=True)
class JoinProjection:
    """Projection controls for join column collision handling."""

    allow_collisions: bool = False
    suffixes: tuple[str, str] | None = None

    def __post_init__(self) -> None:
        if self.suffixes is not None:
            if len(self.suffixes) != 2:
                raise ValueError(
                    "JoinProjection.suffixes must contain exactly two values; "
                    f"received {len(self.suffixes)} in {self.suffixes!r}."
                )


__all__ = [
    "AsofOrder",
    "AsofSpec",
    "ExpressionPredicate",
    "JoinPredicate",
    "JoinProjection",
    "JoinSpec",
    "PartitionSpec",
]

