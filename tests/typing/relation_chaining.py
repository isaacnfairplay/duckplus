"""Type-check snippets ensuring Relation expression chaining stays typed."""

from __future__ import annotations

from typing import Any, assert_type

from duckplus import FilterExpression, Relation
from duckplus.relation.core import Expression


def ensure_relation_chain(rel: Relation[tuple[Any, ...]]) -> None:
    projected = rel.select(
        lambda c: {
            "value": c["value"],
            "doubled": c["value"] * 2,
        }
    )
    assert_type(projected, Relation[tuple[Any, ...]])

    filtered = projected.where(lambda c: c["doubled"] > c.literal(0))
    assert_type(filtered, Relation[tuple[Any, ...]])

    ordered = filtered.order_by(lambda c: c["doubled"].desc())
    assert_type(ordered, Relation[tuple[Any, ...]])

    doubled_column = ordered.c["doubled"]
    assert_type(doubled_column, Expression[Any])

    predicate = doubled_column > ordered.c.literal(0)
    assert_type(predicate, FilterExpression)
