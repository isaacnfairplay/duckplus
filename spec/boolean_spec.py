"""Boolean expression specification for metaclass-driven synthesis."""

from __future__ import annotations

from typing import Final

SPEC: Final[dict[str, dict[str, object]]] = {
    "and_": {
        "arguments": [
            {"name": "other", "kind": "positional_or_keyword", "annotation": "BooleanExprProto"}
        ],
        "return": "BooleanExpr",
        "doc": "Logical AND against another boolean expression.",
    },
    "or_": {
        "arguments": [
            {"name": "other", "kind": "positional_or_keyword", "annotation": "BooleanExprProto"}
        ],
        "return": "BooleanExpr",
        "doc": "Logical OR against another boolean expression.",
    },
    "negate": {
        "arguments": [],
        "return": "BooleanExpr",
        "doc": "Return the logical negation of the boolean expression.",
    },
}

__all__ = ["SPEC"]
