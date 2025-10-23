"""Numeric expression specification for metaclass-driven method synthesis."""

from __future__ import annotations

from typing import Final

SPEC: Final[dict[str, dict[str, object]]] = {
    "abs": {
        "arguments": [],
        "return": "NumericExpr",
        "doc": "Return the absolute value of the numeric expression.",
    },
    "round": {
        "arguments": [
            {"name": "digits", "kind": "positional_or_keyword", "annotation": "int | None", "default": None}
        ],
        "return": "NumericExpr",
        "doc": "Round the expression to the requested number of digits.",
    },
}

__all__ = ["SPEC"]
