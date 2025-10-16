"""Top-level package for duckplus utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = ["DuckCon", "Relation", "Table", "io", "schema"]

try:  # pragma: no branch - small module guard
    from .duckcon import DuckCon
    from .relation import Relation
    from .table import Table
    from . import io, schema
except ModuleNotFoundError as exc:  # pragma: no cover - depends on duckdb
    if TYPE_CHECKING:  # pragma: no cover - import-time hinting only
        from .duckcon import DuckCon  # type: ignore # noqa: F401
        from .relation import Relation  # type: ignore # noqa: F401
        from .table import Table  # type: ignore # noqa: F401
        from . import io, schema  # type: ignore # noqa: F401
    else:
        _IMPORT_ERROR = exc

        def __getattr__(name: str):
            if name in {"DuckCon", "Relation", "Table", "io", "schema"}:
                message = (
                    "DuckDB is required to use duckplus.DuckCon, duckplus.Relation, "
                    "duckplus.Table, duckplus.io, or duckplus.schema helpers. Install it with "
                    "'pip install duckdb' to unlock database features."
                )
                raise ModuleNotFoundError(message) from _IMPORT_ERROR
            raise AttributeError(name) from None

        DuckCon = Relation = Table = io = schema = None  # type: ignore[assignment]  # pylint: disable=invalid-name
