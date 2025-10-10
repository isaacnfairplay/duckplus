"""Duck+ public API."""

from __future__ import annotations

from .connect import DuckConnection, connect
from .core import DuckRel
from .materialize import (
    ArrowMaterializeStrategy,
    Materialized,
    ParquetMaterializeStrategy,
)

__all__ = [
    "ArrowMaterializeStrategy",
    "DuckConnection",
    "DuckRel",
    "Materialized",
    "ParquetMaterializeStrategy",
    "connect",
]
