"""Duck+ public API."""

from .connect import DuckConnection, connect
from .core import DuckRel

__all__ = ["DuckConnection", "DuckRel", "connect"]
