"""Core exceptions used by DuckPlus."""

from __future__ import annotations


class CatalogException(RuntimeError):
    """Raised when a catalog object (like an extension) is missing."""


class AppendError(RuntimeError):
    """Raised when an append operation violates policy or invariants."""
