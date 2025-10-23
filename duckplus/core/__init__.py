"""Core runtime package for DuckPlus."""

from .connection import Connection
from .relation import Relation
from .exceptions import CatalogException, AppendError

__all__ = ["Connection", "Relation", "CatalogException", "AppendError"]
