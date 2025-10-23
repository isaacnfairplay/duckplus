"""DuckPlus 2.0 runtime.

This package exposes the high level APIs for building typed DuckDB
pipelines with fast imports and strong static typing.
"""

from .core.connection import Connection
from .core.relation import Relation
from .table.table import Table

__all__ = ["Connection", "Relation", "Table"]
