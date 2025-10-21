from __future__ import annotations

from .expression import DuckTypeNamespace, SelectStatementBuilder
from .expressions.binary import BlobFactory
from .expressions.boolean import BooleanFactory
from .expressions.decimal import *  # noqa: F401,F403
from .expressions.generic import GenericFactory
from .expressions.numeric import NumericFactory
from .expressions.temporal import TemporalFactory
from .expressions.text import VarcharFactory


ducktype: DuckTypeNamespace
Numeric: NumericFactory
Varchar: VarcharFactory
Boolean: BooleanFactory
Blob: BlobFactory
Generic: GenericFactory
Tinyint: NumericFactory
Smallint: NumericFactory
Integer: NumericFactory
Utinyint: NumericFactory
Usmallint: NumericFactory
Uinteger: NumericFactory
Float: NumericFactory
Double: NumericFactory
Date: TemporalFactory
Timestamp: TemporalFactory
Timestamp_s: TemporalFactory
Timestamp_ms: TemporalFactory
Timestamp_us: TemporalFactory
Timestamp_ns: TemporalFactory
Timestamp_tz: TemporalFactory


def select() -> SelectStatementBuilder: ...
