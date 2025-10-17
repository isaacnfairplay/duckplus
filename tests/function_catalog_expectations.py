"""Expected coverage gaps for DuckDB catalog integration tests.

Update the constants in this module when adding new typed wrappers or when
DuckDB's catalog metadata changes. The tests reference these structures to
provide actionable failure messages without breaking the suite during
incremental adoption.
"""

from __future__ import annotations

# Each entry captures (function_type, function_name, missing_metadata_field).
EXPECTED_DOCSTRING_GAPS: set[tuple[str, str, str]] = set()
