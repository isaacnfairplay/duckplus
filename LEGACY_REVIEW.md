# LEGACY.md review

## Overview
- `LEGACY.md` preserves an earlier, monolithic helper module that wrapped `duckdb.DuckDBPyRelation` with ergonomic IO and transformation utilities under a `Duck` class.
- The file is roughly 3,000 lines and mixes connection helpers, identifier/typing utilities, filesystem discovery, IO adapters, and relational operations, explaining why the current project split these concerns across focused modules.

## Notable sections
- **ODBC helpers (lines 31-202)**: utilities for loading the `nanodbc` extension, assembling DSN / SQL Server / Excel connection strings, and prompting for passwords via the OS keyring when needed.
- **Identifier and typing utilities (lines 240-383)**: runtime validation for identifiers, case-insensitive column resolution, and typed cell fetching logic that underpins column renaming and selection.
- **Filesystem partition handling (lines 479-632)**: recursive globbing plus Hive-style partition filtering for CSV/Parquet/JSON readers.
- **`Duck` wrapper core (lines 668-2607)**: a single class exposing column accessors, selection/aggregation helpers, list/NumPy/Arrow exports, readers for Parquet/CSV/JSON/ODBC, table-targeted DML (insert/delete), join orchestration with partition-aware unions, set operations, window functions, HTML rendering, and materialization/caching helpers.
- **Debug instrumentation (lines 2868-2989)**: opt-in tracing that wraps most instance methods to record duration, caller location, and resulting column names.

## Key takeaways for modernization
- The legacy module intertwined IO, transformation, table mutation, and debugging in one class. Modern Duck+ separates these into smaller modules (`connect`, `core`, `io`, `util`) to reduce surface area and make dependencies explicit.
- Many routines assume interactive prompts (e.g., password input) or rely on deprecated behaviors such as global nanodbc loading. The current guidelines explicitly forbid these patterns in new code.
- While the `Duck` API showcased rich ergonomics, reproducing every method today would break current design principles around immutability (`DuckRel`) vs table mutation (`DuckTable`) and around avoiding interactive IO in core logic.

