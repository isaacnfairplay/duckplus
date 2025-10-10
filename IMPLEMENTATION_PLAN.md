# Duck+ Implementation Plan

This living document captures the staged build-out plan for Duck+. Update it as work finishes or priorities shift so every agent knows the current status.

## How to use this plan
- Always read this file *before* starting new implementation work.
- Mark checkpoints when a stage is complete or a design decision changes.
- Keep the structure and ordering intact so we can track progress over time.

## Stage 0 — Foundations & Shared Utilities
Establish common helpers that every module depends on. Implement these before higher-level features to avoid circular work.

*Status*: ✅ Completed — shared utility helpers and connection extensions are in place. Reviewed 2024-05-11: current `util` and `connect` modules match the design intent and are protected by focused tests (`tests/test_util.py`, `tests/test_connect.py`), so no additional groundwork is required before moving forward.

### Module: `util`
*Goals*: provide identifier validation, column casing utilities, and small type helpers that keep `core` lean.

Planned functions (implement in this order):
1. `def ensure_identifier(name: str, *, allow_quoted: bool = False) -> str`
   - Validates/normalizes identifiers; needed across all modules.
2. `def normalize_columns(columns: Sequence[str]) -> tuple[list[str], dict[str, int]]`
   - Returns canonical column order and lookup map. Supports strict missing handling.
3. `def resolve_columns(requested: Sequence[str], available: Sequence[str], *, missing_ok: bool = False) -> list[str]`
   - Provides case-insensitive resolution for `DuckRel` and `DuckTable` APIs.
4. `def coerce_scalar(value: Any) -> Any`
   - Lightweight coercion for filters/defaults. Kept simple to avoid over-engineering.

*Why this order*: identifier rules underpin column operations; normalization builds on those rules; resolution depends on normalization; scalar coercion is isolated and can be implemented last once column semantics are clear.

### Module: `connect`
*Goals*: provide a typed, context-managed connection creator without leaking raw `duckdb` semantics.

Planned additions:
1. `class DuckConnection(AbstractContextManager[DuckConnection])`
   - Already exists, but will gain instrumentation hooks once `util` logging helpers land.
2. `def connect(database: Pathish | None = None, *, read_only: bool = False, config: Mapping[str, str] | None = None) -> DuckConnection`
   - Extend existing helper to accept config maps. Depends on `util.ensure_identifier` for config key validation.
3. `def load_extensions(conn: DuckConnection, extensions: Sequence[str]) -> None`
   - Explicit extension loading respecting non-interactive rule.

*Why this order*: adjust the class only after utilities exist (even if stubbed) so validation is shared; `connect` signature grows before extension helpers since they may use config values; extension loading is last because it requires a settled connection surface.

## Stage 1 — Core Relational Abstractions
Build the immutable transformation layer first; mutations depend on it.

*Status*: ✅ Completed — `DuckRel` covers projection, filtering, joins, ordering, limiting, and materialization. Unit tests in `tests/test_core.py` cover default join semantics, strict column handling, parameter validation, and materialization strategies, while exploratory pipelines in `tests/test_relation_integration.py::test_feature_engineering_flow` and `tests/test_relation_integration.py::test_backlog_snapshot_flow` verify multi-step usage patterns. Future work can build on this stable surface without revisiting Stage 1.

### Module: `core`
*Goals*: define `DuckRel` with chainable operations, strict column semantics, and explicit projection.

Implementation sequence:
1. `class DuckRel`
   - Constructor signature: `def __init__(self, relation: duckdb.DuckDBPyRelation, *, columns: Sequence[str] | None = None)`
   - Stores relation and column metadata using `util.normalize_columns`.
2. `def project_columns(self, *columns: str, missing_ok: bool = False) -> DuckRel`
   - Uses `util.resolve_columns` to enforce casing rules while keeping the name explicit.
3. `def project(self, expressions: Mapping[str, str]) -> DuckRel`
   - Allows computed columns with explicit aliases.
4. `def filter(self, expression: str, /, *args: Any) -> DuckRel`
   - Parameterized expressions; uses `coerce_scalar` for args.
5. `def inner_join/left_join/semi_join/anti_join(self, other: DuckRel, *, on: Sequence[str] | None = None) -> DuckRel`
   - Each join style remains explicit; defaults to shared columns when `on` is `None` and documents the key-based-only stance (rename before joining for complex cases).
6. `def order_by(self, **orders: Literal["asc", "desc"]) -> DuckRel`
7. `def limit(self, count: int) -> DuckRel`
8. `def materialize(self, *, strategy: MaterializeStrategy | None = None, into: duckdb.DuckDBPyConnection | None = None) -> Materialized`
   - Materialization boundary returning configurable artefacts (Arrow table by default, with pluggable Parquet/temp strategies) and optionally re-hosting data on another connection. Shared strategy implementations now live in a dedicated `materialize` module to keep `core` focused on relational transforms.

*Why this order*: start with construction to capture metadata; selection/projection shape columns before filters; joins rely on prior column validation; ordering/limit require the base operations; `materialize` finishes to hand results to callers.

## Stage 2 — Table Mutations
Introduce stateful operations only after `DuckRel` is stable.

*Status*: ✅ Completed — `DuckTable` now wraps mutation helpers with append,
antijoin, and continuous-ID insert strategies. Focused unit tests in
`tests/test_table.py` guard alignment and ID edge cases, and exploratory
scenarios in `tests/test_table_integration.py` validate dimension and stream
ingestion flows under the `mutable_with_approval` marker so they can evolve as
real-world pipelines expand.

### Module: `table`
*Goals*: wrap `DuckRel` targets with mutation helpers that respect insert semantics.

Planned functions/methods:
1. `class DuckTable`
   - Constructor: `def __init__(self, connection: DuckConnection, name: str)`
2. `def append(self, rel: DuckRel, *, by_name: bool = True) -> None`
3. `def insert_antijoin(self, rel: DuckRel, *, keys: Sequence[str]) -> int`
4. `def insert_by_continuous_id(self, rel: DuckRel, *, id_column: str, inclusive: bool = False) -> int`

*Why this order*: define the wrapper first; append is the basic building block; antijoin depends on append & join semantics; continuous-id insert builds on antijoin logic and requires previous utilities.

## Stage 3 — IO Boundaries
After relational semantics are stable, add IO helpers.

### Module: `io`
*Goals*: typed readers/writers mirroring DuckDB options while enforcing project conventions.

Sequence:
1. `def read_parquet(conn: DuckConnection, paths: Sequence[Path]) -> DuckRel`
2. `def read_csv(conn: DuckConnection, paths: Sequence[Path], *, encoding: str = "utf-8", header: bool = True) -> DuckRel`
3. `def read_json(conn: DuckConnection, paths: Sequence[Path]) -> DuckRel`
4. `def write_parquet(rel: DuckRel, path: Path, *, compression: str = "zstd") -> None`
5. `def write_csv(rel: DuckRel, path: Path, *, encoding: str = "utf-8", header: bool = True) -> None`
6. `def append_csv(table: DuckTable, path: Path, *, encoding: str = "utf-8", header: bool = True) -> None`
7. `def append_ndjson(table: DuckTable, path: Path) -> None`

*Why this order*: readers arrive before writers to enable round-trip tests; CSV/JSON share patterns so we batch them; writing builds on established column metadata; append helpers depend on `DuckTable` semantics finalized earlier.

## Stage 4 — Extras
Only start extras once core and IO pieces are reliable.

### Module: `cli`
- `def main(argv: Sequence[str] | None = None) -> int`
- `def repl(conn: DuckConnection) -> None`

### Module: `html`
- `def to_html(rel: DuckRel, *, max_rows: int = 100) -> str`

*Why last*: extras must not block core delivery and can evolve independently once stable APIs exist.

## Testing Strategy
- Each stage introduces targeted tests in `tests/` mirroring the module name.
- Always cover strict column handling, join defaults, and IO defaults as described in `AGENTS.md`.
- Maintain exploratory integration suites (`tests/test_table_integration.py`,
  `tests/test_relation_integration.py`) under the `mutable_with_approval`
  marker to document realistic pipelines without locking the data scenarios in
  place.

---
*Last updated*: materialize now supports strategies and cross-connection transfers.
