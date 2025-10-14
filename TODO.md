# Implementation Roadmap

## Preflight Discovery Questions
Answer these before starting any TODO item to confirm the work is understood and scoped.
1. What specific behaviour or feature does the TODO item request, and how does it fit into the existing API surface?
2. Which modules, classes, or utilities appear to own this responsibility today, and where should changes likely live?
3. What supporting documentation, tests, or historical implementations (including Git history) should be reviewed first?
4. What success criteria, edge cases, and failure behaviours must be exercised to consider the task complete?
5. How will this change be validated (tests, linters, examples), and are new fixtures or sample data required?

### Notes for "Transformation helpers"
1. We need a `Relation.transform` helper that issues `SELECT * REPLACE` statements so callers can rewrite existing columns while preserving immutability.
2. The behaviour naturally belongs on `duckplus.relation.Relation`, building on the stored `DuckCon` and underlying `DuckDBPyRelation` metadata.
3. Review DuckDB's `SELECT * REPLACE` syntax and existing relation tests in `tests/test_relation.py` to align expectations.
4. The helper must validate requested columns exist, surface clear errors for bad references, support ergonomic casting, and leave other columns untouched.
5. We'll extend unit tests to cover replacement logic, casting shortcuts, error handling, and run mypy/pylint/pytest per project policy.

---

- [x] Add a DuckCon class with a context manager that will be easy to extend for io operations
- [x] Add a relation class that is immutable and has the DuckCon and a duckdbpy connection under the hood with some metadata stored like columns and Duckdb types (as varchar for now)

## Column Manipulation Utilities
- [ ] Transformation helpers
  - [x] Implement `Relation.transform(**replacements)` that issues a `SELECT * REPLACE` statement and validates referenced columns.
  - [x] Provide ergonomic overloads for simple casts, e.g. `relation.transform(column="column::INTEGER")`.
- [ ] Rename helpers
  - [x] Implement `Relation.rename(**renames)` backed by `SELECT * RENAME` and ensure conflicting names raise clear errors.
  - [x] Add `rename_if_exists` soft variant that skips missing columns with warnings/logging.
- [ ] Column addition helpers
  - [x] Implement `Relation.add(**expressions)` using `SELECT *, <expr> AS <alias>`.
  - [ ] Support dependent expressions (new columns referencing existing ones) with validation (blocked by typed expression API; see notes below).
- [ ] Column subset helpers
  - [ ] Implement `Relation.keep(*columns)` to project only requested columns, raising on unknown names by default.
  - [ ] Provide `keep_if_exists` variant that tolerates absent columns.
- [ ] Column drop helpers
  - [ ] Implement `Relation.drop(*columns)` using `SELECT * EXCLUDE` semantics with strict validation.
  - [ ] Provide `drop_if_exists` soft variant mirroring `keep_if_exists` behaviour.

## Typed Expression API
- [ ] Design fluent `ducktype` factory with concrete types (e.g. `Numeric`, `Varchar`, `Blob`).
- [ ] Surface aggregation helpers, e.g. `ducktype.Numeric.Aggregate.sum("sales") -> "sum(sales)"`.
- [ ] Enable expression comparisons (`ducktype.Varchar("customer") == "prime"`) and joins between differently named columns.
- [ ] Support aliasing and renaming via methods like `.alias("my_customer")` with dict/str serialization.
- [ ] Add window function construction helpers on typed expressions.

### Notes for "Typed Expression API"
1. Rich expression objects should expose column dependency metadata so helpers like `Relation.add` can validate references to both existing and newly-created columns.
2. Once expressions carry type information, revisit column helpers (`add`, `transform`, and future subset/drop utilities) to accept expression instances alongside raw SQL strings for safer composition.

## Aggregation and Filtering
- [ ] Implement `Relation.aggregate(group_by, **named_aggs, *filters)` with validation of group columns.
- [ ] Implement `Relation.filter(*conditions)` compatible with typed expressions and raw SQL snippets.

## Advanced Joins
- [ ] Implement "error on column conflict" joins (inner/left/right/outer/semi) that auto-join shared columns and allow explicit conditions.
- [ ] Implement "asof" join leveraging the expression API for ordering and tolerance configuration.

## IO and Appenders
- [ ] Provide IO helpers for CSV, Parquet, and other common formats, reusing `DuckCon` where possible.
- [ ] Add appenders for CSV and NDJSON plus specialised insert tooling (consult long-term Git history for reference patterns).
- [ ] Create a table interfacing API for managed inserts into DuckDB tables.
