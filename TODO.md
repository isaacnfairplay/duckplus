# Implementation Roadmap

## Preflight Discovery Questions
Answer these before starting any TODO item to confirm the work is understood and scoped.
1. What specific behaviour or feature does the TODO item request, and how does it fit into the existing API surface?
2. Which modules, classes, or utilities appear to own this responsibility today, and where should changes likely live?
3. What supporting documentation, tests, or historical implementations (including Git history) should be reviewed first?
4. What success criteria, edge cases, and failure behaviours must be exercised to consider the task complete?
5. How will this change be validated (tests, linters, examples), and are new fixtures or sample data required?

### Preflight Answers – Column addition helper dependencies
1. Update `Relation.add` to validate each expression against the original relation so columns introduced in the same call can no longer be referenced.
2. The `duckplus.relation.Relation` class owns column creation helpers today, so changes belong in `duckplus/relation.py`, with tests in `tests/test_relation.py`.
3. Review the existing `Relation.add` implementation along with its unit tests to mirror validation patterns and understand current error handling.
4. Confirm column order is preserved, that newly introduced aliases are rejected when referenced immediately, and that error messages remain informative.
5. Exercise the behaviour with new pytest coverage and run the mypy, pylint, and pytest suites per the repository policy.

### Preflight Notes – Window function helpers
1. Extend typed expressions with an ergonomic `over` helper so windowed aggregates integrate with the fluent API without falling back to raw SQL strings.
2. The implementation should live alongside existing typed expression logic, primarily in `duckplus/typed/expressions/base.py`, with alias-aware handling in related classes.
3. Review `duckplus/typed/expression.py`, the expression subclasses, and `tests/test_typed_expression.py` to mirror current construction patterns and dependency tracking.
4. The helper must render correct `OVER (...)` clauses, merge partition/order dependencies, support direction keywords, validate inputs, and keep chaining behaviour (including aliasing) intact.
5. Cover the behaviour with new unit tests, update the typed API docs, and run the mypy/pylint/pytest suite to satisfy the repository's pre-commit policy.

### Preflight Answers – Typed if_exists support
1. Extend the typed SELECT builder with `if_exists` options so column projections and star modifiers automatically skip clauses targeting missing columns, mirroring the relation helpers' soft behaviours.
2. The logic belongs in `duckplus/typed/select.py` with validation covered by `tests/test_typed_expression.py` and documentation in `docs/typed_api.md`.
3. Review the existing select-builder helpers and relation-level `*_if_exists` methods to align validation patterns, plus inspect current typed expression tests for coverage expectations.
4. Ensure optional components only render when their column dependencies exist, raise clear errors for ambiguous dependencies, and leave required projections untouched while updating docs to explain the workflow.
5. Validate via targeted pytest coverage for the new paths and run the mypy, uvx, and pylint suites alongside the full pytest run per repository policy.

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
- [x] Column addition helpers
  - [x] Implement `Relation.add(**expressions)` using `SELECT *, <expr> AS <alias>`.
- [ ] Column subset helpers
  - [x] Implement `Relation.keep(*columns)` to project only requested columns, raising on unknown names by default.
  - [x] Provide `keep_if_exists` variant that tolerates absent columns.
- [ ] Column drop helpers
  - [x] Implement `Relation.drop(*columns)` using `SELECT * EXCLUDE` semantics with strict validation.
  - [x] Provide `drop_if_exists` soft variant mirroring `keep_if_exists` behaviour.

## Typed Expression API
- [x] Design fluent `ducktype` factory with concrete types (e.g. `Numeric`, `Varchar`, `Blob`).
  - [x] Ensure concrete types remain composable so future composed/aggregated types (structs, lists) can wrap them without loss of metadata.
- [x] Surface aggregation helpers, e.g. `ducktype.Numeric.Aggregate.sum("sales") -> "sum(sales)"`.
- [x] Enable expression comparisons (`ducktype.Varchar("customer") == "prime"`) and joins between differently named columns.
- [x] Support aliasing and renaming via methods like `.alias("my_customer")` with dict/str serialization.
- [x] Add window function construction helpers on typed expressions.
- [x] Introduce a fluent CASE expression builder that composes with typed operands, including nested usage.
- [x] Provide a fluent SELECT statement builder for assembling projection lists.
- [x] Add if_exists options to allow better support of column management operations and make any operations that happen on an if_exists predicated on its existence

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
