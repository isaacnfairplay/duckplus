# Implementation Roadmap

## Preflight Discovery Questions
Answer these before starting any TODO item to confirm the work is understood and scoped.
1. What specific behaviour or feature does the TODO item request, and how does it fit into the existing API surface?
2. Which modules, classes, or utilities appear to own this responsibility today, and where should changes likely live?
3. What supporting documentation, tests, or historical implementations (including Git history) should be reviewed first?
4. What success criteria, edge cases, and failure behaviours must be exercised to consider the task complete?
5. How will this change be validated (tests, linters, examples), and are new fixtures or sample data required?

### Preflight Answers – Column addition helper dependencies
1. Update `Relation.add` to validate each expression against the original relation so columns introduced in the same call can no longer be referenced, including when typed expressions provide dependency metadata.
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

### Preflight Answers – Relation.aggregate helper
1. Implement `Relation.aggregate` to wrap DuckDB's relation aggregation while validating group-by columns and aggregation expressions against the immutable metadata cached on `Relation`.
2. The behaviour belongs in `duckplus/relation.py`, with new unit tests extending `tests/test_relation.py` and documentation updates landing in `docs/relation.md`.
3. Review DuckDB's `DuckDBPyRelation.aggregate` semantics alongside existing column helper implementations to mirror error handling, plus inspect typed expression factories for aggregation helpers.
4. Verify grouped results preserve column order, ensure typed and SQL aggregates reject unknown dependencies, cover filter handling, and exercise duplicate alias and connection state edge cases.
5. Validate with pytest plus the repository-standard mypy, uvx, and pylint runs to maintain quality gates.

### Preflight Answers – Relation.filter helper
1. Build a `Relation.filter` wrapper that composes boolean SQL snippets and typed expressions to return a filtered relation while keeping the original untouched.
2. The logic should live in `duckplus/relation.py`, likely reusing the filter normalisation introduced for `aggregate`, with coverage added to `tests/test_relation.py`.
3. Study the new `_normalise_filter_clauses` helper, DuckDB's relation `.filter` API, and the typed boolean expression utilities to align behaviour and dependency validation.
4. Ensure conditions validate column references, support chaining multiple filters, surface clear errors for non-boolean expressions, and respect connection state expectations.
5. Confirm correctness with dedicated pytest scenarios alongside the mandated mypy, uvx, and pylint suites before completion.

### Preflight Answers – As-of join helper
1. Add `Relation.asof_join` to wrap DuckDB's ASOF JOIN while composing equality pairs, ordering comparisons, and optional tolerances without mutating input relations.
2. Implement the helper in `duckplus/relation.py`, extend regression coverage in `tests/test_relation.py`, and document behaviour and alias requirements in `docs/relation.md`.
3. Review the existing join helpers, typed expression dependency validation, and DuckDB's ASOF JOIN semantics to mirror error handling and projection rules.
4. Ensure shared column ordering remains stable, ordering comparisons honour backward/forward modes, tolerance clauses filter rows predictably, and typed expressions require the documented `left`/`right` aliases with helpful diagnostics.
5. Validate via focused pytest scenarios plus the repository-standard mypy, uvx, and pylint runs prior to completion.

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
- [x] Implement `Relation.aggregate(group_by, **named_aggs, *filters)` with validation of group columns.
- [x] Implement `Relation.filter(*conditions)` compatible with typed expressions and raw SQL snippets.

## Advanced Joins
- [x] Implement "error on column conflict" joins (inner/left/right/outer/semi) that auto-join shared columns and allow explicit conditions.
- [x] Implement "asof" join leveraging the expression API for ordering and tolerance configuration.

## IO and Appenders
- [x] Provide IO helpers for CSV, Parquet, and other common formats, reusing `DuckCon` where possible.
- [x] Add appenders for CSV and NDJSON plus specialised insert tooling (consult long-term Git history for reference patterns).
- [x] Create a table interfacing API for managed inserts into DuckDB tables.

### Preflight Answers – IO reader keyword fidelity
1. Ensure each reader helper continues to expose the documented keyword arguments explicitly instead of forwarding `**kwargs`, keeping IDE completions reliable.
2. The behaviour lives in the IO helper modules (e.g. `duckplus/io/csv.py`, `duckplus/io/parquet.py`), with typing support defined in a shared alias module if helpful.
3. Review prior reader implementations and DuckDB's Python API signatures to mirror names, defaults, and validation semantics.
4. Validate that positional and keyword usage both function, that typos surface informative errors, and that auto-complete metadata remains intact.
5. Cover the behaviour with targeted unit tests per format and exercise mypy/pylint/pytest along with any stub updates to keep editors happy.
6. Remember that `DuckDBPyConnection.read_csv` prefers Python-specific names (`delimiter`, `quotechar`, `escapechar`, `decimal`, `skiprows`, etc.); table-function aliases like `delim` or `quote` must be normalised and should raise clear errors when conflicting values are provided.

### Preflight Answers – CSV/NDJSON appenders
1. Provide append helpers that stream CSV and NDJSON files into DuckDB tables while supporting creation, overwrite, and target column mapping semantics compatible with existing relation helpers.
2. The behaviour belongs alongside the file readers in `duckplus/io/__init__.py`, with regression tests living under `tests/` and documentation updates in `docs/io.md`.
3. Review DuckDB's relation `.create` and `.insert_into` helpers plus the existing CSV/JSON wrappers to mirror option normalisation and error handling patterns.
4. Ensure append helpers validate target column lists, clean up temporary views, and raise clear errors when tables or schemas are missing.
5. Validate via new pytest coverage and run the repository-standard mypy, uvx, and pylint checks to keep toolchain expectations satisfied.

### Preflight Answers – Table interfacing API
1. Managed table helpers should live alongside `DuckCon` and reuse the existing relation metadata, ensuring inserts only run when the connection is open and the relation originates from the same manager.
2. Shared utilities in `duckplus/_table_utils.py` handle identifier quoting, column normalisation, and transactional inserts so future appenders and table wrappers stay consistent.
3. Tests in `tests/test_table.py` cover create/overwrite behaviour, default-respecting target columns, raw DuckDB relations, and cross-connection validation scenarios for regression coverage.

### IO reader ergonomics
- [x] Mirror DuckDB's CSV reader signature explicitly (e.g. `filename`, `header`, `delim`, etc.) and share a typed alias for reuse across helpers without masking keyword visibility.
- [ ] Apply the explicit keyword signature pattern to Parquet, JSON, and other file readers to guarantee parity with DuckDB defaults.
  - DuckDB's connection helpers sometimes rename table-function arguments (`compression`, `binary_as_string`, etc.), so audit the accepted Python keywords before codifying aliases to avoid the mismatch we saw on CSV.
- [ ] Document each reader's callable signature within `docs/io.md`, emphasising IDE support and providing examples for keyword usage.
- [ ] Add regression tests that instantiate each reader via keyword arguments to guard against accidental signature regressions.

## Extension Integrations
- [ ] Package nano-ODBC community extension support with a `DuckCon.load_nano_odbc()` helper and usage docs.
- [ ] Surface the Excel community extension through a `Relation.from_excel` convenience that loads and documents available parameters.
- [ ] Audit DuckDB bundled extensions (e.g. HTTPFS, Spatial) and queue helpers for any not yet wrapped by the relation API.

### Preflight Answers – Extension enablement
1. Provide thin wrappers that manage extension installation/registration while keeping connection lifecycle rules intact.
2. Changes will live near `duckplus/extensions.py` (or equivalent) and integrate with `DuckCon` so sessions can opt-in cleanly.
3. Review DuckDB's extension loading docs plus community guidance for nano-ODBC and Excel to mirror configuration nuances.
4. Ensure helpers surface clear errors when extensions are unavailable, offer idempotent loading, and integrate with the IO roadmap entries.
5. Validate behaviour with targeted pytest cases using DuckDB's extension availability flags and document manual installation steps when needed.

## File-backed Table Operations
- [ ] Expose `Relation.insert_file`/`delete_file`/`append_file` helpers that treat Parquet/CSV/JSON datasets like managed tables.
- [ ] Reuse the existing table appender abstractions so file-backed operations share validation and transaction semantics.
- [ ] Document parity expectations versus DuckDB's `COPY`/`INSERT` commands, including transactional caveats for immutable formats.
- [ ] Add tests that round-trip data through each file format to confirm insert/delete/append workflows and schema drift handling.

## Practitioner Quality-of-Life Utilities
- [ ] Provide lightweight data profiling helpers (row counts, null ratios) to aid exploratory analysis directly from relations.
- [ ] Add schema diff utilities to compare relations or files and surface column-type drift warnings.
- [ ] Offer sample data exporters (to Pandas/Arrow) with batching options for notebook workflows.

## Future Web Service Enablement
- [ ] Design an opt-in web service layer that mounts relations as JSON/CSV HTTP endpoints while reusing validation helpers.
- [ ] Sketch authentication and request-shaping hooks so APIs can validate inputs before executing relation pipelines.
- [ ] Explore ASGI integration (FastAPI/Starlette) for minimal deployment while keeping dependencies optional and documented as future work.
