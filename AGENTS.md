# AGENTS.md

Guidelines for automated agents (code assistants, bots, LLMs) that contribute to **Duck+ (`duckplus`)**.

---

## Purpose

This document tells agents **what to change, how to change it, and what to never change**. It encodes design intent so automated contributions stay aligned with the project.

---

## Ground Rules (read first)

* **Python**: 3.12+ (develop/test on 3.12 & 3.13; keep compatibility with 3.14-dev).
* **Engine**: DuckDB `>=1.3.0`.
* **Package**: PyPI name `duckplus`, import `duckplus`.
* **License**: MIT.
* **Tooling**: Use **uv/uvx** for everything (no `pip`, no manual venvs).
* **Core is non-interactive**: no prompts, no blocking input, no network auth UIs.
* **No ODBC extras in project config**: DuckDB extensions are loaded at runtime when used.

---

## Project Principles (must preserve)

* **Two core types**

  * `DuckRel`: immutable, transformation-oriented wrapper around a relation.
  * `DuckTable`: mutating operations on a concrete table (insert/append, etc.).
* **Connections**

  * `duckplus.connect()` context manager; default: in-memory.
  * Optionally `LOAD secrets;` if available; do **not** add generic public `.sql` entry points.
* **Columns & casing**

  * Preserve original column **case**.
  * Provide `columns_lower` and `columns_lower_set` conveniences.
  * **Case-insensitive resolution**; **strict by default** on missing columns (raise unless explicitly opted out).
* **Joins (default behavior)**

  * Join on **shared columns** by default.
  * Keep **all left** columns.
  * **Drop right-side key** columns (the left already has the keys).
  * **Error** on non-key name collisions by default; allowed suppression adds suffixes mirroring DuckDB `_1` / `_2`.
  * Never rely on DuckDB’s default projection to “figure it out”; always project explicitly.
* **Inserts**

  * `append(rel, by_name=True)` aligns by column **name**.
  * `insert_antijoin(rel, keys=...)` excludes already-present rows by keys.
  * `insert_by_continuous_id(rel, id_col, inclusive=False)` assumes contiguous increasing IDs and appends rows with ID greater (or ≥) than current max.
* **I/O**

  * Readers: `read_parquet`, `read_csv`, `read_json` accept `Path` or `Sequence[Path]` (may accept `os.DirEntry`); **do not encourage raw strings** in docs/examples.
  * Writers: defaults — Parquet `compression='zstd'`, temp-then-rename; CSV `encoding='utf-8'`, header on.
  * Appenders: `append_csv`, `append_ndjson`.
* **Extras live outside core**:

  * `cli` (SQL playground & call-selection REPL, read-only transformations).
  * `html` (simple `to_html()` table view).
  * Core must remain non-interactive and offline.

---

## Allowed Tasks for Agents

* Create/modify files under:

  * `src/duckplus/` (`connect.py`, `core.py`, `io.py`, `util.py`, `__init__.py`, `py.typed`)
  * `tests/` (unit tests mirroring features)
  * Docs (`README.md`, `CONTRIBUTING.md`, `AGENTS.md`)
  * CI (`.github/workflows/ci.yml`) or uv config in `pyproject.toml`
* Add **well-scoped** features that uphold principles above.
* Improve typing (PEP 561, `TypedDict`, `Protocol`) without weakening `mypy --strict`.
* Add tests for every new public method or changed default.

---

## Disallowed Tasks (do not propose or commit)

* Adding interactive prompts, background threads, or network calls inside core modules.
* Introducing generic public `Connection.sql(...)` or similar escape hatches.
* Changing default join/insert/strict-missing semantics.
* Adding ODBC/Keyring deps or config blocks to `pyproject.toml`.
* Encouraging string paths in public docs or examples.
* Silent performance regressions (e.g., unnecessary materialization) or behavioral changes.

---

## Dev Commands (must be used)

```bash
# Sync environment (dev + extras if required by PR)
uv sync

# Run tests
uv run pytest

# Type check (Python)
uv run mypy src/duckplus

# Type check (Rust 'ty' from uv team)
uvx ty src/duckplus
```

CI should run the same steps.

---

## Code & Typing Standards

* Keep functions small; names explicit.
* `mypy` **strict** across `src/duckplus`.
* Public API needs docstrings and tests.
* Avoid default DuckDB behaviors that hide projection/ordering semantics; generate explicit SQL.
* Prefer single-projection transforms; avoid temp columns when possible.

---

## Testing Requirements

* Use in-memory DuckDB.
* Add tests for:

  * Strict-missing behavior (`missing_ok=False` default).
  * Join projection rules and collision handling (error by default; suffix when allowed).
  * Insert strategies (`append(by_name=True)`, `insert_antijoin`, `insert_by_continuous_id`).
  * Writers and appenders (verify rows/headers, use temp dirs).
* No real network, credentials, or external services in tests.

---

## PR Checklist (agents must satisfy)

* [ ] Feature/change is **small**, focused, and documented.
* [ ] New/updated tests cover behavior and edge cases.
* [ ] `uv run pytest` passes.
* [ ] `uv run mypy src/duckplus` passes (strict).
* [ ] `uvx ty src/duckplus` passes.
* [ ] Public docs updated (README/CONTRIBUTING) if API or defaults changed.
* [ ] No interactive code, no ODBC extras, no new runtime deps without approval.

---

## Commit & PR Conventions

* Use imperative, present-tense commit subjects (≤72 chars).
* Explain **why** and **what** in the body; reference issues (e.g., “Fixes #123”).
* Include a brief “Design notes” section in the PR describing how the change preserves the principles above and any performance considerations.

---

## Design Notes for Future Agent Work

* **Join Inspector** (future): expose approximate per-partition row counts and optional extra-ON cardinality hints; return a `DuckRel`. Keep it read-only, fast path by default.
* **CLI extra**: SQL playground (alias defaults to `this`), call-selection REPL for transformation methods only (no I/O, no DML).
* **HTML extra**: minimal table rendering for previews; no interactivity.

---

## Living Implementation Plan

* Review and update `IMPLEMENTATION_PLAN.md` before starting work on `src/duckplus` or related tests.
* Record stage progress and any reprioritization in that file so later agents understand what has shipped and what remains.
* Keep the ordering and rationale from the plan intact unless a follow-up change explicitly revises it.

---

## Historical Context (LEGACY.md)

* `LEGACY.md` documents the retired monolithic `Duck` helper that bundled connection prompts, identifier validation, filesystem discovery, IO adapters, table mutation, and debugging into a single ~3k-line class.
* Modern Duck+ intentionally split these responsibilities across focused modules (`connect`, `core`, `io`, `util`) to preserve immutability boundaries between `DuckRel` (transformations) and `DuckTable` (mutations).
* Legacy helpers relied on interactive credentials (keyring/getpass) and global extension loading (`nanodbc`). New contributions must continue to avoid interactive flows and keep extension loading explicit.
* The historical API offered convenience wrappers across Parquet/CSV/JSON/ODBC readers, HTML rendering, caching, joins, set ops, and window functions. Reintroduce functionality only when it aligns with current principles (non-interactive core, explicit projections, separation of concerns).

---

## Security & Privacy

* Do not add telemetry or phone-home code.
* Do not embed credentials or PII in code, tests, or fixtures.
* Assume all inputs can be untrusted; prefer explicit whitelists (e.g., identifier validation) where relevant.

---

By following this guide, agents can contribute safely without breaking Duck+ design guarantees.
