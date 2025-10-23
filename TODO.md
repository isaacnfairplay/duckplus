# Implementation Roadmap

## Preflight Discovery Questions
Answer these before starting any roadmap item to keep work aligned with the direct-Python architecture.
1. How does the change expand the Protocol-exposed surface without introducing runtime registries or heavy imports?
2. Which concrete module (e.g. `duckplus/typed/string.py`, `duckplus/io/policies.py`) should host the behaviour so the metaclass or Relation API stays discoverable from Python alone?
3. Which documentation examples and tests must be updated so Pyright and ty continue to "see" the methods through their Protocol annotations?
4. Which KPIs (import-time budget, append policy guarantees, extension autoload flows) need measurement to prove the change is production ready?
5. What follow-up validation (pytest, mypy, `uvx ty check`, `pylint`, docs build) is required before shipping the work?

## Discovery Log – Protocol coverage baseline (2025-05-27)
1. `duckplus.typed` now ships Protocols for numeric and string expressions with a metaclass that attaches runtime helpers directly from `spec/*.py` dictionaries, satisfying the no-`.pyi` mandate while keeping imports lightweight.
2. Tests under `tests/test_typed_protocols.py` assert that Protocol annotations expose the generated helpers (`strip`, `split_part`, `length`) and that return types map onto numeric Protocols, mirroring the static-analysis contract.
3. Factories in `duckplus/typed/factory.py` return concrete expression classes and provide helper casts so examples can opt into the Protocol view that Pyright and ty require.
4. Boolean expression support reuses the same metaclass path via `spec/boolean_spec.py`, new Protocols, and factories that surface logical helpers in docs and tests.

## Discovery Log – Temporal expression surface (2025-05-28)
1. Temporal expressions now flow through `spec/temporal_spec.py` with Protocol coverage, runtime classes, and factories so Pyright and ty see helpers like `date_trunc`, `extract`, and `strftime`.
2. Tests under `tests/test_typed_protocols.py` assert the SPEC/Protocol alignment and return-type bridging into numeric, boolean, and string Protocols.
3. Docs and README examples illustrate casting temporal expressions to `TemporalExprProto`, reinforcing the annotation guidance.

## Discovery Log – Relation append engine (2025-05-27)
1. `Relation.append_csv` enforces header/delimiter compatibility, integrates anti-join de-duplication, and supports partition routing plus rollover policies, keeping CSV append predictable.
2. `Relation.append_parquet` defaults to directory-based layouts and only allows single-file mutation with an explicit warning, aligning with the append policy KPIs.
3. Policy helpers in `duckplus/io/policies.py` centralise dedupe/partition/rollover logic so both CSV and Parquet writers can share behaviour without indirect registries.

## Discovery Log – Relation observability (2025-05-29)
1. `Relation.materialize()` now populates cached `row_count` and `estimated_size_bytes` properties so monitoring code can read lightweight metrics without re-materialising snapshots.
2. The runtime and README documentation highlight the new metrics, and regression tests lock the behaviour in place.

## Discovery Log – Append simulations (2025-05-30)
1. ``Relation.simulate_append_csv`` and ``Relation.simulate_append_parquet`` return planned actions so environments without ``pyarrow`` can validate partition and rollover policies.
2. Simulation dataclasses live in ``duckplus/io/simulation.py`` and are re-exported through ``duckplus.io`` for ergonomic imports.
3. Parquet simulations consume metadata sidecars written during actual appends, guaranteeing policy parity without introducing runtime registries.

## Discovery Log – Table insert callables (2025-05-30)
1. ``Table.insert`` now accepts either a ``Relation`` or a zero-argument callable returning a ``Relation`` while still forcing explicit ``materialize()`` semantics.
2. Callable inserts participate in create/overwrite flows with schema validation mirroring the eager path, backed by new tests in ``tests/test_table_insert.py``.
3. README and runtime docs document the lazy insert pattern so contributors know when to reach for callables.

## Discovery Log – Documentation refresh (2025-05-31)
1. ``docs/v2/migration.md`` documents the differences between DuckPlus 1.x and 2.0, focusing on Protocol annotations, relation semantics, and append policies.
2. ``docs/v2/typed_reference.md`` provides a quick lookup for Protocols, factories, and casting helpers so contributors can annotate code consistently.
3. The README points to the new guides and its examples now annotate string, boolean, temporal, and numeric expressions with the appropriate Protocols.


## Discovery Log – v1 archive warning cleanup (2025-06-01)
1. Normalized archived toctrees and added explicit reference stubs so the Sphinx build completes without warnings across the 1.x guides.
2. Root-level demo landing pages now link directly to the 1.4 archives, preserving quick navigation without relying on removed reference trees.

## Discovery Log – Optional dependency detection (2025-06-01)
1. Optional dependency imports now rely on ``importlib.util.find_spec`` plus ``import_module`` so linting no longer fails when ``pyarrow`` or ``pandas`` are absent.
2. ``Connection.read_excel`` and parquet helpers raise deterministic errors without wrapping imports in ``try``/``except``, keeping in line with the direct-Python API guidelines.

## Discovery Log – CSV header validation (2025-06-02)
1. ``Relation.append_csv`` now reads the stored delimiter and quoting metadata when checking headers, preventing false mismatches when a file uses custom separators.
2. Regression coverage locks the behaviour in ``tests/test_relation_append.py`` and documentation callouts explain how the ``.duckplus-meta`` sidecar preserves parser settings across appends.

## Backlog

### Protocol surface expansion
- [x] Introduce boolean expression Protocols and specs so comparison/logic helpers participate in static analysis alongside string/numeric counterparts.
- [x] Add temporal/timestamp Protocols with conversion and extract helpers, ensuring metaclass synthesis mirrors the runtime API.
- [x] Provide temporal factories (e.g. `timestamp()`) that return Protocol-aware expressions with examples in the docs and tests mirroring the usage.

### Relation and table enhancements
- [x] Extend `Relation.materialize()` with caching metrics (row count, estimated size) surfaced through read-only properties for monitoring.
- [x] Add append policy simulations that operate without `pyarrow` (e.g. CSV-only fallbacks) so environments without optional deps still validate rollover/partition flows.
- [x] Allow `Table.insert` to accept a callable (lazy relation builder) while keeping materialization explicit, accompanied by tests covering overwrite/create combinations.

### Documentation refresh
- [x] Draft migration notes comparing DuckPlus 1.x and 2.0, focusing on Protocol annotations and the metaclass runtime so contributors understand the new ergonomics.
- [x] Add a "typed expressions quick reference" page to `/docs/v2/` enumerating available Protocols and factories.
- [x] Update the README examples to annotate variables with the relevant `*ExprProto` interfaces, reinforcing the static typing guidance.

### Quality gates
The following checks remain mandatory before merging any work:
- `pytest`
- `mypy duckplus`
- `uvx ty check`
- `pylint duckplus`
- Documentation build when docs change
