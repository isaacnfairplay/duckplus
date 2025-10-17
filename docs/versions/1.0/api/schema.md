# ``duckplus.schema``

Schema helpers compare relations or files and surface drift in a structured way.
The module exports frozen dataclasses for reporting diffs along with utilities
for loading file-based schemas through the IO layer.

## Dataclasses

- ``ColumnTypeDrift`` – describes a column whose DuckDB type changed. Fields:
  ``column`` (original name), ``expected_type`` (baseline type string), and
  ``observed_type`` (candidate type string).
- ``SchemaDiff`` – aggregates missing and unexpected columns together with type
  drift. Fields:

  - ``missing_from_candidate`` – tuple of baseline columns absent from the
    candidate.
  - ``unexpected_in_candidate`` – tuple of columns present only in the candidate.
  - ``type_drift`` – tuple of :class:`ColumnTypeDrift` instances.
  - ``baseline_label`` / ``candidate_label`` – human-friendly identifiers used in
    warnings and reports.
  - ``is_match`` – property returning ``True`` when the schemas are identical.

## Functions

- ``diff_relations(baseline, candidate, *, baseline_label=None, candidate_label=None, warn=True)`` –
  compare two relations. Returns :class:`SchemaDiff` and optionally emits a
  ``UserWarning`` summarising type drift.
- ``diff_files(duckcon, baseline, candidate, *, file_format, baseline_options=None, candidate_options=None, warn=True)`` –
  compare files by loading them through :mod:`duckplus.io`. ``file_format`` must
  be ``"csv"``, ``"parquet"``, or ``"json"``; incompatible values raise
  ``ValueError``.

Both helpers require an open :class:`duckplus.duckcon.DuckCon` when reading from
files and reuse ``diff_relations`` internally so callers receive consistent
structures regardless of input sources.
