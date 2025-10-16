# DuckDB bundled extension audit

This document is generated via ``scripts/audit_extensions.py`` and captures
the bundled DuckDB extensions that still require dedicated relation helpers
in DuckPlus.

| Extension | Description | Relation helpers | Status |
| --- | --- | --- | --- |
| `autocomplete` | Adds support for autocomplete in the shell | _Not applicable_ | not pursuing (CLI-only Linux shell integration) |
| `encodings` | All unicode encodings to UTF-8 | _Not applicable_ | not pursuing (DuckDB shell unicode utilities) |
| `fts` | Adds support for Full-Text Search Indexes | _None yet_ | queued |
| `httpfs` | Adds support for reading and writing files over a HTTP(S) connection | _None yet_ | queued |
| `inet` | Adds support for IP-related data types and functions | _None yet_ | queued |
| `spatial` | Geospatial extension that adds support for working with spatial data and functions | _None yet_ | queued |
| `tpcds` | Adds TPC-DS data generation and query support | _Not applicable_ | not pursuing (benchmark data generation) |
| `tpch` | Adds TPC-H data generation and query support | _Not applicable_ | not pursuing (benchmark data generation) |
| `ui` | Adds local UI for DuckDB | _Not applicable_ | not pursuing (bundled UI experience) |
