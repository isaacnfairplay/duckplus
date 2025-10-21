# Community extension integration targets

> **Note**
> A high-level summary for users now ships with the versioned docs at
> {doc}`versions/1.4/community_extensions <versions/1.4/community_extensions>`.

This planning note summarises the high-priority DuckDB community extensions
identified for DuckPlus integration and records which pieces of the public API
are expected to change when each extension is supported.

| Extension | Primary capabilities | Impacted DuckPlus API surface | Notes |
| --- | --- | --- | --- |
| `zipfs` | Virtual filesystem for navigating and reading files stored inside ZIP archives. | `DuckCon` extension loader, `duckplus.io` readers (CSV/Parquet/JSON helpers), relation ingest conveniences. | Add `_load_zipfs()` on `DuckCon` and extend IO helpers to understand archive URIs (e.g. `zip://path::file.csv`). |
| `yaml` | Table function to ingest YAML documents with schema inference controls. | `DuckCon` extension loader, `duckplus.io` ingestion helpers, typed schema utilities. | Provide `Relation.from_yaml`/`duckplus.io.read_yaml` wrappers plus typed column mapping helpers for nested YAML structures. |
| `webbed` | HTML and XML extraction via DOM-aware table functions and XPath helpers. | `DuckCon` loader, `duckplus.io` scraping helpers, typed expression functions for XPath/text extraction. | Plan ergonomic wrappers for `read_html`, HTML table flattening, and typed helpers for XPath/css query functions. |
| `stochastic` | Probability distributions, random sampling, and statistical utility functions. | Typed expression library (`duckplus.static_typed`), relation transformation helpers. | Surface typed function wrappers (e.g. `ducktype.Numeric.statistical.cdf`) and ensure relation helpers can register stochastic UDF outputs. |
| `msolap` | Connectivity to Microsoft Analysis Services (MSOLAP) cubes through the ADBC bridge. | `DuckCon` loader, IO/connectivity helpers, relation factory for remote datasets. | Offer `_load_msolap()` plus connection helpers that return relations bound to remote cube queries. |
| `markdown` | Markdown document parser that extracts structured content such as headings and tables. | `DuckCon` loader, `duckplus.io` ingestion helpers, relation transformation utilities. | Add readers for Markdown sections/tables and convenience filters for heading hierarchies. |
| `miniplot` | ASCII/Unicode plotting utilities driven from DuckDB relations. | Typed expression formatting helpers, relation export utilities. | Provide helpers that render plots from `Relation` selections and wrap miniplot plotting functions within typed expression DSL for configuration. |
| `marisa` | High-performance static tries for prefix lookup and predictive text operations. | Typed expression DSL, relation transformation helpers. | Surface typed wrappers for MARISA lookup functions and ensure relation helpers expose index build/apply workflows. |
| `hdf5` | Reader for HDF5 datasets with support for hierarchical groups and typed datasets. | `DuckCon` loader, `duckplus.io` ingestion helpers, schema mapping utilities. | Add `_load_hdf5()` alongside `Relation.from_hdf5` wrappers with options for dataset selection and column typing. |
| `rapidfuzz` | Fast fuzzy string matching and similarity scoring functions. | Typed expression DSL, relation filter helpers. | Create typed wrappers for RapidFuzz similarity/distance functions and integrate with relation filtering/sorting helpers. |

The table should be kept in sync with the roadmap in `TODO.md` so upcoming
extension work is scoped before implementation begins.
