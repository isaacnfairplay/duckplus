# Sanitised traceability pipeline

The internal traceability scripts contain rich examples of DuckDB syntax, but
those flows reference tables and paths that cannot be published verbatim.  This
walkthrough mirrors the original data movements with anonymised relations and
is backed by executable tests so the documentation stays in sync with the code.

```{tip}
Each code listing below is pulled directly from
``tests/test_examples_traceability_pipeline.py``.  The tests double as a smoke
suite while guaranteeing the documentation never diverges from reality.
```

## Ranking anonymised programs

The first helper demonstrates how to combine catalogue fragments with recent log
entries.  It uses CTEs to capture the probe barcode, correlate matching
fragments, and prioritise results using ``ROW_NUMBER``.

```{note}
The full traceability pipeline example source was archived with the DuckPlus 1.x release bundle. Checkout the matching tag and rebuild the docs to review the exact implementation.
```

## Collecting companion barcodes

The panel lookup mirrors the private alternate search logic with neutral column
names.  It performs a panel-based join and unions results from an alternate
capture stream when available.

```{note}
The full traceability pipeline example source was archived with the DuckPlus 1.x release bundle. Checkout the matching tag and rebuild the docs to review the exact implementation.
```

## Repairing unit costs

The final example rebuilds the rename-and-aggregate pipeline that restores
missing costs.  It aggregates the latest price snapshots, computes fallbacks per
item, and merges the healed rows with the untouched events.

```{note}
The full traceability pipeline example source was archived with the DuckPlus 1.x release bundle. Checkout the matching tag and rebuild the docs to review the exact implementation.
```
