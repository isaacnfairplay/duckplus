# Sales analytics pipeline demo

The ``duckplus.examples.sales_pipeline`` module exercises the core DuckPlus
primitives in a realistic reporting scenario.  It seeds a managed
:class:`~duckplus.duckcon.DuckCon` with deterministic ``orders`` and ``returns``
relations, derives enriched metrics, and aggregates the results for leadership
reporting.  The example returns a
:class:`~duckplus.examples.sales_pipeline.SalesDemoReport` dataclass so that tests
and documentation can embed the generated artefacts directly.  The module ships
with docstrings that mirror this page, making it easy to jump between prose and
code while investigating.

## Running the walkthrough

Execute the module to build the in-memory dataset and print the captured
summaries::

    python -m duckplus.examples.sales_pipeline

The command prints region-level and channel-level results followed by a sample
SELECT statement emitted by the typed builder.

```{tip}
The demo requires no external data sources—the dataset is synthesised from
Python literals so it runs identically on every machine.  This makes it ideal
for onboarding sessions or quick smoke tests when you upgrade DuckDB.
```

## Preview rows

The helper stores a compact preview to make doc examples reproducible.  The
first five enriched rows are::

    (1, 2024-06-01, 'north', 'acme', 'online', False, 120.0, 18.5, None,
     101.5, 7.105, 94.395, False, 'starter', False)
    (2, 2024-06-01, 'north', 'acme', 'field', True, 240.0, 22.0,
     'Damaged packaging', 218.0, 15.26, 202.74, False, 'growth', True)
    (3, 2024-06-02, 'west', 'venture', 'field', False, 310.0, 35.0, None,
     275.0, 19.25, 255.75, True, 'growth', False)
    (4, 2024-06-02, 'west', 'venture', 'online', False, 180.0, 15.0, None,
     165.0, 11.55, 153.45, False, 'starter', False)
    (5, 2024-06-03, 'south', 'nomad', 'online', True, 95.0, 9.0,
     'Late delivery', 86.0, 6.02, 79.98, False, 'starter', True)

The values mirror the tuples stored in
:attr:`SalesDemoReport.preview_rows <duckplus.examples.sales_pipeline.SalesDemoReport.preview_rows>`.
Because the dataclass captures both the enriched relation and its metadata, you
can assert on ``report.preview_columns`` in tests to confirm column order and
retain deterministic docs.

## Region performance

``SalesDemoReport.region_rows`` summarises return rates and revenue by sales
region.  The deterministic output enables the documentation and tests to agree
on the same numbers.  The aggregation uses typed expressions for ``sum`` and
``count_if`` to demonstrate how numeric helpers compose:

| region | total_orders | net_revenue | high_value_orders | return_rate |
| ------ | ------------ | ----------- | ----------------- | ----------- |
| east   | 2            | 301.0       | 1                 | 0.50        |
| north  | 2            | 319.5       | 0                 | 0.50        |
| south  | 2            | 448.0       | 1                 | 0.50        |
| west   | 2            | 440.0       | 1                 | 0.00        |

## Channel performance

The channel summary surfaces repeat behaviour and contribution averages::

    ('field', 2, 1, 229.245)
    ('online', 4, 1, 166.12125)
    ('partner', 2, 1, 139.965)

These rows correspond to
:attr:`SalesDemoReport.channel_rows <duckplus.examples.sales_pipeline.SalesDemoReport.channel_rows>`.  Call
``summarise_by_channel`` from the module when you need to recompute the relation
for exploratory analysis.

## Typed projection example

The demo emits the typed SELECT used to showcase ``if_exists`` clauses.  It
replaces the ``service_tier`` column with a computed label while falling back to
``fulfilled`` when ``return_reason`` is absent::

    SELECT * REPLACE (
        CASE WHEN "is_returned" THEN 'service'
             WHEN "is_high_value" THEN 'priority'
             ELSE "service_tier" END AS "service_tier",
        CASE WHEN "return_reason" IS NULL THEN 'fulfilled'
             ELSE "return_reason" END AS "return_reason"
    ),
    sum("net_revenue") AS "cumulative_net"
    FROM enriched_orders

Because the SELECT builder is dependency-aware, the optional clauses disappear
if an upstream relation omits ``return_reason`` or ``net_revenue``.  Reuse the
``build_enriched_orders`` helper in your own scripts when you want to add new
metrics or persist the intermediate relation to disk.
