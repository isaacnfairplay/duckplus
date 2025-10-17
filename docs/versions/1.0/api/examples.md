# ``duckplus.examples``

Example modules provide runnable demonstrations that double as regression tests.
Importing :mod:`duckplus.examples` exposes the curated entry points directly.

- ``pi_demo`` – Monte Carlo estimation of π showcasing immutable relations and the
  typed expression DSL.
- ``sales_pipeline`` – deterministic sales analytics workflow that underpins the
  practitioner demo and validates aggregation helpers.

Run ``python -m duckplus.examples.sales_pipeline`` to regenerate the sample
reports or inspect the modules referenced from the guides for inline commentary.
