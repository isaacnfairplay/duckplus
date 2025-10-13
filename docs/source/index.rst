Duck+ documentation
===================

Welcome to the Duck+ documentation set. Duck+ offers typed, immutable wrappers
around DuckDB so you can compose analytics pipelines with predictable joins,
explicit casing, and safe mutation helpers.

.. toctree::
   :maxdepth: 2
   :caption: Reference

   api_reference.rst

.. toctree::
   :maxdepth: 1
   :caption: Guides

   aggregate_demos.rst
   typed_pipeline_demos.rst

Getting started
---------------

Duck+ is published on PyPI as ``duckplus`` and targets Python 3.12+.

.. code-block:: bash

   uv pip install duckplus

If you are contributing to the project, clone the repository and follow the
setup commands in the repository ``README.md`` to run tests, type checking, and
docs builds locally.
