# Language Server Demonstration

This walkthrough exercises the Pyright language server against the statically generated DuckDB function catalog. The goal is to
show, with real LSP responses, the completions and type information surfaced to IDEs.

## Prerequisites

* Node.js 18+ (Pyright ships as an npm package).
* Run `npm install` from the repository root to install the pinned `pyright` dependency declared in `package.json`.
* Ensure Python 3.11+ is available so the DuckPlus package itself can be imported.

## Running the demo

Execute the helper script from the project root:

```bash
python scripts/demo_language_server.py
```

The script launches `pyright-langserver` over stdio, opens a synthetic module that imports the typed function namespaces, and
issues completion, hover, and signature-help requests at representative positions. No DuckDB connection is required; everything
runs against the generated catalog.

## Sample output

```
initialize: 16 capabilities

Completion for SCALAR_FUNCTIONS.Varchar:
  - alias
  - array_extract
  - array_to_json
  - bar
  - base64

Completion for SCALAR_FUNCTIONS.Numeric:
  - abs
  - acos
  - acosh
  - add
  - age

Completion for AGGREGATE_FUNCTIONS.Numeric:
  - any_value
  - approx_count_distinct
  - approx_quantile
  - arbitrary
  - arg_max

Hover for SCALAR_FUNCTIONS.Varchar.lower:
  (variable) lower: _DuckDBFunctionCall[VarcharExpression]

Hover for SCALAR_FUNCTIONS.Numeric.abs:
  (variable) abs: _DuckDBFunctionCall[NumericExpression]

Hover for AGGREGATE_FUNCTIONS.Numeric.sum:
  (variable) sum: _DuckDBFunctionCall[NumericExpression]

Signature help for SCALAR_FUNCTIONS.Varchar.lower:
  (*operands: object) -> VarcharExpression
```

The completions prove that the language server sees the generated function members without executing DuckDB, and the hover/
signature help show the typed expression returned by each call. IDEs can therefore surface the available functions and their
return categories directly from static analysis.

