# Language Server Demonstration

This walkthrough exercises the Pyright language server against the statically generated DuckDB function catalog that ships with DuckPlus. The goal is to show, with real LSP responses, the completions and type information surfaced to IDEs.

## Prerequisites

* Node.js 18+ (Pyright ships as an npm package).
* Run `npm install` from the repository root to install the pinned `pyright` dependency declared in `package.json`.
* Ensure Python 3.11+ is available so the DuckPlus package itself can be imported.

## Running the demo

Execute the helper script from the project root:

```bash
python scripts/demo_language_server.py
```

The script launches `pyright-langserver` over stdio, opens a synthetic module that imports the typed function namespaces directly, and issues completion, hover, and signature-help requests at representative positions. No DuckDB connection is required; everything runs against the generated catalog bundled with the package.

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
  (method) ScalarVarcharFunctions.lower(*operands: object) -> VarcharExpression

  Call DuckDB function ``lower``.

  Overloads:
  - main.lower(VARCHAR col0) -> VARCHAR

Hover for SCALAR_FUNCTIONS.Numeric.abs:
  (method) ScalarNumericFunctions.abs(*operands: object) -> NumericExpression

  Call DuckDB function ``abs``.

  Overloads:
  - main.abs(NUMERIC col0) -> NUMERIC

Hover for AGGREGATE_FUNCTIONS.Numeric.sum:
  (method) AggregateNumericFunctions.sum(*operands: object) -> NumericExpression

  Call DuckDB function ``sum``.

  Overloads:
  - main.sum(NUMERIC col0) -> NUMERIC

Signature help for SCALAR_FUNCTIONS.Varchar.lower:
  (*operands: object) -> VarcharExpression
```

The completions prove that the language server sees the generated function members without executing DuckDB, and the hover/
signature help now surface each helper’s docstring alongside the typed expression returned by the call. IDEs can therefore
display both the overload metadata and return categories directly from static analysis.

