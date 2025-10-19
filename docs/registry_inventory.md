# Registry Inventory

This snapshot tracks the remaining places where DuckPlus still relies on
dictionary-driven registries or module-level loaders so future work can retire
them in favour of direct Python definitions. Each entry notes the module, the
mechanism that still performs runtime registration, and the current reason it
remains in place.

## Typed function namespaces
- **Module:** `duckplus/typed/functions.py`
- **Mechanism:** `_StaticFunctionNamespace` keeps `_IDENTIFIER_FUNCTIONS` and
  `_SYMBOLIC_FUNCTIONS` dictionaries that map public names to concrete method
  implementations during class construction.
- **Notes:** The decorator-based helpers populate the dictionaries to retain
  compatibility with generated namespaces and user lookups. Transitioning fully
  to decorator-managed attributes will let the mappings be removed once
  generated modules are rewritten.

## Generated typed function catalog
- **Module:** `duckplus/typed/_generated_function_namespaces.py`
- **Mechanism:** Each generated namespace class includes `_IDENTIFIER_FUNCTIONS`
  (and sometimes `_SYMBOLIC_FUNCTIONS`) dictionaries filled with literal
  entries. These are emitted by `scripts/generate_function_namespaces.py` and are
  consumed by `_StaticFunctionNamespace` to resolve helper lookups.
- **Notes:** The generator script still builds the dictionaries because the
  output needs to expose thousands of functions. Replacing the generator with
  per-function modules would allow decorator-based registration instead of
  writing dictionary literals.

## Decimal factory shims
- **Module:** `duckplus/typed/expression.py`
- **Mechanism:** `_attach_decimal_factories` attaches every decimal helper to the
  `DuckTypeNamespace` class while `_register_decimal_factories` rebuilds the
  `_decimal_names` list on each instance for legacy consumers.
- **Notes:** The shim keeps the existing API surface intact while the fluent
  namespace migrates away from runtime caches. Once callers switch to the
  decorator-populated attributes, the instance-level rebinding can be removed.

## Module-level decimal exports
- **Modules:** `duckplus/typed/__init__.py`, `duckplus/typed/ducktype.py`
- **Mechanism:** Both modules iterate over `ducktype.decimal_factory_names` and
  inject each decimal helper into their module namespaces via `globals()`.
- **Notes:** The loops ensure imports like `from duckplus import Decimal_18_2`
  continue to work even though the factories now live on `DuckTypeNamespace`.
  Stabilising a direct import surface would let the loops disappear.

## Namespace generator script
- **Module:** `scripts/generate_function_namespaces.py`
- **Mechanism:** When rendering new namespace classes the script accumulates
  `identifier_registry` and `symbol_registry` dictionaries that become the class
  attributes mentioned above.
- **Notes:** The script encodes the remaining reliance on registries; once the
  runtime namespaces are hand-authored Python modules the generator can be
  retired entirely.
