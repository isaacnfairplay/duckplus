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

## Decimal factory module
- **Module:** `duckplus/typed/expressions/decimal.py`
- **Mechanism:** The
  :func:`duckplus.typed.expressions.decimal.register_decimal_factories`
  decorator applies at class definition time so every decimal helper binds
  directly to :class:`DuckTypeNamespace` while the module exports each factory
  as a normal attribute.
- **Notes:** Importing the module attaches factories without any runtime
  rebinding. Consumer modules can now re-export the helpers using standard
  imports instead of mutating ``globals()``.

## Namespace generator script
- **Module:** `scripts/generate_function_namespaces.py`
- **Mechanism:** When rendering new namespace classes the script accumulates
  `identifier_registry` and `symbol_registry` dictionaries that become the class
  attributes mentioned above.
- **Notes:** The script encodes the remaining reliance on registries; once the
  runtime namespaces are hand-authored Python modules the generator can be
  retired entirely. See :doc:`function_namespace_generator_retirement` for the
  deprecation timeline and migration checkpoints that gate removal.
