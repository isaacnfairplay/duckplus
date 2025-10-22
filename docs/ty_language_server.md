# Ty language server compatibility

The `ty` type checker includes a language server that mirrors the CLI's static
analysis. DuckPlus exercises that server against the full static-typed API so
editors surface every helper that succeeds at runtime.

## Coverage strategy

* **Attribute discovery.** A generated contract enumerates every public
  attribute on factories, expressions, CASE builders, aggregate factories, and
  the SELECT builder. The rendered snippet is fed to `ty` so completions stay in
  sync with the runtime API, catching any helper that the language server would
  otherwise miss.【F:tests/typecheck_cases.py†L32-L123】
* **Positive usage scenarios.** Shared fixtures cover column factories, CASE
  expressions, SELECT composition, function namespaces, and type metadata. The
  same source snippets are executed by both `mypy` and `ty`, guaranteeing that
  successful editor hovers and completions reflect runnable Python code.【F:tests/typecheck_cases.py†L220-L514】
* **Diagnostic parity.** Negative tests assert the exact diagnostics that `ty`
  should report when callers invoke unsupported helpers, pass invalid argument
  types, or mix incompatible expressions. This ensures the language server calls
  out mistakes that would raise at runtime.【F:tests/typecheck_cases.py†L240-L365】

## Release gating

The contract harness collects every `ty` run, records failures, and gates the
release according to the package version declared in `pyproject.toml`. Versions
prior to 1.4.6 treat the suite as informational, while 1.4.6 requires an
80 percent pass rate and 1.4.7 will demand full parity. The accumulated report
is exposed through pytest so regression output highlights any missing language
server coverage.【F:tests/test_ty_integration.py†L13-L94】

## What this means in editors

Because the same code drives runtime imports and the `ty` language server
checks, autocompletion sees every typed helper (including dynamically attached
methods like `split_part`) and flags incorrect usage with the same diagnostics
as direct execution. IDEs can therefore rely on the bundled API surface without
custom plugins or manual stubs.【F:tests/typecheck_cases.py†L32-L189】【F:tests/typecheck_cases.py†L514-L676】
