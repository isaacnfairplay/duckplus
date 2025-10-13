# AGENTS.md

Guidance for automated contributors to Duck+ (`duckplus`). Focus on long-term
library stability, accurate documentation, and consistently high quality output.

---

## Mission

* Keep the public API predictable and backwards compatible unless a change is
  explicitly approved.
* Maintain a user-first documentation set that explains how to install,
  configure, and extend Duck+ without assuming prior knowledge of the
  codebase.
* Invest in sustainable improvements (refactors, tests, docs) that keep the
  project healthy over time rather than short-lived hacks.

---

## Long-term guardrails

* Python support: 3.12 and newer. DuckDB support: 1.3.0 and newer.
* Package name remains `duckplus`; license is MIT.
* Preserve the two core wrappers: `DuckRel` for immutable relational
  operations, `DuckTable` for mutation helpers.
* Never add interactive prompts, background threads, or network-bound side
  effects to the core package.
* Keep joins explicit about projection, casing, and collision handling.

---

## Documentation standards

* Update README, docs, and examples whenever behavior changes so users always
  see current, task-oriented guidance.
* Prefer concise “how to” sections, sample code, and clear explanations of
  defaults. Avoid internal process notes unless they help users succeed.
* Remove stale or duplicative documents when they no longer reflect the
  project.
* When editing docs, check for consistency across README, `docs/`, and inline
  docstrings.

---

## Code quality expectations

* Keep functions small, typed, and well-documented.
* Add or update tests for every user-visible change. Favor in-memory DuckDB
  usage in tests; never rely on external services.
* Do not introduce new runtime dependencies without approval.
* Maintain strict typing: `mypy --strict` must continue to pass.

---

## Required commands before submitting changes

```bash
uv sync           # ensure the environment matches project expectations
uv run pytest     # run the test suite
uv run mypy src/duckplus  # strict type checking
uvx ty src/duckplus       # supplemental static analysis
```

---

## Preferred workflow

1. Scope the change narrowly and confirm it aligns with long-term goals.
2. Update documentation alongside code.
3. Run the required commands above.
4. Prepare commits and PR descriptions that explain the motivation, behavior
   changes, and any follow-up work.
