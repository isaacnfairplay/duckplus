# Always Do these steps before commit
1. Make sure docs are up to date
2. run mypy and uvx and pylint
3. run tests
4. make sure all tests pass
5. add any tests that are needed
6. make sure they past
7. Guarantee every function that contributes to DuckPlus behaviour is defined directly in Python modules (classes, functions, decorators) rather than being described through runtime-loaded registries, dicts, or dataclass metadata.
8. Keep TODO.md aligned with the fluent, direct-Python API direction—update it whenever work reveals new insights or invalidates existing guidance.

# Users will develop the api surface requests in TODO.md
1. harmonously work to implement the new feature
2. Ensure it has no unexpected behaviour and does not regress
3. hide it in TODO.md

## TODO Stewardship Directive
- Keep the preflight discovery questions in `TODO.md` current. After checking off any TODO item, review the repository for new learnings and update the question list so it stays relevant and high-signal for upcoming work.
