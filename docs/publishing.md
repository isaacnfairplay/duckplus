# Publishing Duck+ (`duckplus`) to PyPI

Duck+ ships as a standard Python distribution managed by `uv`. Follow this
checklist to cut a release and publish it to PyPI.

## 1. Prepare the release branch

1. Update the version in `pyproject.toml`.
2. Commit the change with an informative message (for example,
   `Prepare release X.Y.Z`).
3. Ensure `duckplus.__version__` reports the same value by importing it from a
   local checkout (this happens automatically when the project version changes).

## 2. Run the validation suite

Run the same checks that CI executes. All commands are mediated through `uv`:

```bash
uv run pytest
uv run mypy src/duckplus
uvx ty check src/duckplus
```

If any step fails, fix the regressions before proceeding.

## 3. Build distribution artifacts

Use `uv build` to produce the source distribution (`sdist`) and wheel:

```bash
uv build
```

The outputs land in the `dist/` directory. Inspect the metadata with
`uv build --wheel --sdist --out-dir dist/` if you need to regenerate them.

## 4. Smoke-test the wheel

Install the freshly built wheel into a temporary environment to make sure the
public API imports correctly:

```bash
uv pip install --python 3.12 dist/duckplus-*.whl
uv run --python 3.12 python -c "import duckplus; print(duckplus.__version__)"
```

When finished, remove the temporary environment or uninstall the package using
`uv pip uninstall --python 3.12 duckplus` from the same interpreter.

## 5. Publish to PyPI

Export an API token with publishing permissions and push the release using
`uv publish`:

```bash
export PYPI_TOKEN="pypi-..."
uv publish --token "$PYPI_TOKEN"
```

`uv publish` uploads both the wheel and source distribution while preserving the
metadata declared in `pyproject.toml`.

## 6. Tag the release

After the upload succeeds, create a git tag that matches the version string and
push it to GitHub so future changelog entries can reference it:

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

Document the release in your changelog if new user-facing features shipped as
part of the version.
