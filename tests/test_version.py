from __future__ import annotations

from pathlib import Path
import tomllib

import duckplus


def test_duckplus_version_matches_pyproject() -> None:
    """The importable version must mirror the packaged metadata."""

    project_root = Path(__file__).resolve().parents[1]
    pyproject = project_root / "pyproject.toml"
    pyproject_data = tomllib.loads(pyproject.read_text())
    project_section = pyproject_data["project"]

    assert duckplus.__version__ == project_section["version"]
