"""Integration check that installs duckplus from git and validates typing."""

from __future__ import annotations

import json
import os
import subprocess
import textwrap
import venv
from pathlib import Path

import duckplus as repo_duckplus

BOOLEAN_HAS_COALESCE = hasattr(repo_duckplus.ducktype.Boolean("flag"), "coalesce")


def _venv_executable(venv_dir: Path, executable: str) -> Path:
    """Return the platform-specific path to an executable inside ``venv_dir``."""

    if os.name == "nt":  # pragma: no cover - Windows path handling
        suffix = ".exe"
        scripts_dir = venv_dir / "Scripts"
    else:
        suffix = ""
        scripts_dir = venv_dir / "bin"
    return scripts_dir / f"{executable}{suffix}"


def _create_virtualenv(destination: Path) -> None:
    """Create a virtual environment with pip available."""

    builder = venv.EnvBuilder(with_pip=True, clear=True)
    builder.create(destination)


def _run(pieces: list[str | os.PathLike[str]]) -> subprocess.CompletedProcess[str]:
    """Run a subprocess command and return the completed process."""

    return subprocess.run(pieces, check=True, capture_output=True, text=True)


def test_git_install_behaviour(tmp_path: Path) -> None:
    """Install from git and ensure runtime behaviour plus mypy support."""

    repo_root = Path(__file__).resolve().parents[1]
    venv_dir = tmp_path / "venv"
    _create_virtualenv(venv_dir)

    pip_exe = _venv_executable(venv_dir, "pip")
    python_exe = _venv_executable(venv_dir, "python")

    repo_uri = repo_root.as_uri()
    _run([str(pip_exe), "install", "--quiet", repo_uri])
    _run([str(pip_exe), "install", "--quiet", "mypy>=1.8.0"])

    behaviour_script = textwrap.dedent(
        """
        import json
        import duckplus

        payload = {
            "duckcon_helper_methods": [
                "read_csv",
                "read_parquet",
                "read_json",
            ],
            "boolean_has_coalesce": hasattr(duckplus.ducktype.Boolean("flag"), "coalesce"),
        }

        duckcon = duckplus.DuckCon()
        with duckcon:
            relation = duckcon.connection.sql(
                "SELECT 1 AS value UNION ALL SELECT 2 AS value UNION ALL SELECT 3 AS value"
            )
            wrapper = duckplus.Relation.from_relation(duckcon, relation)
            aggregate = (
                wrapper.aggregate()
                .agg(
                    duckplus.ducktype.Numeric.Aggregate.sum("value"),
                    alias="total",
                )
                .all()
            )
            row = aggregate.relation.fetchone()
            payload["aggregate_total"] = row[0] if row is not None else None

        payload["duckcon_closed_after"] = not duckcon.is_open

        payload["helpers_callable"] = {
            name: callable(getattr(duckplus.DuckCon, name, None))
            for name in payload["duckcon_helper_methods"]
        }
        print(json.dumps(payload))
        """
    )
    behaviour_result = _run([str(python_exe), "-c", behaviour_script])
    behaviour_payload = json.loads(behaviour_result.stdout)

    assert behaviour_payload["aggregate_total"] == 6
    assert behaviour_payload["duckcon_closed_after"]
    assert behaviour_payload["boolean_has_coalesce"] is BOOLEAN_HAS_COALESCE
    assert all(behaviour_payload["helpers_callable"].values())

    mypy_result = _run(
        [
            str(python_exe),
            "-m",
            "mypy",
            "-p",
            "duckplus",
        ]
    )
    assert "Success: no issues found" in mypy_result.stdout
