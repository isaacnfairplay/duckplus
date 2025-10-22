"""Helpers for accessing project configuration in tests."""

from __future__ import annotations

from functools import cache
from pathlib import Path

import tomllib


@cache
def project_version_tuple() -> tuple[int, ...]:
    """Return the project version as a tuple of integers."""

    root = Path(__file__).resolve().parents[1]
    data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    version = data["project"]["version"]
    parts = []
    for piece in version.split("."):
        number = ""
        for character in piece:
            if character.isdigit():
                number += character
            else:
                break
        if not number:
            break
        parts.append(int(number))
    return tuple(parts)
