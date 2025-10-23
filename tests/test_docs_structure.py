from __future__ import annotations

import json
from pathlib import Path


def test_version_switcher_lists_v1_and_v2() -> None:
    switcher = Path("docs/_static/version_switcher.json")
    payload = json.loads(switcher.read_text(encoding="utf-8"))
    names = {entry["url"] for entry in payload}
    assert "v1/" in names
    assert "v2/" in names

