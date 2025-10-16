"""Generate a Markdown report summarising DuckDB bundled extension coverage."""

# pylint: disable=wrong-import-position

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from duckplus.duckcon import DuckCon
from duckplus.extensions import (
    DEFAULT_BUNDLED_HELPER_COVERAGE,
    collect_bundled_extension_audit,
)

DOC_PATH = ROOT / "docs" / "extensions_audit.md"


# Some bundled extensions ship ergonomics that are intentionally out of scope
# for DuckPlus. Capture these decisions so the generated report records the
# rationale instead of suggesting helper work that does not align with the
# project goals.
STATUS_OVERRIDES: dict[str, dict[str, str]] = {
    "autocomplete": {
        "helpers": "_Not applicable_",
        "status": "not pursuing (CLI-only Linux shell integration)",
    },
    "encodings": {
        "helpers": "_Not applicable_",
        "status": "not pursuing (DuckDB shell unicode utilities)",
    },
    "tpcds": {
        "helpers": "_Not applicable_",
        "status": "not pursuing (benchmark data generation)",
    },
    "tpch": {
        "helpers": "_Not applicable_",
        "status": "not pursuing (benchmark data generation)",
    },
    "ui": {
        "helpers": "_Not applicable_",
        "status": "not pursuing (bundled UI experience)",
    },
}


def _format_description(description: str | None) -> str:
    if not description:
        return ""
    return description.replace("|", "\\|")


def _format_helpers(name: str, helper_names: tuple[str, ...]) -> tuple[str, str]:
    override = STATUS_OVERRIDES.get(name)
    if override is not None:
        return override.get("helpers", ""), override.get("status", "")

    if helper_names:
        helpers = ", ".join(f"`{helper_name}`" for helper_name in helper_names)
        status = "covered"
    else:
        helpers = "_None yet_"
        status = "queued"
    return helpers, status


def main() -> None:
    manager = DuckCon()
    with manager:
        audit = collect_bundled_extension_audit(
            manager, helper_coverage=DEFAULT_BUNDLED_HELPER_COVERAGE
        )

    lines = [
        "# DuckDB bundled extension audit",
        "",
        "This document is generated via ``scripts/audit_extensions.py`` and captures",
        "the bundled DuckDB extensions that still require dedicated relation helpers",
        "in DuckPlus.",
        "",
        "| Extension | Description | Relation helpers | Status |",
        "| --- | --- | --- | --- |",
    ]

    for entry in audit:
        helpers, status = _format_helpers(entry.info.name, entry.helper_names)
        description = _format_description(entry.info.description)
        lines.append(
            f"| `{entry.info.name}` | {description} | {helpers} | {status} |"
        )

    DOC_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
