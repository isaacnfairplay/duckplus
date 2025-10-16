"""Utilities for auditing DuckDB extension coverage."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping, Sequence

from .duckcon import DuckCon, ExtensionInfo

__all__ = [
    "BundledExtensionAuditEntry",
    "COMMUNITY_EXTENSION_NAMES",
    "DEFAULT_BUNDLED_HELPER_COVERAGE",
    "audit_bundled_extensions",
    "collect_bundled_extension_audit",
]

# DuckDB ships a handful of extensions that are distributed alongside the
# engine. These are distinct from the community extensions that live in the
# ``duckdb-extensions`` bundle and from the statically linked primitives such as
# ``json`` or ``parquet``. The constants below provide the heuristics DuckPlus
# uses to identify bundled extensions.
COMMUNITY_EXTENSION_NAMES = frozenset(
    {
        "aws",
        "azure",
        "delta",
        "ducklake",
        "excel",
        "iceberg",
        "motherduck",
        "mysql_scanner",
        "postgres_scanner",
        "sqlite_scanner",
        "vss",
    }
)
_STATICALLY_LINKED_INSTALL_MODES = frozenset({"STATICALLY_LINKED"})


@dataclass(frozen=True)
class BundledExtensionAuditEntry:
    """Snapshot describing relation helper coverage for a bundled extension."""

    info: ExtensionInfo
    helper_names: tuple[str, ...]

    @property
    def has_helper(self) -> bool:
        """Return ``True`` when at least one relation helper covers the extension."""

        return bool(self.helper_names)


# Mapping of extension names to the relation helpers that already cover them.
# The audit defaults to this mapping but allows callers to provide richer
# context—useful during tests or when additional helpers are introduced in the
# future.
DEFAULT_BUNDLED_HELPER_COVERAGE: Mapping[str, tuple[str, ...]] = {}


def _is_bundled_extension(info: ExtensionInfo) -> bool:
    """Return ``True`` if *info* represents a DuckDB bundled extension."""

    install_mode = (info.install_mode or "").upper()
    if install_mode in _STATICALLY_LINKED_INSTALL_MODES:
        return False

    if info.name in COMMUNITY_EXTENSION_NAMES:
        return False

    return True


def audit_bundled_extensions(
    infos: Sequence[ExtensionInfo],
    *,
    helper_coverage: Mapping[str, Sequence[str]] | None = None,
) -> tuple[BundledExtensionAuditEntry, ...]:
    """Return audit entries for DuckDB bundled extensions.

    Parameters
    ----------
    infos:
        Extension metadata as returned by :meth:`DuckCon.extensions`.
    helper_coverage:
        Optional mapping of extension names to the relation helpers that
        currently cover them. Providing the mapping allows callers to augment the
        default DuckPlus coverage information.
    """

    if helper_coverage is None:
        helper_coverage = DEFAULT_BUNDLED_HELPER_COVERAGE

    entries: list[BundledExtensionAuditEntry] = []
    for info in infos:
        if not _is_bundled_extension(info):
            continue

        helpers = tuple(helper_coverage.get(info.name, ()))
        entry = BundledExtensionAuditEntry(info=info, helper_names=helpers)
        entries.append(entry)

    entries.sort(key=lambda entry: entry.info.name.lower())
    return tuple(entries)


def collect_bundled_extension_audit(
    duckcon: DuckCon,
    *,
    helper_coverage: Mapping[str, Sequence[str]] | None = None,
) -> tuple[BundledExtensionAuditEntry, ...]:
    """Convenience wrapper that opens DuckCon metadata for auditing."""

    infos = duckcon.extensions()
    return audit_bundled_extensions(infos, helper_coverage=helper_coverage)
