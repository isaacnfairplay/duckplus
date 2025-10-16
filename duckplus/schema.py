"""Schema comparison utilities for DuckPlus relations and files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from collections.abc import Iterable
import warnings

from . import io as io_helpers
from ._table_utils import require_connection
from .duckcon import DuckCon
from .relation import Relation

__all__ = [
    "ColumnTypeDrift",
    "SchemaDiff",
    "diff_relations",
    "diff_files",
]


@dataclass(frozen=True)
class ColumnTypeDrift:
    """Describe a column whose DuckDB type changed between schemas."""

    column: str
    expected_type: str
    observed_type: str


@dataclass(frozen=True)
class SchemaDiff:
    """Summary of differences between two schemas."""

    missing_from_candidate: tuple[str, ...]
    unexpected_in_candidate: tuple[str, ...]
    type_drift: tuple[ColumnTypeDrift, ...]
    baseline_label: str
    candidate_label: str

    @property
    def is_match(self) -> bool:
        """Return ``True`` when the compared schemas are identical."""

        return (
            not self.missing_from_candidate
            and not self.unexpected_in_candidate
            and not self.type_drift
        )


def diff_relations(
    baseline: Relation,
    candidate: Relation,
    *,
    baseline_label: str | None = None,
    candidate_label: str | None = None,
    warn: bool = True,
) -> SchemaDiff:
    """Compare two relations and return a schema diff summary."""

    baseline_label = baseline_label or "baseline relation"
    candidate_label = candidate_label or "candidate relation"

    baseline_map = _build_column_map(baseline)
    candidate_map = _build_column_map(candidate)

    missing_from_candidate = tuple(
        baseline_map[key][0]
        for key in sorted(baseline_map.keys() - candidate_map.keys())
    )
    unexpected_in_candidate = tuple(
        candidate_map[key][0]
        for key in sorted(candidate_map.keys() - baseline_map.keys())
    )

    type_drift = tuple(
        ColumnTypeDrift(
            column=baseline_map[key][0],
            expected_type=baseline_map[key][1],
            observed_type=candidate_map[key][1],
        )
        for key in sorted(baseline_map.keys() & candidate_map.keys())
        if baseline_map[key][1] != candidate_map[key][1]
    )

    if warn and type_drift:
        _warn_type_drift(type_drift, baseline_label, candidate_label)

    return SchemaDiff(
        missing_from_candidate=missing_from_candidate,
        unexpected_in_candidate=unexpected_in_candidate,
        type_drift=type_drift,
        baseline_label=baseline_label,
        candidate_label=candidate_label,
    )


_SUPPORTED_FORMATS = ("csv", "parquet", "json")


def diff_files(  # pylint: disable=too-many-arguments
    duckcon: DuckCon,
    baseline: Sequence[str | Path] | str | Path,
    candidate: Sequence[str | Path] | str | Path,
    *,
    file_format: str,
    baseline_options: Mapping[str, object] | None = None,
    candidate_options: Mapping[str, object] | None = None,
    warn: bool = True,
) -> SchemaDiff:
    """Compare the schemas of two file sources."""

    if file_format not in _SUPPORTED_FORMATS:
        msg = f"diff_files format '{file_format}' is not supported"
        raise ValueError(msg)

    require_connection(duckcon, "diff_files")

    reader = _resolve_reader(file_format)
    baseline_relation = reader(
        duckcon,
        baseline,
        **_normalise_options(baseline_options),
    )
    candidate_relation = reader(
        duckcon,
        candidate,
        **_normalise_options(candidate_options),
    )

    return diff_relations(
        baseline_relation,
        candidate_relation,
        baseline_label=_describe_source(baseline),
        candidate_label=_describe_source(candidate),
        warn=warn,
    )


def _build_column_map(relation: Relation) -> dict[str, tuple[str, str]]:
    return {
        column.casefold(): (column, column_type)
        for column, column_type in zip(
            relation.columns, relation.types, strict=False
        )
    }


def _warn_type_drift(
    drift: Iterable[ColumnTypeDrift],
    baseline_label: str,
    candidate_label: str,
) -> None:
    details = ", ".join(
        f"{item.column}: {item.expected_type} -> {item.observed_type}"
        for item in drift
    )
    message = (
        f"Column type drift detected between {baseline_label} and "
        f"{candidate_label}: {details}"
    )
    warnings.warn(message, UserWarning, stacklevel=3)


def _resolve_reader(file_format: str):
    mapping = {
        "csv": io_helpers.read_csv,
        "parquet": io_helpers.read_parquet,
        "json": io_helpers.read_json,
    }
    return mapping[file_format]


def _normalise_options(options: Mapping[str, object] | None) -> dict[str, object]:
    if options is None:
        return {}
    return dict(options)


def _describe_source(source: Sequence[str | Path] | str | Path) -> str:
    if isinstance(source, (str, Path)):
        return str(source)
    return ", ".join(str(item) for item in source)
