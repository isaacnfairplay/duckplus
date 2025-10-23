"""Append policy helpers for DuckPlus relations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class AppendDedupe:
    """Deduplication strategy applied before appending rows."""

    mode: str = "none"
    keys: Sequence[str] | None = None
    on_all_columns: bool = False

    def filter_rows(
        self, existing_rows: Iterable[Mapping[str, object]],
        new_rows: Sequence[Mapping[str, object]],
        columns: Sequence[str],
    ) -> list[Mapping[str, object]]:
        if self.mode != "anti_join":
            return list(new_rows)

        keys = list(columns) if self.on_all_columns else list(self.keys or [])
        if not keys:
            keys = list(columns)
        existing_index = {
            tuple(_normalise_for_index(row.get(col)) for col in keys) for row in existing_rows
        }
        deduped: list[Mapping[str, object]] = []
        for row in new_rows:
            key = tuple(_normalise_for_index(row.get(col)) for col in keys)
            if key in existing_index:
                continue
            deduped.append(row)
        return deduped


@dataclass(frozen=True)
class Partition:
    """Partition routing options."""

    scheme: str = "none"
    column: str | None = None
    buckets: int | None = None

    def route(self, row: Mapping[str, object]) -> str | None:
        if self.scheme == "none":
            return None
        if self.scheme == "by_column":
            if not self.column:
                raise ValueError("column must be provided for by_column partitioning")
            value = row.get(self.column)
            return f"{self.column}={value}"
        if self.scheme == "hash":
            if not self.column or not self.buckets:
                raise ValueError("column and buckets must be set for hash partitioning")
            value = row.get(self.column)
            bucket = hash(value) % self.buckets
            return f"{self.column}={bucket}"
        raise ValueError(f"Unsupported partition scheme: {self.scheme}")

    def ensure_directory(self, base_path: Path, partition_key: str | None) -> Path:
        if not partition_key:
            return base_path
        target = base_path / partition_key
        target.mkdir(parents=True, exist_ok=True)
        return target


def _normalise_for_index(value: object) -> object:
    if value is None:
        return None
    return str(value)


@dataclass(frozen=True)
class Rollover:
    """Rollover options for file-based appends."""

    max_rows: int | None = None
    max_size_mb: int | None = None

    def select_target(self, base_path: Path, *, force_new: bool = False) -> Path:
        if base_path.is_dir():
            return base_path
        if not base_path.exists():
            return base_path
        if not force_new:
            return base_path

        # Generate a new filename with an incrementing suffix.
        stem = base_path.stem
        suffix = base_path.suffix
        parent = base_path.parent
        index = 1
        while True:
            candidate = parent / f"{stem}_{index}{suffix}"
            if not candidate.exists():
                return candidate
            index += 1
