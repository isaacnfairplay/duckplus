"""Immutable relation implementation."""

from __future__ import annotations

import csv
import importlib.util
import json
import warnings
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, cast

from duckplus.core.exceptions import AppendError
from duckplus.io.policies import AppendDedupe, Partition, Rollover
from duckplus.io.simulation import AppendSimulation, AppendSimulationAction


@dataclass(frozen=True)
class RelationSnapshot:
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]

    def to_dicts(self) -> list[dict[str, object]]:
        return [dict(zip(self.columns, row)) for row in self.rows]


class Relation:
    """Immutable dataset handle."""

    __slots__ = ("_snapshot", "_materialized", "_row_count", "_estimated_size_bytes")

    def __init__(
        self,
        columns: Sequence[str],
        rows: Iterable[Mapping[str, object]],
        *,
        materialized: bool = True,
    ) -> None:
        normalized_columns = tuple(columns)
        normalized_rows = tuple(
            tuple(row.get(column) for column in normalized_columns) for row in rows
        )
        self._snapshot = RelationSnapshot(normalized_columns, normalized_rows)
        self._materialized = materialized
        self._row_count = len(normalized_rows)
        self._estimated_size_bytes = sum(
            _estimate_row_size(normalized_columns, row)
            for row in normalized_rows
        )

    @property
    def columns(self) -> tuple[str, ...]:
        return self._snapshot.columns

    def __iter__(self):
        for row in self._snapshot.rows:
            yield dict(zip(self.columns, row))

    def materialize(self) -> "Relation":
        if self._materialized:
            return self
        return Relation(self.columns, self._snapshot.to_dicts(), materialized=True)

    @property
    def row_count(self) -> int:
        """Return the cached row count for the relation."""

        return self._row_count

    @property
    def estimated_size_bytes(self) -> int:
        """Return the estimated in-memory footprint for the relation."""

        return self._estimated_size_bytes

    # -- CSV -----------------------------------------------------------------
    def write_csv(
        self,
        path: str | Path,
        *,
        delimiter: str = ",",
        quoting: str = '"',
        include_header: bool = True,
    ) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, delimiter=delimiter, quotechar=quoting)
            if include_header:
                writer.writerow(self.columns)
            for row in self._snapshot.rows:
                writer.writerow(row)
        _write_csv_meta(target, delimiter, quoting)
        return target

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    def append_csv(
        self,
        path: str | Path,
        *,
        delimiter: str = ",",
        quoting: str = '"',
        dedupe: AppendDedupe | None = None,
        partition: Partition | None = None,
        rollover: Rollover | None = None,
    ) -> list[Path]:
        target = Path(path)
        dedupe = dedupe or AppendDedupe()
        partition = partition or Partition()
        rollover = rollover or Rollover()

        base_dir = target.parent if target.suffix else target
        file_name = target.name if target.suffix else "part.csv"

        rows_by_partition: dict[str | None, list[dict[str, object]]] = {}
        for row in self:
            key = partition.route(row)
            rows_by_partition.setdefault(key, []).append(row)

        written_paths: list[Path] = []
        for key, rows in rows_by_partition.items():
            destination = partition.ensure_directory(base_dir, key)
            initial_path = destination / file_name
            file_path = rollover.select_target(initial_path)
            existing_rows_for_dedupe = _read_existing_csv(initial_path, delimiter, quoting)
            existing_rows = _read_existing_csv(file_path, delimiter, quoting)
            if file_path.exists():
                if rollover.max_rows is not None and len(existing_rows) >= rollover.max_rows:
                    file_path = rollover.select_target(file_path, force_new=True)
                    existing_rows = _read_existing_csv(file_path, delimiter, quoting)
                if file_path != initial_path:
                    existing_rows_for_dedupe = _read_existing_csv(
                        initial_path, delimiter, quoting
                    )
                if (
                    rollover.max_size_mb is not None
                    and file_path.exists()
                    and file_path.stat().st_size >= rollover.max_size_mb * 1024 * 1024
                ):
                    file_path = rollover.select_target(file_path, force_new=True)
                    existing_rows = _read_existing_csv(file_path, delimiter, quoting)
                    if file_path != initial_path:
                        existing_rows_for_dedupe = _read_existing_csv(
                            initial_path, delimiter, quoting
                        )
            filtered_rows = dedupe.filter_rows(
                existing_rows_for_dedupe, rows, self.columns
            )
            if not filtered_rows:
                continue
            file_path.parent.mkdir(parents=True, exist_ok=True)
            mode = "a" if file_path.exists() else "w"
            meta = _read_csv_meta(file_path)
            if meta:
                stored_delim, stored_quoting = meta
                if stored_delim != delimiter or stored_quoting != quoting:
                    raise AppendError("CSV delimiter or quoting incompatible with existing file")
            with file_path.open(mode, newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, delimiter=delimiter, quotechar=quoting)
                if mode == "w":
                    writer.writerow(self.columns)
                elif mode == "a" and _read_header(file_path, delimiter, quoting) != list(
                    self.columns
                ):
                    raise AppendError("CSV header mismatch during append")
                for row in filtered_rows:
                    writer.writerow(tuple(row.get(col) for col in self.columns))
            _write_csv_meta(file_path, delimiter, quoting)
            written_paths.append(file_path)
        return written_paths

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    def simulate_append_csv(
        self,
        path: str | Path,
        *,
        delimiter: str = ",",
        quoting: str = '"',
        dedupe: AppendDedupe | None = None,
        partition: Partition | None = None,
        rollover: Rollover | None = None,
    ) -> AppendSimulation:
        target = Path(path)
        dedupe = dedupe or AppendDedupe()
        partition = partition or Partition()
        rollover = rollover or Rollover()

        base_dir = target.parent if target.suffix else target
        file_name = target.name if target.suffix else "part.csv"

        rows_by_partition: dict[str | None, list[dict[str, object]]] = {}
        for row in self:
            key = partition.route(row)
            rows_by_partition.setdefault(key, []).append(row)

        actions: list[AppendSimulationAction] = []
        for key, rows in rows_by_partition.items():
            destination = base_dir if key is None else base_dir / key
            initial_path = destination / file_name
            file_path = rollover.select_target(initial_path)
            rolled_over = False

            existing_rows_for_dedupe = _read_existing_csv(
                initial_path, delimiter, quoting
            )
            existing_rows = _read_existing_csv(file_path, delimiter, quoting)
            if file_path.exists():
                if (
                    rollover.max_rows is not None
                    and len(existing_rows) >= rollover.max_rows
                ):
                    file_path = rollover.select_target(file_path, force_new=True)
                    rolled_over = True
                    existing_rows = _read_existing_csv(file_path, delimiter, quoting)
                if (
                    rollover.max_size_mb is not None
                    and file_path.exists()
                    and file_path.stat().st_size >= rollover.max_size_mb * 1024 * 1024
                ):
                    file_path = rollover.select_target(file_path, force_new=True)
                    rolled_over = True
                    existing_rows = _read_existing_csv(file_path, delimiter, quoting)

            meta = _read_csv_meta(file_path)
            if meta:
                stored_delim, stored_quoting = meta
                if stored_delim != delimiter or stored_quoting != quoting:
                    raise AppendError(
                        "CSV delimiter or quoting incompatible with existing file"
                    )
            if file_path.exists() and _read_header(file_path, delimiter, quoting) != list(
                self.columns
            ):
                raise AppendError("CSV header mismatch during append")

            filtered_rows = dedupe.filter_rows(
                existing_rows_for_dedupe, rows, self.columns
            )
            if not filtered_rows:
                continue

            actions.append(
                AppendSimulationAction(
                    path=file_path,
                    partition_key=key,
                    existing_rows=len(existing_rows),
                    rows_to_append=len(filtered_rows),
                    will_create=not file_path.exists(),
                    dedupe_applied=dedupe.mode == "anti_join"
                    and len(filtered_rows) != len(rows),
                    rollover_applied=rolled_over,
                )
            )

        return AppendSimulation(
            backend="csv",
            actions=tuple(actions),
        )

    # -- JSON ----------------------------------------------------------------
    def write_json(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            json.dump(self._snapshot.to_dicts(), handle, ensure_ascii=False)
        return target

    def append_json(
        self,
        path: str | Path,
        *,
        dedupe: AppendDedupe | None = None,
    ) -> Path:
        target = Path(path)
        dedupe = dedupe or AppendDedupe()
        existing_records: Sequence[Mapping[str, object]] = []
        if target.exists():
            with target.open("r", encoding="utf-8") as handle:
                existing_raw = json.load(handle)
            existing_records = cast(
                Sequence[Mapping[str, object]], tuple(dict(row) for row in existing_raw)
            )
        filtered = dedupe.filter_rows(existing_records, list(self), self.columns)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            json.dump(
                [*list(existing_records), *[dict(row) for row in filtered]],
                handle,
                ensure_ascii=False,
            )
        return target

    # -- Parquet -------------------------------------------------------------
    def write_parquet(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        pa, pq = _import_pyarrow()
        table = _to_arrow_table(pa, self.columns, self._snapshot.rows)
        pq.write_table(table, target)
        _write_parquet_meta(target, len(self._snapshot.rows))
        return target

    # pylint: disable=too-many-arguments,too-many-locals
    def append_parquet(
        self,
        path: str | Path,
        *,
        dedupe: AppendDedupe | None = None,
        partition: Partition | None = None,
        rollover: Rollover | None = None,
        force_single_file: bool = False,
    ) -> list[Path]:
        dedupe = dedupe or AppendDedupe()
        partition = partition or Partition()
        rollover = rollover or Rollover()

        base = Path(path)
        if base.suffix and not force_single_file:
            raise AppendError(
                (
                    "Parquet append requires a directory target. "
                    "Set force_single_file=True to override."
                )
            )
        if base.suffix and force_single_file:
            warnings.warn(
                (
                    "Single-file Parquet append may rewrite files. "
                    "Consider using a directory target instead."
                ),
                stacklevel=2,
            )
        base.mkdir(parents=True, exist_ok=True)

        pa, pq = _import_pyarrow()

        written: list[Path] = []
        for key, rows in _partition_rows(self, partition).items():
            destination = partition.ensure_directory(base, key)
            target_file = rollover.select_target(destination / "part.parquet")
            existing_records: list[dict[str, object]] = []
            if target_file.exists():
                existing_table = pq.read_table(target_file)
                if (
                    rollover.max_rows is not None
                    and existing_table.num_rows >= rollover.max_rows
                ):
                    target_file = rollover.select_target(target_file, force_new=True)
                    existing_records = []
                elif (
                    rollover.max_size_mb is not None
                    and target_file.stat().st_size >= rollover.max_size_mb * 1024 * 1024
                ):
                    target_file = rollover.select_target(target_file, force_new=True)
                    existing_records = []
                else:
                    existing_records = existing_table.to_pylist()
            filtered_source = dedupe.filter_rows(
                existing_records,
                rows,
                self.columns,
            )
            filtered = [dict(row) for row in filtered_source]
            if not filtered:
                continue
            payload = [*existing_records, *filtered]
            table_fragment = pa.Table.from_pylist(payload)
            pq.write_table(table_fragment, target_file)
            _write_parquet_meta(target_file, len(payload))
            written.append(target_file)
        return written

    # pylint: disable=too-many-arguments,too-many-locals
    def simulate_append_parquet(
        self,
        path: str | Path,
        *,
        dedupe: AppendDedupe | None = None,
        partition: Partition | None = None,
        rollover: Rollover | None = None,
        force_single_file: bool = False,
    ) -> AppendSimulation:
        dedupe = dedupe or AppendDedupe()
        partition = partition or Partition()
        rollover = rollover or Rollover()

        base = Path(path)
        if base.suffix and not force_single_file:
            raise AppendError(
                (
                    "Parquet append requires a directory target. "
                    "Set force_single_file=True to override."
                )
            )

        warnings_out: list[str] = []
        if base.suffix and force_single_file:
            warnings_out.append(
                (
                    "Single-file Parquet append may rewrite files. "
                    "Consider using a directory target instead."
                )
            )

        actions: list[AppendSimulationAction] = []
        notes: set[str] = set()
        rows_by_partition = _partition_rows(self, partition)

        for key, rows in rows_by_partition.items():
            destination = base if key is None else base / key
            target_file = rollover.select_target(destination / "part.parquet")

            rolled_over = False
            existing_meta = _read_parquet_meta(target_file)
            existing_rows = existing_meta.get("row_count") if existing_meta else None

            if (
                rollover.max_rows is not None
                and existing_rows is not None
                and existing_rows >= rollover.max_rows
            ):
                target_file = rollover.select_target(target_file, force_new=True)
                rolled_over = True
                existing_rows = 0
            elif (
                rollover.max_size_mb is not None
                and target_file.exists()
                and target_file.stat().st_size >= rollover.max_size_mb * 1024 * 1024
            ):
                target_file = rollover.select_target(target_file, force_new=True)
                rolled_over = True
                existing_rows = 0

            if dedupe.mode == "anti_join":
                notes.add(
                    "Parquet dedupe simulation skips existing-row checks without pyarrow"
                )

            actions.append(
                AppendSimulationAction(
                    path=target_file,
                    partition_key=key,
                    existing_rows=existing_rows,
                    rows_to_append=len(rows),
                    will_create=not target_file.exists(),
                    dedupe_applied=False,
                    rollover_applied=rolled_over,
                )
            )

        return AppendSimulation(
            backend="parquet",
            actions=tuple(actions),
            warnings=tuple(warnings_out),
            notes=tuple(sorted(notes)),
        )


def _read_existing_csv(path: Path, delimiter: str, quoting: str) -> list[dict[str, object]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=delimiter, quotechar=quoting)
        try:
            header = next(reader)
        except StopIteration:
            return []
        rows: list[dict[str, object]] = []
        for row in reader:
            rows.append(dict(zip(header, row)))
    return rows


def _read_header(path: Path, delimiter: str, quoting: str) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=delimiter, quotechar=quoting)
        try:
            return next(reader)
        except StopIteration:
            return []


def _csv_meta_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".duckplus-meta")


def _write_csv_meta(path: Path, delimiter: str, quoting: str) -> None:
    meta_path = _csv_meta_path(path)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump({"delimiter": delimiter, "quoting": quoting}, handle)


def _read_csv_meta(path: Path) -> tuple[str, str] | None:
    meta_path = _csv_meta_path(path)
    if not meta_path.exists():
        return None
    with meta_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload["delimiter"], payload["quoting"]


def _partition_rows(
    relation: Relation,
    partition: Partition,
) -> dict[str | None, list[dict[str, object]]]:
    buckets: dict[str | None, list[dict[str, object]]] = {}
    for row in relation:
        key = partition.route(row) if partition else None
        buckets.setdefault(key, []).append(row)
    return buckets


def _parquet_meta_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".duckplus-meta")


def _read_parquet_meta(path: Path) -> dict[str, int] | None:
    meta_path = _parquet_meta_path(path)
    if not meta_path.exists():
        return None
    with meta_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload


def _write_parquet_meta(path: Path, row_count: int) -> None:
    meta_path = _parquet_meta_path(path)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump({"row_count": row_count}, handle)


def _to_arrow_table(
    pa_module: Any, columns: Sequence[str], rows: Sequence[Sequence[object]]
):
    arrays = {
        column: pa_module.array([row[idx] for row in rows])
        for idx, column in enumerate(columns)
    }
    return pa_module.table(arrays)


def _estimate_row_size(columns: Sequence[str], row: Sequence[object]) -> int:
    serialized = json.dumps(dict(zip(columns, row)), ensure_ascii=False).encode("utf-8")
    return len(serialized)


def _import_pyarrow():
    """Return the optional ``pyarrow`` modules or raise ``AppendError``."""

    if importlib.util.find_spec("pyarrow") is None:
        raise AppendError("pyarrow is required for parquet support")
    if importlib.util.find_spec("pyarrow.parquet") is None:
        raise AppendError("pyarrow is required for parquet support")
    pa = cast(Any, import_module("pyarrow"))
    pq = cast(Any, import_module("pyarrow.parquet"))
    return pa, pq
