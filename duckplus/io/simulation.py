"""Append simulation dataclasses."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple


@dataclass(frozen=True)
class AppendSimulationAction:
    """Single planned append operation."""

    path: Path
    partition_key: str | None
    existing_rows: int | None
    rows_to_append: int
    will_create: bool
    dedupe_applied: bool
    rollover_applied: bool


@dataclass(frozen=True)
class AppendSimulation:
    """Collection of append actions for a backend."""

    backend: str
    actions: Tuple[AppendSimulationAction, ...]
    warnings: Tuple[str, ...] = ()
    notes: Tuple[str, ...] = ()

    @property
    def total_rows(self) -> int:
        return sum(action.rows_to_append for action in self.actions)


__all__ = ["AppendSimulation", "AppendSimulationAction"]
