"""Secret management helpers for Duck+."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import duckdb

from . import util
from .connect import DuckConnection


def _quote(value: str) -> str:
    """Return a SQL-quoted representation of *value*."""

    escaped = value.replace("'", "''")
    return f"'{escaped}'"


@dataclass(frozen=True, slots=True)
class SecretDefinition:
    """Definition for a DuckDB secret."""

    name: str
    engine: str
    parameters: Mapping[str, str]

    def normalized(self) -> "SecretRecord":
        """Return a normalized, validated representation of the secret."""

        normalized_name = util.ensure_identifier(self.name, allow_quoted=True)
        normalized_engine = util.ensure_identifier(self.engine, allow_quoted=True)
        normalized_parameters: list[tuple[str, str]] = []
        for key, value in self.parameters.items():
            normalized_key = util.ensure_identifier(key, allow_quoted=True)
            normalized_parameters.append((normalized_key, str(value)))
        return SecretRecord(
            name=normalized_name,
            engine=normalized_engine,
            parameters=tuple(normalized_parameters),
        )


@dataclass(frozen=True, slots=True)
class SecretRecord:
    """Concrete record for a stored secret."""

    name: str
    engine: str
    parameters: tuple[tuple[str, str], ...]

    def to_definition(self) -> SecretDefinition:
        """Convert the record back into a :class:`SecretDefinition`."""

        return SecretDefinition(
            name=self.name,
            engine=self.engine,
            parameters=dict(self.parameters),
        )


class SecretRegistry:
    """Connection-independent registry for DuckDB secrets."""

    def __init__(self) -> None:
        self._store: dict[str, SecretRecord] = {}

    def has_secret(self, name: str) -> bool:
        """Return ``True`` if a secret named *name* exists."""

        normalized = util.ensure_identifier(name, allow_quoted=True)
        return normalized in self._store

    def list_secrets(self) -> list[SecretRecord]:
        """Return all stored secrets."""

        return list(self._store.values())

    def get_secret(self, name: str) -> SecretRecord | None:
        """Return the secret named *name* when it exists."""

        normalized = util.ensure_identifier(name, allow_quoted=True)
        return self._store.get(normalized)

    def save(self, record: SecretRecord, *, replace: bool = False) -> bool:
        """Save *record* and report whether it replaced an existing secret."""

        existing = self._store.get(record.name)
        if existing is not None and not replace:
            raise ValueError(f"Secret already exists: {record.name}")
        self._store[record.name] = record
        return existing is not None

    def drop_secret(self, name: str) -> SecretRecord:
        """Remove *name* from the registry and return the stored record."""

        normalized = util.ensure_identifier(name, allow_quoted=True)
        try:
            return self._store.pop(normalized)
        except KeyError as exc:  # pragma: no cover - defensive, error rewording only
            raise KeyError(f"Secret not found: {normalized}") from exc


_GLOBAL_SECRET_REGISTRY = SecretRegistry()


class SecretManager:
    """Manage DuckDB secrets with graceful fallback when the extension is missing."""

    def __init__(
        self,
        connection: DuckConnection,
        *,
        registry: SecretRegistry | None = None,
        auto_load: bool = True,
    ) -> None:
        self._connection = connection
        self._registry = registry or _GLOBAL_SECRET_REGISTRY
        self._extension_state: str = "unknown"
        if auto_load:
            self.ensure_extension()

    @property
    def connection(self) -> DuckConnection:
        """Return the wrapped Duck+ connection."""

        return self._connection

    @property
    def registry(self) -> SecretRegistry:
        """Return the connection-independent registry backing the manager."""

        return self._registry

    def ensure_extension(self) -> bool:
        """Attempt to load the DuckDB ``secrets`` extension.

        Returns ``True`` when the extension is available. Errors encountered while
        loading the extension are swallowed, allowing callers to rely on the
        in-memory fallback store.
        """

        if self._extension_state == "available":
            return True
        if self._extension_state == "missing":
            return False

        raw = self._connection.raw
        try:
            raw.execute("LOAD secrets")
        except duckdb.Error:  # pragma: no cover - executed only when DuckDB raises
            self._extension_state = "missing"
            return False
        else:
            self._extension_state = "available"
            return True

    def has_secret(self, name: str) -> bool:
        """Return ``True`` when a secret with *name* exists."""

        return self._registry.has_secret(name)

    def list_secrets(self) -> list[SecretRecord]:
        """Return the stored secrets as :class:`SecretRecord` instances."""

        return self._registry.list_secrets()

    def get_secret(self, name: str) -> SecretRecord | None:
        """Retrieve *name* as a :class:`SecretRecord` when present."""

        return self._registry.get_secret(name)

    def create_secret(self, definition: SecretDefinition, *, replace: bool = False) -> SecretRecord:
        """Create a secret and return the stored :class:`SecretRecord`.

        When the DuckDB ``secrets`` extension is available the definition is
        mirrored into the underlying connection. Otherwise the secret is retained
        solely within Duck+ so higher-level connection helpers can reference it
        later.
        """

        record = definition.normalized()
        replaced = self._registry.save(record, replace=replace)

        if self.ensure_extension():
            if replaced:
                self._connection.raw.execute(f"DROP SECRET IF EXISTS {record.name}")
            self._create_in_duckdb(record)

        return record

    def drop_secret(self, name: str) -> None:
        """Remove the secret named *name* from Duck+ (and DuckDB when available)."""

        record = self._registry.drop_secret(name)

        if self._extension_state == "available":
            self._connection.raw.execute(f"DROP SECRET IF EXISTS {record.name}")

    def sync(self, names: Sequence[str] | None = None) -> None:
        """Synchronize stored secrets into DuckDB when the extension loads later."""

        if not self.ensure_extension():
            return

        if names is None:
            records = self._registry.list_secrets()
        else:
            records = []
            for name in names:
                record = self._registry.get_secret(name)
                if record is None:
                    normalized = util.ensure_identifier(name, allow_quoted=True)
                    raise KeyError(f"Secret not found: {normalized}")
                records.append(record)

        for record in records:
            self._create_in_duckdb(record, replace=True)

    def _create_in_duckdb(self, record: SecretRecord, *, replace: bool = False) -> None:
        raw = self._connection.raw
        if replace:
            raw.execute(f"DROP SECRET IF EXISTS {record.name}")

        assignments = [f"TYPE {record.engine}"]
        for key, value in record.parameters:
            assignments.append(f"{key} {_quote(value)}")
        clause = ", ".join(assignments)
        raw.execute(f"CREATE SECRET {record.name} ({clause})")
