"""Tests for Duck+ secret helpers."""

from __future__ import annotations

import pytest

from duckplus import SecretDefinition, SecretManager, SecretRegistry, connect


@pytest.fixture()
def registry() -> SecretRegistry:
    return SecretRegistry()


def test_secret_definition_validation() -> None:
    secret = SecretDefinition(
        name="storage_creds",
        engine="S3",
        parameters={"KEY_ID": "abc", "SECRET": "xyz"},
    )
    record = secret.normalized()
    assert record.name == "storage_creds"
    assert record.engine == "S3"
    assert record.parameters == (("KEY_ID", "abc"), ("SECRET", "xyz"))

    with pytest.raises(ValueError):
        SecretDefinition(name="not valid", engine="S3", parameters={}).normalized()

    with pytest.raises(TypeError):
        SecretDefinition(name="ok", engine="S3", parameters={1: "value"}).normalized()  # type: ignore[arg-type]


def test_secret_manager_fallback_roundtrip(registry: SecretRegistry) -> None:
    with connect() as conn:
        manager = SecretManager(conn, registry=registry, auto_load=False)
        assert manager.ensure_extension() is False

        record = manager.create_secret(
            SecretDefinition(
                name="app_secret",
                engine="S3",
                parameters={"KEY_ID": "k", "SECRET": "s"},
            )
        )
        assert manager.has_secret("app_secret")
        assert manager.get_secret("app_secret") == record
        assert manager.list_secrets() == [record]

        with pytest.raises(ValueError):
            manager.create_secret(
                SecretDefinition(
                    name="app_secret",
                    engine="S3",
                    parameters={"KEY_ID": "other", "SECRET": "other"},
                )
            )

        updated = manager.create_secret(
            SecretDefinition(
                name="app_secret",
                engine="S3",
                parameters={"KEY_ID": "other", "SECRET": "other"},
            ),
            replace=True,
        )
        assert updated.parameters == (("KEY_ID", "other"), ("SECRET", "other"))

    with connect() as other_conn:
        other_manager = SecretManager(other_conn, registry=registry, auto_load=False)
        assert other_manager.has_secret("app_secret")
        assert other_manager.get_secret("app_secret") == updated

        other_manager.drop_secret("app_secret")

    with pytest.raises(KeyError):
        registry.drop_secret("app_secret")


def test_drop_missing_secret_errors(registry: SecretRegistry) -> None:
    with connect() as conn:
        manager = SecretManager(conn, registry=registry, auto_load=False)
        with pytest.raises(KeyError):
            manager.drop_secret("missing")


def test_sync_replays_to_duckdb(
    registry: SecretRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    with connect() as conn:
        manager = SecretManager(conn, registry=registry, auto_load=False)
        manager.create_secret(
            SecretDefinition(
                name="sync_me",
                engine="S3",
                parameters={"KEY_ID": "k", "SECRET": "s"},
            )
        )

        recorded: list[tuple[str, tuple[tuple[str, str], ...], bool]] = []

        def fake_create(record, *, replace: bool = False) -> None:
            recorded.append((record.name, record.parameters, replace))

        monkeypatch.setattr(manager, "_create_in_duckdb", fake_create)
        monkeypatch.setattr(manager, "ensure_extension", lambda: True)

        manager.sync()
        assert recorded == [("sync_me", (("KEY_ID", "k"), ("SECRET", "s")), True)]

        manager.sync(names=["sync_me"])
        assert recorded[-1] == ("sync_me", (("KEY_ID", "k"), ("SECRET", "s")), True)

        with pytest.raises(KeyError):
            manager.sync(names=["missing"]) 
