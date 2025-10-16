from duckplus.duckcon import DuckCon
from duckplus.extensions import audit_bundled_extensions


EXPECTED_BUNDLED = {
    "autocomplete",
    "encodings",
    "fts",
    "httpfs",
    "inet",
    "spatial",
    "tpcds",
    "tpch",
    "ui",
}


def test_audit_filters_bundled_extensions() -> None:
    manager = DuckCon()
    with manager:
        infos = manager.extensions()

    audit = audit_bundled_extensions(infos)
    names = {entry.info.name for entry in audit}
    assert names == EXPECTED_BUNDLED
    assert all(not entry.helper_names for entry in audit)


def test_audit_includes_helper_metadata() -> None:
    manager = DuckCon()
    with manager:
        infos = manager.extensions()

    audit = audit_bundled_extensions(
        infos,
        helper_coverage={"httpfs": ("duckplus.io.read_csv",)},
    )

    helper_map = {entry.info.name: entry.helper_names for entry in audit}
    assert helper_map["httpfs"] == ("duckplus.io.read_csv",)
