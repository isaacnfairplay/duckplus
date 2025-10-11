from __future__ import annotations

from duckplus import DuckRel, connect, to_html


def test_to_html_renders_headers_and_rows() -> None:
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS id, 'Alice' AS name"))
        html = to_html(rel)
    assert "<th scope=\"col\">id</th>" in html
    assert "<td>1</td>" in html
    assert "Alice" in html


def test_to_html_escapes_cells() -> None:
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT '<tag>' AS raw"))
        html = to_html(rel)
    assert "&lt;tag&gt;" in html


def test_to_html_truncates_rows() -> None:
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT * FROM (VALUES (1), (2)) AS t(id)"))
        html = to_html(rel, max_rows=1)
    assert "Additional rows not shown" in html


def test_to_html_supports_attributes() -> None:
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS value"))
        html = to_html(rel, table_class="preview", table_id="table-1")
    assert '<table class="preview" id="table-1">' in html


def test_to_html_rejects_invalid_limit() -> None:
    with connect() as conn:
        rel = DuckRel(conn.raw.sql("SELECT 1 AS value"))
    try:
        to_html(rel, max_rows=0)
    except ValueError as exc:  # pragma: no cover - exercised by assertion
        assert "max_rows" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError")
