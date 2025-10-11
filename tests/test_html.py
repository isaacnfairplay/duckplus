from __future__ import annotations

import duckdb
import pytest

from duckplus.core import DuckRel
from duckplus.html import to_html


@pytest.fixture()
def connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def sample_rel(connection: duckdb.DuckDBPyConnection) -> DuckRel:
    relation = connection.sql(
        """
        SELECT *
        FROM (VALUES
            (1, 'Alpha & <Beta>', 'note'),
            (2, '"Gamma"', NULL)
        ) AS t(id, label, details)
        ORDER BY id
        """
    )
    return DuckRel(relation)


def test_to_html_renders_table_with_escaped_values(sample_rel: DuckRel) -> None:
    html = to_html(sample_rel)

    assert html.startswith("<table")
    assert "<thead><tr><th>id</th><th>label</th><th>details</th></tr></thead>" in html
    assert "Alpha &amp; &lt;Beta&gt;" in html
    assert "&quot;Gamma&quot;" in html
    assert "<td></td>" in html  # NULL defaults to blank


def test_to_html_supports_custom_null_display(sample_rel: DuckRel) -> None:
    html = to_html(sample_rel, null_display="(null)")

    assert "<td>(null)</td>" in html


def test_to_html_respects_row_limit_and_footer(connection: duckdb.DuckDBPyConnection) -> None:
    relation = connection.sql(
        """
        SELECT *
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)
        ORDER BY id
        """
    )
    rel = DuckRel(relation)

    html = to_html(rel, max_rows=3)
    body = html.split("<tbody>")[1].split("</tbody>")[0]

    assert body.count("<tr>") == 3
    assert "<td>4</td>" not in body
    assert (
        "<tfoot><tr><td colspan=\"1\">Showing first 3 of 5 rows (2 more rows not shown).</td></tr></tfoot>"
        in html
    )


def test_to_html_applies_supported_attributes(sample_rel: DuckRel) -> None:
    html = to_html(sample_rel, class_="preview", id="table-1")

    assert html.startswith('<table class="preview" id="table-1">')


def test_to_html_rejects_negative_limits(sample_rel: DuckRel) -> None:
    with pytest.raises(ValueError):
        to_html(sample_rel, max_rows=-1)
