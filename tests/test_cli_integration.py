from __future__ import annotations

import io
from pathlib import Path

import pytest

from duckplus import DuckRel, cli, connect, to_html


pytestmark = pytest.mark.mutable_with_approval


def test_exploratory_cli_and_html_flow(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Exploratory scenario exercising CLI previews and HTML rendering."""

    db_path = tmp_path / "analytics.duckdb"
    with connect(database=db_path, read_only=False) as conn:
        conn.raw.execute(
            """
            CREATE TABLE metrics(
                metric_id INTEGER,
                label VARCHAR,
                value DOUBLE
            )
            """
        )
        conn.raw.execute(
            """
            INSERT INTO metrics VALUES
                (1, 'Alpha', 10.0),
                (2, 'Beta', 20.5),
                (3, 'Gamma', 30.75)
            """
        )

    exit_code = cli.main(["--database", str(db_path), "schema", "metrics"])
    assert exit_code == 0
    schema_output = capsys.readouterr().out
    assert "metric_id" in schema_output
    assert "value" in schema_output

    exit_code = cli.main([
        "--database",
        str(db_path),
        "--display-limit",
        "2",
        "sql",
        "SELECT metric_id, label, value FROM metrics ORDER BY metric_id",
    ])
    assert exit_code == 0
    sql_output = capsys.readouterr().out
    assert "Alpha" in sql_output
    assert "Gamma" not in sql_output
    assert "additional rows not shown" in sql_output

    with connect(database=db_path, read_only=True) as conn:
        input_stream = io.StringIO(
            "SELECT AVG(value) AS average_value FROM metrics\n" "exit\n"
        )
        output_stream = io.StringIO()
        cli.repl(conn, stdin=input_stream, stdout=output_stream, limit=5)
    repl_output = output_stream.getvalue()
    assert "average_value" in repl_output
    assert "20.416" in repl_output or "20.416666" in repl_output

    with connect(database=db_path, read_only=True) as conn:
        rel = DuckRel(
            conn.raw.sql(
                "SELECT metric_id, label, value FROM metrics ORDER BY metric_id"
            )
        )
        html_output = to_html(rel, max_rows=2, table_class="preview")
    assert "<table class=\"preview\"" in html_output
    assert "Additional rows not shown" in html_output
