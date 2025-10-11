from __future__ import annotations

import io
from pathlib import Path

import pytest

from duckplus import cli, connect


def test_main_sql_outputs_relation(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli.main(["sql", "SELECT 1 AS id, 'alpha' AS label"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "id" in captured.out
    assert "alpha" in captured.out
    assert captured.err == ""


def test_main_sql_respects_limit(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli.main([
        "--display-limit",
        "1",
        "sql",
        "SELECT * FROM (VALUES (1), (2)) AS t(id)",
    ])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "1" in captured.out
    assert "2" not in captured.out
    assert "additional rows not shown" in captured.out


def test_main_sql_reports_duckdb_errors(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli.main(["sql", "SELECT * FROM missing_table"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "DuckDB failed to execute SQL" in captured.err


def test_schema_describes_existing_table(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    with connect(database=db_path, read_only=False) as conn:
        conn.raw.execute("CREATE TABLE metrics(metric_id INTEGER, amount DOUBLE)")

    exit_code = cli.main(["--database", str(db_path), "schema", "metrics"])
    assert exit_code == 0
    captured = capsys.readouterr()
    assert "metric_id" in captured.out
    assert "amount" in captured.out


def test_schema_reports_missing_relation(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = tmp_path / "warehouse.duckdb"
    with connect(database=db_path, read_only=False):
        pass
    exit_code = cli.main(["--database", str(db_path), "schema", "missing"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "No schema information" in captured.err


def test_repl_executes_queries() -> None:
    with connect() as conn:
        input_stream = io.StringIO("SELECT 42 AS value\nexit\n")
        output_stream = io.StringIO()
        cli.repl(conn, stdin=input_stream, stdout=output_stream, limit=5)

    output = output_stream.getvalue()
    assert "value" in output
    assert "42" in output


def test_repl_reports_errors() -> None:
    with connect() as conn:
        input_stream = io.StringIO("SELECT * FROM missing\nquit\n")
        output_stream = io.StringIO()
        cli.repl(conn, stdin=input_stream, stdout=output_stream, limit=5)

    output = output_stream.getvalue()
    assert "error:" in output


def test_main_rejects_negative_display_limit(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli.main(["--display-limit", "-1", "--repl"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "display-limit" in captured.err


def test_main_reports_connection_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    directory = tmp_path / "data_dir"
    directory.mkdir()

    exit_code = cli.main(["--database", str(directory), "sql", "SELECT 1"])
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "failed to open" in captured.err.lower()


def test_repl_rejects_negative_limit() -> None:
    with connect() as conn:
        with pytest.raises(cli.CLIError):
            cli.repl(conn, limit=-1)
