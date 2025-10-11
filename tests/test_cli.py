from __future__ import annotations

import io
import sys

import duckplus
from duckplus import cli as cli_module
from duckplus import cli_main


def test_sql_command_outputs_table(capsys) -> None:
    code = cli_module.main(["sql", "SELECT 1 AS value"])

    captured = capsys.readouterr()
    assert code == 0
    assert "value" in captured.out
    assert "(1 row)" in captured.out


def test_sql_truncation_message(capsys) -> None:
    code = cli_module.main(
        [
            "sql",
            "SELECT * FROM (VALUES (1), (2), (3)) AS t(id)",
            "--limit",
            "2",
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert "... more rows available (showing first 2)" in captured.out


def test_schema_command_lists_columns(capsys) -> None:
    code = cli_module.main(["schema", "SELECT 1 AS id, 'alpha' AS label"])

    captured = capsys.readouterr()
    assert code == 0
    assert "column" in captured.out
    assert "type" in captured.out
    assert "id" in captured.out
    assert "label" in captured.out


def test_repl_exits_on_command(monkeypatch) -> None:
    stdin = io.StringIO("SELECT 1 AS value\n.exit\n")
    stdout = io.StringIO()
    stderr = io.StringIO()

    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)

    with duckplus.connect() as conn:
        cli_module.repl(conn)

    assert "duckplus>" in stdout.getvalue()
    assert "(1 row)" in stdout.getvalue()
    assert stderr.getvalue() == ""


def test_main_runs_repl_without_command(monkeypatch) -> None:
    stdin = io.StringIO(".quit\n")
    stdout = io.StringIO()
    stderr = io.StringIO()

    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)

    code = cli_main(["--repl"])

    assert code == 0
    assert "duckplus>" in stdout.getvalue()
    assert stderr.getvalue() == ""
