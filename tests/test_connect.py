from __future__ import annotations

import duckplus


def test_connect_executes_simple_query() -> None:
    with duckplus.connect() as conn:
        result = conn.raw.execute("SELECT 42").fetchone()

    assert result == (42,)
