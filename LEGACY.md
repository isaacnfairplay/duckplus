from __future__ import annotations

import time
from pathlib import Path
from typing import List, Optional, Protocol, Callable

import duckdb
from datetime import date, datetime

from tooling.api.boilerplate import CACHE_FOLDER, from_odbc_s21, warehouse_source
from tooling import Duck
from logger_factory import setup_logger

logger = setup_logger(level="INFO", add_console=True)

# =============================================================================
# Small helpers
# =============================================================================

def _list_cache_files(folder: Path) -> list[Path]:
    return [p for p in folder.iterdir() if p.is_file() and p.suffix in (".parquet", ".empty")]

def _latest_entry(folder: Path) -> Optional[Path]:
    files = _list_cache_files(folder)
    return max(files, key=lambda p: p.stat().st_mtime) if files else None

def _cleanup_keep_only(folder: Path, keep_stem: str) -> int:
    deleted = 0
    for p in _list_cache_files(folder):
        if p.stem != keep_stem:
            try:
                p.unlink()
                deleted += 1
            except FileNotFoundError:
                pass
    return deleted

def _is_fresh(path: Path, ttl_seconds: int) -> bool:
    try:
        return (time.time() - path.stat().st_mtime) <= ttl_seconds
    except FileNotFoundError:
        return False

def _today_stamp() -> str:
    return datetime.now().strftime("%Y%m%d")

def _sql_count(con: duckdb.DuckDBPyConnection, rel: Duck, view_name: str) -> int:
    rel.rel.create_view(view_name)
    try:
        return int(con.sql(f"SELECT count(*)::BIGINT FROM {view_name}").fetchone()[0])
    finally:
        con.sql(f"DROP VIEW {view_name}")

def _copy_parquet_via_sql(con: duckdb.DuckDBPyConnection, rel: Duck, out_path: Path, view_name: str) -> None:
    rel.rel.create_view(view_name)
    try:
        con.sql(f"COPY (SELECT * FROM {view_name}) TO '{out_path.as_posix()}' (FORMAT PARQUET)")
    finally:
        con.sql(f"DROP VIEW {view_name}")

# =============================================================================
# Casting helpers (avoid Decimal/Union surprises from parquet/DB2)
# =============================================================================

def _cast_top_schema(rel: Duck) -> Duck:
    return (
        rel.transform_columns(
            parent_item="CAST(parent_item AS VARCHAR)",
            key_parent_item="CAST(key_parent_item AS VARCHAR)",
            mid_level_parent="CAST(mid_level_parent AS VARCHAR)",
            key_mid_level_parent="CAST(key_mid_level_parent AS VARCHAR)",
            manufacturing_route_code="CAST(manufacturing_route_code AS VARCHAR)",
            OverallOperationCodeTop="try_cast(OverallOperationCodeTop AS INTEGER)",
            OperationCodeTop="try_cast(OperationCodeTop AS INTEGER)",
            start_e_top="CAST(start_e_top AS DATE)",
            end_e_top="CAST(end_e_top AS DATE)",
        ).materialize()
    )

def _cast_bottom_schema(rel: Duck) -> Duck:
    return (
        rel.transform_columns(
            mid_level_parent_full="CAST(mid_level_parent_full AS VARCHAR)",
            key_mid_level_parent_full="CAST(key_mid_level_parent_full AS VARCHAR)",
            key_mid_level_parent="CAST(key_mid_level_parent AS VARCHAR)",
            component_item="CAST(component_item AS VARCHAR)",
            key_component_item="CAST(key_component_item AS VARCHAR)",
            manufacturing_route_code="CAST(manufacturing_route_code AS VARCHAR)",
            OverallOperatonCodeMid="try_cast(OverallOperatonCodeMid AS INTEGER)",
            OperationCodeMid="try_cast(OperationCodeMid AS INTEGER)",
            start_e_mid="CAST(start_e_mid AS DATE)",
            end_e_mid="CAST(end_e_mid AS DATE)",
        ).materialize()
    )

# =============================================================================
# Strategy Pattern
# =============================================================================

class MSP28Strategy(Protocol):
    """A source strategy that can produce TOP and BOTTOM relations (already trimmed/cast)."""
    def materialize_top(self, con: duckdb.DuckDBPyConnection, parent_item_key: str) -> Duck: ...
    def materialize_bottom(self, con: duckdb.DuckDBPyConnection, mid_keys: list[str]) -> Duck: ...

# --- Warehouse (Parquet) Strategy (DEFAULT) ----------------------------------

class WarehouseMSP28Strategy:
    """
    Pulls from warehouse_source('system_twenty_one_MSP28') with the known columns:

    EFFECTIVITY_FROM_DATE, EFFECTIVE_TO_DATE, PARENT_ITEM, MANUFACTURING_ROUTE_CODE,
    OPERATION_SEQUENCE, COMPONENT_SEQUENCE, COMPONENT_ITEM, ISSUING_STOCKROOM,
    QUANTITY_PER, SHRINKAGE_PERCENT, CUMULATIVE_SHRINKAGE_FACTOR,
    PRODUCTION_UNIT_OF_MEASURE, KEY_ITEM_POLICY, YYYYMMFDAT28, filename
    """

    def _base(self, con: duckdb.DuckDBPyConnection) -> Duck:
        # Select only what we need, then rename to canonical lower-case names
        base = (
            warehouse_source(con, "system_twenty_one_MSP28", union_by_name=True)
            .select_columns(
                "EFFECTIVITY_FROM_DATE",
                "EFFECTIVE_TO_DATE",
                "PARENT_ITEM",
                "MANUFACTURING_ROUTE_CODE",
                "OPERATION_SEQUENCE",
                "COMPONENT_SEQUENCE",
                "COMPONENT_ITEM",
            )
            .rename(
                effectivity_from_date="EFFECTIVITY_FROM_DATE",
                effective_to_date="EFFECTIVE_TO_DATE",
                parent_item="PARENT_ITEM",
                manufacturing_route_code="MANUFACTURING_ROUTE_CODE",
                operation_sequence="OPERATION_SEQUENCE",
                component_sequence="COMPONENT_SEQUENCE",
                component_item="COMPONENT_ITEM",
            )
            .transform_columns(
                parent_item="trim({col})",
                component_item="trim({col})",
                manufacturing_route_code="trim({col})",
                operation_sequence="try_cast({col} AS INTEGER)",
                component_sequence="try_cast({col} AS INTEGER)",
                effectivity_from_date="CAST({col} AS DATE)",
                effective_to_date="CAST({col} AS DATE)",
            )
            .add_columns(
                key_parent_item="replace(parent_item, ' ', '')",
                key_component_item="replace(component_item, ' ', '')",
            )
        )
        return base

    def materialize_top(self, con: duckdb.DuckDBPyConnection, parent_item_key: str) -> Duck:
        base = self._base(con)
        # Filter strictly by spaceless key, then shape to TOP schema
        top = (
            base
            .filter(f"key_parent_item = '{parent_item_key}'")
            .rename(
                mid_level_parent="component_item",
                key_mid_level_parent="key_component_item",
                start_e_top="effectivity_from_date",
                end_e_top="effective_to_date",
                OperationCodeTop="component_sequence",
                OverallOperationCodeTop="operation_sequence",
            )
            .select_columns(
                "parent_item",
                "key_parent_item",
                "mid_level_parent",
                "key_mid_level_parent",
                "manufacturing_route_code",
                "OverallOperationCodeTop",
                "OperationCodeTop",
                "start_e_top",
                "end_e_top",
            )
            .distinct()
            .materialize()
        )
        return _cast_top_schema(top)

    def materialize_bottom(self, con: duckdb.DuckDBPyConnection, mid_keys: list[str]) -> Duck:
        if not mid_keys:
            return Duck(con, con.sql("SELECT * FROM (SELECT 1) WHERE 1=0")).materialize()

        base = self._base(con)
        in_list = ",".join(f"'{k}'" for k in mid_keys)

        bottom = (
            base
            .filter(f"key_parent_item IN ({in_list})")
            .add_columns(key_mid_level_parent_full="key_parent_item")
            .rename(
                mid_level_parent_full="parent_item",
                key_mid_level_parent="key_parent_item",
                start_e_mid="effectivity_from_date",
                end_e_mid="effective_to_date",
                OperationCodeMid="component_sequence",
                OverallOperatonCodeMid="operation_sequence",
            )
            .select_columns(
                "mid_level_parent_full",
                "key_mid_level_parent_full",
                "key_mid_level_parent",
                "component_item",
                "key_component_item",
                "manufacturing_route_code",
                "OverallOperatonCodeMid",
                "OperationCodeMid",
                "start_e_mid",
                "end_e_mid",
            )
            .distinct()
            .materialize()
        )
        return _cast_bottom_schema(bottom)

# --- DB2 (Direct) Strategy (kept around; not default) ------------------------

class DB2MSP28Strategy:
    """Direct System21 strategy kept for optional use; identical shaping/casting to Warehouse."""

    _DATE_FROM = """
    CASE
      WHEN effectivity_from_raw IN (0, 9999999) THEN NULL
      WHEN (effectivity_from_raw / 10000) BETWEEN 50 AND 99 THEN NULL
      ELSE make_date(
             CAST(CASE
                  WHEN (effectivity_from_raw / 10000) <= 51
                  THEN 2000 + (effectivity_from_raw / 10000)
                  ELSE 2000 + ((effectivity_from_raw / 10000) - 100)
                 END AS BIGINT),
             CAST(((effectivity_from_raw % 10000) / 100) AS BIGINT),
             CAST((effectivity_from_raw % 100) AS BIGINT)
           )
    END
    """
    _DATE_TO = _DATE_FROM.replace("effectivity_from_raw", "effectivity_to_raw")

    @staticmethod
    def _build_top_query(parent_key: str) -> str:
        return f"""
            SELECT
                TRIM(PRNT28)                 AS parent_item,
                TRIM(COMP28)                 AS component_item,
                CAST(RTCD28 AS VARCHAR(32))  AS manufacturing_route_code,
                CAST(OPSQ28 AS INTEGER)      AS operation_sequence,
                CAST(CSEQ28 AS INTEGER)      AS component_sequence,
                FDAT28                       AS effectivity_from_raw,
                TDAT28                       AS effectivity_to_raw
            FROM AULT3F31GS.MSP28
            WHERE CONO28 = '01'
              AND REPLACE(TRIM(PRNT28), ' ', '') = '{parent_key}'
        """

    @staticmethod
    def _build_bottom_query(mid_keys: list[str]) -> str:
        if not mid_keys:
            return """
                SELECT
                    TRIM(PRNT28) AS parent_item,
                    TRIM(COMP28) AS component_item,
                    CAST(RTCD28 AS VARCHAR(32))  AS manufacturing_route_code,
                    CAST(OPSQ28 AS INTEGER)      AS operation_sequence,
                    CAST(CSEQ28 AS INTEGER)      AS component_sequence,
                    FDAT28 AS effectivity_from_raw,
                    TDAT28 AS effectivity_to_raw
                FROM AULT3F31GS.MSP28
                WHERE 1=0
            """
        in_list = ",".join(f"'{k}'" for k in mid_keys)
        return f"""
            SELECT
                TRIM(PRNT28) AS parent_item,
                TRIM(COMP28) AS component_item,
                CAST(RTCD28 AS VARCHAR(32))  AS manufacturing_route_code,
                CAST(OPSQ28 AS INTEGER)      AS operation_sequence,
                CAST(CSEQ28 AS INTEGER)      AS component_sequence,
                FDAT28 AS effectivity_from_raw,
                TDAT28 AS effectivity_to_raw
            FROM AULT3F31GS.MSP28
            WHERE CONO28 = '01'
              AND REPLACE(TRIM(PRNT28), ' ', '') IN ({in_list})
        """

    def materialize_top(self, con: duckdb.DuckDBPyConnection, parent_item_key: str) -> Duck:
        src = from_odbc_s21(con=con, query=self._build_top_query(parent_item_key))
        top = (
            src
            .add_columns(
                key_parent_item="replace(parent_item, ' ', '')",
                key_component_item="replace(component_item, ' ', '')",
                effectivity_from_date=self._DATE_FROM,
                effectivity_to_date=self._DATE_TO,
            )
            .rename(
                mid_level_parent="component_item",
                key_mid_level_parent="key_component_item",
                start_e_top="effectivity_from_date",
                end_e_top="effective_to_date",
                OperationCodeTop="component_sequence",
                OverallOperationCodeTop="operation_sequence",
            )
            .select_columns(
                "parent_item",
                "key_parent_item",
                "mid_level_parent",
                "key_mid_level_parent",
                "manufacturing_route_code",
                "OverallOperationCodeTop",
                "OperationCodeTop",
                "start_e_top",
                "end_e_top",
            )
            .distinct()
            .materialize()
        )
        return _cast_top_schema(top)

    def materialize_bottom(self, con: duckdb.DuckDBPyConnection, mid_keys: list[str]) -> Duck:
        src = from_odbc_s21(con=con, query=self._build_bottom_query(mid_keys))
        bottom = (
            src
            .add_columns(
                key_parent_item="replace(parent_item, ' ', '')",
                key_component_item="replace(component_item, ' ', '')",
                effectivity_from_date=self._DATE_FROM,
                effectivity_to_date=self._DATE_TO,
            )
            .add_columns(key_mid_level_parent_full="key_parent_item")
            .rename(
                mid_level_parent_full="parent_item",
                key_mid_level_parent="key_parent_item",
                start_e_mid="effectivity_from_date",
                end_e_mid="effective_to_date",
                OperationCodeMid="component_sequence",
                OverallOperatonCodeMid="operation_sequence",
            )
            .select_columns(
                "mid_level_parent_full",
                "key_mid_level_parent_full",
                "key_mid_level_parent",
                "component_item",
                "key_component_item",
                "manufacturing_route_code",
                "OverallOperatonCodeMid",
                "OperationCodeMid",
                "start_e_mid",
                "end_e_mid",
            )
            .distinct()
            .materialize()
        )
        return _cast_bottom_schema(bottom)

# =============================================================================
# Unified cache builder for a relation
# =============================================================================

def _ensure_cache_rel(
    con: duckdb.DuckDBPyConnection,
    folder: Path,
    TTL: int,
    build_rel_fn: Callable[[], Duck],
) -> Path:
    day_stem = _today_stamp()
    latest = _latest_entry(folder)
    if latest and _is_fresh(latest, TTL):
        print(f"[cache] HIT -> {latest.name}")
        return latest

    print(f"[cache] MISS/EXPIRED -> rebuilding")
    rel = build_rel_fn()
    rc = _sql_count(con, rel, "__tmp_for_count")

    if rc == 0:
        empty = folder / f"{day_stem}.empty"
        empty.touch()
        _cleanup_keep_only(folder, day_stem)
        print(f"[cache] EMPTY -> {empty.name}")
        return empty

    out = folder / f"{day_stem}.parquet"
    _copy_parquet_via_sql(con, rel, out, "__tmp_for_copy")
    _cleanup_keep_only(folder, day_stem)
    print(f"[cache] wrote rows={rc} -> {out.name}")
    return out

# =============================================================================
# Public API
# =============================================================================

def top_level_routes_and_structures(
    parent_item: int,
    TTL: int = 3600 * 24,
    strategy: MSP28Strategy | None = None,
) -> List[Path]:
    """
    Multistage cache for a given top-level parent:
      1) _top/YYYYMMDD.parquet  (raw TOP rows)
      2) _bottom/YYYYMMDD.parquet (raw BOTTOM rows for TOP's mid keys)
      3) YYYYMMDD.parquet (final combined with earliest OperationCodeMid per mid key)

    Default strategy is WarehouseMSP28Strategy (parquet warehouse). You may pass
    DB2MSP28Strategy() to use direct System21.
    """
    strategy = strategy or WarehouseMSP28Strategy()

    resolved = "top_level_routes_structures"
    root = CACHE_FOLDER / resolved / f"parent_item={parent_item}"
    top_dir = root / "_top"
    bot_dir = root / "_bottom"
    root.mkdir(parents=True, exist_ok=True)
    top_dir.mkdir(exist_ok=True)
    bot_dir.mkdir(exist_ok=True)

    day_stem = _today_stamp()
    combined_path = root / f"{day_stem}.parquet"
    combined_empty = root / f"{day_stem}.empty"

    with duckdb.connect() as con:
        # ---- TOP stage
        top_path = _ensure_cache_rel(
            con, top_dir, TTL,
            build_rel_fn=lambda: strategy.materialize_top(con, str(parent_item))
        )
        if top_path.suffix == ".empty":
            combined_empty.touch()
            _cleanup_keep_only(root, day_stem)
            return []

        top = Duck.from_parquet(con, [top_path])
        top = _cast_top_schema(top)

        # derive mid keys
        mid_keys = top.select_columns("key_mid_level_parent").distinct().fetch_list_str()
        if not mid_keys:
            print("[bottom] NO mid_keys; abort.")
            combined_empty.touch()
            _cleanup_keep_only(root, day_stem)
            return []

        # ---- BOTTOM stage
        bottom_path = _ensure_cache_rel(
            con, bot_dir, TTL,
            build_rel_fn=lambda: strategy.materialize_bottom(con, mid_keys)
        )
        if bottom_path.suffix == ".empty":
            combined_empty.touch()
            _cleanup_keep_only(root, day_stem)
            return []

        bottom = Duck.from_parquet(con, [bottom_path])
        bottom = _cast_bottom_schema(bottom)

        # ---- Narrow: earliest OperationCodeMid per key_mid_level_parent
        narrowed = (
            bottom
            .aggregate("key_mid_level_parent", OperationCodeMid="min(OperationCodeMid)")
            .inner_join(
                bottom,
                unnatural_pairs={
                    "key_mid_level_parent": "key_mid_level_parent",
                    "OperationCodeMid": "OperationCodeMid",
                },
            )
            .materialize()
        )
        narrowed = _cast_bottom_schema(narrowed)

        # ---- Combine (ONLY on key_mid_level_parent)
        combined = top.inner_join(
            narrowed,
            unnatural_pairs={"key_mid_level_parent": "key_mid_level_parent"},
        )
        rc = _sql_count(con, combined, "__tmp_combined_count")
        print(f"[combine] rows={rc}")

        if rc == 0:
            combined_empty.touch()
            _cleanup_keep_only(root, day_stem)
            return []

        _copy_parquet_via_sql(con, combined, combined_path, "__tmp_combined_copy")
        _cleanup_keep_only(root, day_stem)
        print(f"[cache] WRITE combined -> {combined_path.name}")
        logger.info("cache write parent_item=%s -> %s", parent_item, combined_path.name)

    return [combined_path]

def get_top_level_routes_and_structures(parent_items: list[int], date_val: date) -> list[Path]:
    ttl_by_date = max(
        3600 * 24,
        (datetime.combine(date.today(), datetime.min.time()) - datetime.combine(date_val, datetime.min.time())).total_seconds(),
    )
    print(f"[batch] start count={len(parent_items)} TTL={ttl_by_date:.0f}s date={date_val.isoformat()}")
    out: list[Path] = []
    for pid in parent_items:
        try:
            res = top_level_routes_and_structures(pid, TTL=int(ttl_by_date))
            if not res:
                with duckdb.connect() as con:
                    Duck(con, con.sql(f"SELECT {pid} as product_code")).append_csv(
                        CACHE_FOLDER / "top_level_routes_structures" / "empty.csv"
                    )
            print(f"[batch] parent_item={pid} -> {len(res)} file(s)")
            out.extend(res)
        except Exception as e:
            logger.error("parent_item=%s failed: %s", pid, e, exc_info=True)
            print(f"[batch] ERROR parent_item={pid}: {e}")
    print(f"[batch] done files={len(out)}")
    if len(out) == 0:
        with duckdb.connect() as con:
            place = CACHE_FOLDER / "top_level_routes_structures" / "place.parquet"
            Duck(con, con.sql("SELECT * FROM (SELECT 1) WHERE 1=0")).write_parquet(place.as_posix())
            return [place]
    return out

if __name__ == "__main__":
    # Discover all numeric parent codes using the warehouse (unchanged discovery path)
    with duckdb.connect() as con:
        options = (
            warehouse_source(con, "system_twenty_one_MSP28", union_by_name=True)
            .add_columns(m="try_cast(PARENT_ITEM as UINTEGER)")
            .select_columns("m")
            .filter("m is not null")
            .distinct()
            .fetch_list_int()
        )
    logger.info("Example batch call with %d options…", len(options))
    print(f"[main] options count={len(options)}")
    _ = get_top_level_routes_and_structures(options, date.today())
