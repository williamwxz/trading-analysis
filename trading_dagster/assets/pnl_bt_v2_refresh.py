"""
ClickHouse Backtest PnL v2 Refresh Asset

Incrementally refreshes analytics.strategy_pnl_1min_bt_v2 every 10 minutes.
Uses Python-based anchor chaining (same as prod_v2).
Source: strategy_output_history_bt_v2 (backtest data).
"""

import time
from typing import List, Optional

from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..utils.clickhouse_client import execute, insert_rows, query_rows, query_scalar
from ..utils.pnl_compute import (
    fetch_new_bars_bt,
    compute_bt_pnl,
    PROD_INSERT_COLUMNS,
)

TARGET_TABLE = "strategy_pnl_1min_bt_v2"
SOURCE_TABLE = "strategy_output_history_bt_v2"


def _get_underlyings() -> List[str]:
    rows = query_rows(
        f"SELECT DISTINCT underlying FROM analytics.{SOURCE_TABLE} ORDER BY underlying"
    )
    return [str(r[0]) for r in rows]


def _get_source_max_revision(underlying: str) -> Optional[str]:
    v = query_scalar(
        f"SELECT toString(max(revision_ts)) FROM analytics.{SOURCE_TABLE} "
        f"WHERE underlying = '{underlying}'"
    )
    v = str(v).strip() if v else None
    return None if not v or v == "1970-01-01 00:00:00" else v


def _get_target_watermark(underlying: str) -> Optional[str]:
    v = query_scalar(
        f"SELECT toString(max(last_revision_ts)) FROM analytics.pnl_refresh_watermarks "
        f"WHERE underlying = '{underlying}' AND target_table = '{TARGET_TABLE}'"
    )
    v = str(v).strip() if v else None
    if v and v != "1970-01-01 00:00:00":
        return v
    v2 = query_scalar(
        f"SELECT toString(max(updated_at) - INTERVAL 2 HOUR) FROM analytics.{TARGET_TABLE} "
        f"WHERE underlying = '{underlying}'"
    )
    v2 = str(v2).strip() if v2 else None
    return None if not v2 or v2 == "1970-01-01 00:00:00" else v2


def _write_watermark(underlying: str, last_revision_ts: str) -> None:
    execute(
        f"INSERT INTO analytics.pnl_refresh_watermarks "
        f"(underlying, target_table, last_revision_ts, updated_at) "
        f"VALUES ('{underlying}', '{TARGET_TABLE}', "
        f"toDateTime('{last_revision_ts}'), now())"
    )





def _refresh_underlying(underlying: str, since: str, context) -> int:
    bars = fetch_new_bars_bt(SOURCE_TABLE, underlying, since)
    if not bars:
        context.log.info(f"[{underlying}] No new bars")
        return 0

    context.log.info(f"[{underlying}] {len(bars)} new bars")

    rows = compute_bt_pnl(bars)
    if not rows:
        return 0

    total = insert_rows(f"analytics.{TARGET_TABLE}", PROD_INSERT_COLUMNS, rows)
    context.log.info(f"[{underlying}] Inserted {total} rows")
    return total


@asset(
    name="pnl_bt_v2_refresh",
    group_name="strategy_pnl",
    deps=["binance_futures_ohlcv_minutely"],
    automation_condition=(
        AutomationCondition.any_deps_updated()
        | (
            AutomationCondition.on_cron("*/5 * * * *")
            & ~AutomationCondition.in_progress()
        )
    ),

    description=(
        "Incrementally refreshes analytics.strategy_pnl_1min_bt_v2 every 10 minutes. "
        "Uses Python-based anchor chaining for correct bar-to-bar PnL continuity."
    ),
    compute_kind="clickhouse",
)
def pnl_bt_v2_refresh_asset(
    context: AssetExecutionContext,
) -> MaterializeResult:
    underlyings = _get_underlyings()
    context.log.info(f"Underlyings: {underlyings}")

    skipped: List[str] = []
    refreshed: List[str] = []
    failed: List[str] = []
    total_elapsed = 0.0

    for underlying in underlyings:
        source_max = _get_source_max_revision(underlying)
        if not source_max:
            skipped.append(underlying)
            continue

        target_wm = _get_target_watermark(underlying)
        if target_wm and source_max <= target_wm:
            context.log.info(f"[{underlying}] Up to date (watermark={target_wm})")
            skipped.append(underlying)
            continue

        since = target_wm or "2020-01-01 00:00:00"
        context.log.info(f"[{underlying}] Refreshing (since={since})")

        t0 = time.time()
        try:
            _refresh_underlying(underlying, since, context)
            elapsed = time.time() - t0
            total_elapsed += elapsed
            context.log.info(f"[{underlying}] Done in {elapsed:.1f}s")
            refreshed.append(underlying)
            try:
                _write_watermark(underlying, source_max)
            except Exception as wm_exc:
                context.log.warning(f"[{underlying}] Watermark write failed: {wm_exc}")
        except Exception as exc:
            context.log.error(f"[{underlying}] Failed: {exc}")
            failed.append(underlying)

    if failed:
        raise RuntimeError(f"Failed underlyings: {failed}")

    return MaterializeResult(
        metadata={
            "underlyings_refreshed": MetadataValue.int(len(refreshed)),
            "underlyings_skipped": MetadataValue.int(len(skipped)),
            "refreshed": MetadataValue.text(", ".join(refreshed) or "none"),
            "elapsed_seconds": MetadataValue.float(round(total_elapsed, 1)),
        }
    )


__all__ = ["pnl_bt_v2_refresh_asset"]
