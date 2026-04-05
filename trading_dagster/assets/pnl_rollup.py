"""
ClickHouse PnL Rollup Asset

Incrementally rolls up analytics.strategy_pnl_1min_*_v2 into
analytics.strategy_pnl_1hour_*_v2.

V2-only — no v1 rollup pairs (simplified from falcon-lakehouse).
"""

import time
from typing import List, Optional, Tuple

from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..utils.clickhouse_client import execute, query_rows, query_scalar


def _get_underlyings(source_table: str) -> List[str]:
    rows = query_rows(
        f"SELECT DISTINCT underlying FROM {source_table} ORDER BY underlying"
    )
    return [str(r[0]) for r in rows]


def _get_source_watermark(underlying: str, source_table: str) -> Optional[str]:
    v = query_scalar(
        f"SELECT toString(max(updated_at)) FROM {source_table} WHERE underlying = '{underlying}'"
    )
    v = str(v).strip() if v else None
    return None if not v or v == "1970-01-01 00:00:00" else v


def _get_rollup_watermark(underlying: str, rollup_table: str, buffer_hours: int) -> Optional[str]:
    v = query_scalar(
        f"SELECT if(max(ts) = toDateTime(0), '', toString(max(ts) - INTERVAL {buffer_hours} HOUR)) "
        f"FROM {rollup_table} FINAL WHERE underlying = '{underlying}'"
    )
    v = str(v).strip() if v else None
    return None if not v or v == "1970-01-01 00:00:00" else v


def _rollup_sql(
    underlying: str,
    source_table: str,
    rollup_table: str,
    bucket_fn: str,
    since: str,
) -> str:
    return f"""\
INSERT INTO {rollup_table}
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    bucket                          AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at
FROM (
    SELECT
        strategy_table_name, strategy_id, strategy_name, underlying,
        config_timeframe, source, version,
        ts AS src_ts,
        {bucket_fn}(ts) AS bucket,
        cumulative_pnl, benchmark, position, price, final_signal, weighting
    FROM {source_table} FINAL
    WHERE underlying = '{underlying}'
      AND ts >= toDateTime('{since}')
)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, bucket
"""


# V2-only rollup pairs
_ROLLUP_PAIRS: List[Tuple[str, str]] = [
    ("analytics.strategy_pnl_1min_prod_v2",       "analytics.strategy_pnl_{resolution}_prod_v2"),
    ("analytics.strategy_pnl_1min_bt_v2",         "analytics.strategy_pnl_{resolution}_bt_v2"),
    ("analytics.strategy_pnl_1min_real_trade_v2",  "analytics.strategy_pnl_{resolution}_real_trade_v2"),
]


def _run_rollup(
    context: AssetExecutionContext,
    resolution: str,
    bucket_fn: str,
    buffer_hours: int,
) -> MaterializeResult:
    refreshed: List[str] = []
    skipped: List[str] = []
    failed: List[str] = []
    total_elapsed = 0.0

    for source_table, rollup_tmpl in _ROLLUP_PAIRS:
        rollup_table = rollup_tmpl.format(resolution=resolution)
        variant = source_table.split("_")[-1]  # prod_v2, bt_v2, real_trade_v2

        underlyings = _get_underlyings(source_table)
        context.log.info(f"[{variant}] Underlyings: {underlyings}")

        for underlying in underlyings:
            source_wm = _get_source_watermark(underlying, source_table)
            if not source_wm:
                skipped.append(f"{variant}/{underlying}")
                continue

            rollup_wm = _get_rollup_watermark(underlying, rollup_table, buffer_hours)
            if rollup_wm and source_wm <= rollup_wm:
                skipped.append(f"{variant}/{underlying}")
                continue

            since = rollup_wm or "2020-01-01 00:00:00"
            context.log.info(f"[{variant}/{underlying}] Rolling up since {since}")

            sql = _rollup_sql(underlying, source_table, rollup_table, bucket_fn, since)
            t0 = time.time()
            try:
                execute(sql)
                elapsed = time.time() - t0
                total_elapsed += elapsed
                context.log.info(f"[{variant}/{underlying}] Done in {elapsed:.1f}s")
                refreshed.append(f"{variant}/{underlying}")
            except Exception as exc:
                context.log.error(f"[{variant}/{underlying}] Failed: {exc}")
                failed.append(f"{variant}/{underlying}")

    if failed:
        raise RuntimeError(f"Failed: {failed}")

    return MaterializeResult(
        metadata={
            "refreshed": MetadataValue.text(", ".join(refreshed) or "none"),
            "skipped": MetadataValue.text(", ".join(skipped) or "none"),
            "elapsed_seconds": MetadataValue.float(round(total_elapsed, 1)),
        }
    )


@asset(
    name="pnl_1hour_rollup",
    group_name="strategy_pnl",
    deps=[
        "pnl_prod_v2_refresh",
        "pnl_bt_v2_refresh",
        "pnl_real_trade_v2_refresh",
    ],
    automation_condition=(
        AutomationCondition.on_cron("15 * * * *") & ~AutomationCondition.in_progress()
    ),
    description=(
        "Rolls up strategy_pnl_1min_*_v2 → strategy_pnl_1hour_*_v2. "
        "Runs 15 min past each hour."
    ),
    compute_kind="clickhouse",
)
def pnl_1hour_rollup_asset(context: AssetExecutionContext) -> MaterializeResult:
    return _run_rollup(context, resolution="1hour", bucket_fn="toStartOfHour", buffer_hours=24)


__all__ = ["pnl_1hour_rollup_asset"]
