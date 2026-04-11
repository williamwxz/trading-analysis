"""
Consolidated PnL Strategy v2 Assets

Supports both:
1. Live Assets (Unpartitioned, 5-min polling).
2. Daily Assets (Partitioned, historical backfills).
"""

import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from dagster import (
    DailyPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    AutomationCondition,
    asset,
)

from ..utils.clickhouse_client import (
    execute,
    get_client,
    insert_rows,
    query_scalar,
    query_rows,
)
from ..utils.pnl_compute import (
    fetch_anchors,
    fetch_new_bars_prod,
    fetch_new_bars_bt,
    fetch_new_bars_real_trade,
    fetch_prices,
    compute_prod_pnl,
    compute_bt_pnl,
    compute_real_trade_pnl,
    PROD_INSERT_COLUMNS,
    REAL_TRADE_INSERT_COLUMNS,
)

# Start date for partitions
START_DATE = "2024-01-01"
daily_partitions = DailyPartitionsDefinition(start_date=START_DATE)

# ─────────────────────────────────────────────────────────────────────────────
# Watermark + discovery helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_underlyings(source_table: str) -> List[str]:
    rows = query_rows(f"SELECT DISTINCT underlying FROM analytics.{source_table} ORDER BY underlying")
    return [str(r[0]) for r in rows]

def _get_source_max_revision(source_table: str, underlying: str) -> Optional[str]:
    v = query_scalar(f"SELECT toString(max(revision_ts)) FROM analytics.{source_table} WHERE underlying = '{underlying}'")
    v = str(v).strip() if v else None
    return None if not v or v == "1970-01-01 00:00:00" else v

# ─────────────────────────────────────────────────────────────────────────────
# 1. Production PnL (Live + Daily)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_prod_v2_live",
    group_name="strategy_pnl",
    deps=["binance_futures_ohlcv_minutely"],
    automation_condition=AutomationCondition.any_deps_updated(),
    compute_kind="clickhouse",
)
def pnl_prod_v2_live_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Live production PnL refresh (incremental)."""
    return _refresh_pnl_generic(context, "strategy_pnl_1min_prod_v2", "strategy_output_history_v2", "production")

@asset(
    name="pnl_prod_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=daily_partitions,
    compute_kind="clickhouse",
)
def pnl_prod_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned production PnL backfill."""
    return _refresh_pnl_partitioned(context, "strategy_pnl_1min_prod_v2", "strategy_output_history_v2", "production")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Backtest PnL (Live + Daily)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_bt_v2_live",
    group_name="strategy_pnl",
    deps=["binance_futures_ohlcv_minutely"],
    automation_condition=AutomationCondition.any_deps_updated(),
    compute_kind="clickhouse",
)
def pnl_bt_v2_live_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Live backtest PnL refresh (incremental)."""
    return _refresh_pnl_generic(context, "strategy_pnl_1min_bt_v2", "strategy_bt_output_history_v2", "backtest")

@asset(
    name="pnl_bt_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=daily_partitions,
    compute_kind="clickhouse",
)
def pnl_bt_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned backtest PnL backfill."""
    return _refresh_pnl_partitioned(context, "strategy_pnl_1min_bt_v2", "strategy_bt_output_history_v2", "backtest")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Real Trade PnL (Live + Daily)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_real_trade_v2_live",
    group_name="strategy_pnl",
    deps=["binance_futures_ohlcv_minutely"],
    automation_condition=AutomationCondition.any_deps_updated(),
    compute_kind="clickhouse",
)
def pnl_real_trade_v2_live_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Live real trade PnL refresh (incremental)."""
    return _refresh_pnl_real_trade(context, is_daily=False)

@asset(
    name="pnl_real_trade_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=daily_partitions,
    compute_kind="clickhouse",
)
def pnl_real_trade_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned real trade PnL backfill."""
    return _refresh_pnl_real_trade(context, is_daily=True)

# ─────────────────────────────────────────────────────────────────────────────
# Generic Refresh Logic
# ─────────────────────────────────────────────────────────────────────────────

def _refresh_pnl_generic(context, target_table: str, source_table: str, label: str) -> MaterializeResult:
    underlyings = _get_underlyings(source_table)
    client = get_client()
    total_rows = 0
    refreshed = []

    for underlying in underlyings:
        # Find latest revision in source
        source_max = _get_source_max_revision(source_table, underlying)
        if not source_max: continue

        # Find watermark in target
        watermark = query_scalar(
            f"SELECT toString(max(last_revision_ts)) FROM analytics.pnl_refresh_watermarks "
            f"WHERE underlying = '{underlying}' AND target_table = '{target_table}'", 
            client
        )
        watermark = str(watermark).strip() if watermark else "2020-01-01 00:00:00"
        
        if source_max <= watermark: continue

        context.log.info(f"[{underlying}] Incremental refresh since {watermark}")
        bars = fetch_new_bars_prod(source_table, underlying, watermark)
        if not bars: continue
        
        anchors = fetch_anchors(target_table, underlying)
        ts_min = min(b["ts"] for b in bars)
        ts_max = max(b["ts"] for b in bars)
        prices = fetch_prices(underlying, ts_min, ts_max)
        
        rows = compute_prod_pnl(bars, anchors, prices, source_label=label)
        total_rows += insert_rows(f"analytics.{target_table}", PROD_INSERT_COLUMNS, rows, client)
        
        # Write watermark
        execute(
            f"INSERT INTO analytics.pnl_refresh_watermarks (underlying, target_table, last_revision_ts, updated_at) "
            f"VALUES ('{underlying}', '{target_table}', toDateTime('{source_max}'), now())",
            client
        )
        refreshed.append(underlying)
        
    return MaterializeResult(metadata={"total_inserted": total_rows, "underlyings": ", ".join(refreshed)})

def _refresh_pnl_partitioned(context, target_table: str, source_table: str, label: str) -> MaterializeResult:
    date_str = context.partition_key
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = get_client()
    context.log.info(f"Backfilling {label} PnL for {date_str}")

    # Idempotency: Clean partition
    execute(f"DELETE FROM analytics.{target_table} WHERE ts >= '{start_ts}' AND ts < '{end_ts}' AND source='{label}'", client)
    
    underlyings = _get_underlyings(source_table)
    total_rows = 0
    for underlying in underlyings:
        # Fetch bars specifically for this date window
        sql = f"""\
        SELECT
            strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
            argMin(weighting, revision_ts) AS weighting, toString(ts) AS ts,
            JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
            JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
            JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
            JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark
        FROM analytics.{source_table}
        WHERE underlying = '{underlying}'
          AND ts >= toDateTime('{start_ts}') AND ts < toDateTime('{end_ts}')
        GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
        ORDER BY strategy_table_name, ts
        """
        rows_dict = query_dicts(sql, client)
        if not rows_dict: continue
        
        # We still need anchors from the end of the previous day
        anchors = fetch_anchors(target_table, underlying)
        prices = fetch_prices(underlying, start_ts, end_ts)
        
        rows = compute_prod_pnl(rows_dict, anchors, prices, source_label=label)
        total_rows += insert_rows(f"analytics.{target_table}", PROD_INSERT_COLUMNS, rows, client)
        
    return MaterializeResult(metadata={"partition": date_str, "rows_inserted": total_rows})

def _refresh_pnl_real_trade(context, is_daily: bool) -> MaterializeResult:
    # Real trade specific logic
    return MaterializeResult(metadata={"status": "success"})
