"""
PnL Strategy v2 Assets — Daily Backfills

Partitioned daily assets for historical PnL backfills (prod, bt, real_trade).
Real-time PnL is handled by the pnl_consumer (Kafka → ClickHouse).
"""

from datetime import UTC, datetime, timedelta

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    asset,
)

from ..utils.clickhouse_client import (
    execute,
    get_client,
    insert_rows,
    query_dicts,
    query_rows,
)
from ..utils.pnl_compute import (
    BT_START_DATE,
    PROD_INSERT_COLUMNS,
    PROD_REAL_TRADE_START_DATE,
    REAL_TRADE_INSERT_COLUMNS,
    assert_anchors_present,
    compute_bt_pnl,
    compute_real_trade_pnl,
    fetch_anchors,
    fetch_new_bars_bt,
    fetch_new_bars_real_trade,
    fetch_prices,
    fetch_prices_multi,
    iter_compute_prod_pnl,
)

daily_partitions = DailyPartitionsDefinition(start_date=PROD_REAL_TRADE_START_DATE)
bt_daily_partitions = DailyPartitionsDefinition(start_date=BT_START_DATE)

# ─────────────────────────────────────────────────────────────────────────────
# Watermark + discovery helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_underlyings(source_table: str) -> list[str]:
    rows = query_rows(
        f"SELECT DISTINCT underlying FROM analytics.{source_table} "
        f"WHERE underlying IS NOT NULL AND underlying != '' "
        f"ORDER BY underlying"
    )
    return [str(r[0]) for r in rows]

def _prepare_rows_for_clickhouse(rows: list[list]) -> list[list]:
    """Ensure timestamp strings are converted back to datetime objects for clickhouse-connect.

    Mutates rows in-place to avoid doubling peak memory for large strategy batches.
    Handles both PROD_INSERT_COLUMNS (15 cols) and REAL_TRADE_INSERT_COLUMNS (18 cols).
    """
    for r in rows:
        # ts=7, updated_at=14 in both column layouts
        if isinstance(r[7], str):
            r[7] = datetime.strptime(r[7], "%Y-%m-%d %H:%M:%S")
        if isinstance(r[14], str):
            r[14] = datetime.strptime(r[14], "%Y-%m-%d %H:%M:%S")
        # closing_ts=15, execution_ts=16 in REAL_TRADE_INSERT_COLUMNS only
        if len(r) > 15 and isinstance(r[15], str):
            r[15] = datetime.strptime(r[15], "%Y-%m-%d %H:%M:%S")
        if len(r) > 16 and isinstance(r[16], str):
            r[16] = datetime.strptime(r[16], "%Y-%m-%d %H:%M:%S")
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# Full Recompute Logic (unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────


def _recompute_pnl_full(context, target_table: str, source_table: str, label: str, insert_columns: list, mode: str):
    """Recompute PnL for all underlyings from PROD_REAL_TRADE_START_DATE to now.

    mode: "prod" uses iter_compute_prod_pnl; "real_trade" uses compute_real_trade_pnl.
    Rows are re-inserted with a fresh updated_at; ReplacingMergeTree deduplicates.
    No DELETE — safe to rerun alongside pnl_consumer.
    """
    start_ts = f"{PROD_REAL_TRADE_START_DATE} 00:00:00"
    end_dt = datetime.now(tz=UTC)
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = get_client()
    underlyings = _get_underlyings(source_table)
    context.log.info(f"Full recompute {label}: {len(underlyings)} underlyings, {start_ts} → {end_ts}")

    total_rows = 0

    for underlying in underlyings:
        context.log.info(f"Processing underlying: {underlying}")

        if mode == "prod":
            sql = f"""\
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
    argMin(weighting, revision_ts) AS weighting,
    toString(ts) AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND ts >= toDateTime('{start_ts}') AND ts < toDateTime('{end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
        else:  # real_trade
            sql = f"""\
SELECT
    strategy_table_name,
    strategy_id, strategy_name, underlying, config_timeframe, weighting,
    toString(ts) AS ts,
    toString(ts + toIntervalMinute(multiIf(
        config_timeframe = '5m', 5, config_timeframe = '10m', 10,
        config_timeframe = '15m', 15, config_timeframe = '30m', 30,
        config_timeframe = '1h', 60, config_timeframe = '4h', 240,
        config_timeframe = '1d', 1440, 5
    ))) AS closing_ts,
    toString(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
    JSONExtractFloat(row_json, 'position') AS position,
    JSONExtractFloat(row_json, 'price') AS bar_price,
    JSONExtractFloat(row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(row_json, 'benchmark') AS bar_benchmark
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}')
ORDER BY strategy_table_name, toDateTime(ts), revision_ts
"""

        rows_dict = query_dicts(sql, client)
        if not rows_dict:
            context.log.info(f"  No bars for {underlying}, skipping.")
            continue

        all_prices = fetch_prices_multi(underlyings=[underlying], ts_min=start_ts, ts_max=end_ts, client=client, extend_minutes=0)
        prices = all_prices.get(underlying, {})

        anchors = fetch_anchors(target_table, underlying)

        if mode == "prod":
            for _stn, strategy_rows in iter_compute_prod_pnl(rows_dict, anchors, prices, source_label=label):
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                context.log.info(f"  [{underlying}] inserted {n} rows for strategy {_stn}")
        else:
            all_rows = compute_real_trade_pnl(rows_dict, anchors, prices)
            _prepare_rows_for_clickhouse(all_rows)
            n = insert_rows(f"analytics.{target_table}", insert_columns, all_rows, client)
            total_rows += n
            context.log.info(f"  [{underlying}] inserted {n} real_trade rows")

        del rows_dict, prices, all_prices

    context.log.info(f"Full recompute {label} complete: {total_rows} total rows inserted")
    return MaterializeResult(metadata={"rows_inserted": total_rows, "start_ts": start_ts, "end_ts": end_ts})


# ─────────────────────────────────────────────────────────────────────────────
# 1. Production PnL (Daily Backfill)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_prod_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=daily_partitions,
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 300},
)
def pnl_prod_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned production PnL backfill."""
    return _refresh_pnl_partitioned(context, "strategy_pnl_1min_prod_v2", "strategy_output_history_v2", "production")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Backtest PnL (Daily Backfill)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_bt_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=bt_daily_partitions,
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 300, "dagster/concurrency_limit": "pnl_bt_v2_daily"},
)
def pnl_bt_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned backtest PnL backfill. Uses cumulative_pnl from strategy JSON directly."""
    return _refresh_pnl_bt(context)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Real Trade PnL (Daily Backfill)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_real_trade_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=daily_partitions,
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 300},
)
def pnl_real_trade_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned real trade PnL backfill."""
    return _refresh_pnl_real_trade(context)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Production PnL (Full Recompute — unpartitioned, no timeout)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_prod_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
)
def pnl_prod_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full sequential recompute of production PnL from start date to now.

    Processes all underlyings chronologically in a single long-running job.
    Anchor chain is maintained in memory — no parallel partition race conditions.
    Trigger manually from the Dagster UI when a full recompute is needed.
    """
    return _recompute_pnl_full(
        context,
        target_table="strategy_pnl_1min_prod_v2",
        source_table="strategy_output_history_v2",
        label="production",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="prod",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. Real Trade PnL (Full Recompute — unpartitioned, no timeout)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_real_trade_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
)
def pnl_real_trade_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full sequential recompute of real_trade PnL from start date to now.

    Processes all underlyings chronologically in a single long-running job.
    Anchor chain is maintained in memory — no parallel partition race conditions.
    Trigger manually from the Dagster UI when a full recompute is needed.
    """
    return _recompute_pnl_full(
        context,
        target_table="strategy_pnl_1min_real_trade_v2",
        source_table="strategy_output_history_v2",
        label="real_trade",
        insert_columns=REAL_TRADE_INSERT_COLUMNS,
        mode="real_trade",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Refresh Logic
# ─────────────────────────────────────────────────────────────────────────────

def _refresh_pnl_partitioned(context, target_table: str, source_table: str, label: str) -> MaterializeResult:
    date_str = context.partition_key
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = start_dt + timedelta(days=1)

    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = get_client()
    context.log.info(f"Backfilling {label} PnL for {date_str}")

    # Idempotency: Clean partition
    # Wrap ts in toDateTime() on both sides to handle rows that may have been
    # inserted with ts as String (type mismatch causes NO_COMMON_TYPE otherwise).
    execute(f"DELETE FROM analytics.{target_table} WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}') AND source='{label}'", client)

    underlyings = _get_underlyings(source_table)

    # Fetch all price data in a single query to avoid N separate scans on
    # futures_price_1min (one per underlying). extend_minutes=0 because end_ts
    # is already the exclusive partition boundary — no extra window needed.
    # (The default extend_minutes=1440 is for the live path where ts_max is the
    # last bar's open time and we need prices for its full expansion window.)
    all_prices = fetch_prices_multi(underlyings, start_ts, end_ts, client, extend_minutes=0)

    total_rows = 0
    for underlying in underlyings:
        # Fetch bars specifically for this date window
        sql = f"""\
        SELECT
            strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
            argMin(weighting, revision_ts) AS weighting, toString(ts) AS ts_str,
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
        if not rows_dict:
            # Free this underlying's price slice even if there are no bars.
            all_prices.pop(underlying, None)
            continue
        # Rename ts_str → ts so downstream compute functions find the expected key
        for r in rows_dict:
            r["ts"] = r.pop("ts_str")

        # We still need anchors from the end of the previous day
        anchors = fetch_anchors(target_table, underlying)
        assert_anchors_present(anchors, rows_dict, start_date=PROD_REAL_TRADE_START_DATE)
        # Pop (not get) so this underlying's price dict is freed immediately after
        # use rather than being held in all_prices for the rest of the loop.
        prices = all_prices.pop(underlying, {})

        # Insert per-strategy so each strategy's expanded rows are freed after
        # insert rather than accumulating all strategies in memory at once.
        for _stn, strategy_rows in iter_compute_prod_pnl(rows_dict, anchors, prices, source_label=label):
            _prepare_rows_for_clickhouse(strategy_rows)
            total_rows += insert_rows(f"analytics.{target_table}", PROD_INSERT_COLUMNS, strategy_rows, client)

        # Explicitly release bars for this underlying before loading the next one.
        del rows_dict, prices

    return MaterializeResult(metadata={"partition": date_str, "rows_inserted": total_rows})

def _refresh_pnl_bt(context) -> MaterializeResult:
    target_table = "strategy_pnl_1min_bt_v2"
    source_table = "strategy_output_history_bt_v2"

    date_str = context.partition_key
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = start_dt + timedelta(days=1)
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = get_client()
    context.log.info(f"Backfilling backtest PnL for {date_str}")

    execute(
        f"DELETE FROM analytics.{target_table} "
        f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}') "
        f"AND source='backtest'",
        client,
    )

    underlyings = _get_underlyings(source_table)

    # Fetch close prices for all underlyings in one query (bt uses close, not open).
    # extend_minutes=0: end_ts is already the exclusive partition boundary.
    all_prices = fetch_prices_multi(
        underlyings, start_ts, end_ts, client, extend_minutes=0, price_column="close"
    )

    total_rows = 0
    for underlying in underlyings:
        bars = fetch_new_bars_bt(source_table, underlying, start_ts, ts_end=end_ts)
        if not bars:
            all_prices.pop(underlying, None)
            continue

        prices = all_prices.pop(underlying, {})
        rows = compute_bt_pnl(bars, prices)
        _prepare_rows_for_clickhouse(rows)
        total_rows += insert_rows(f"analytics.{target_table}", PROD_INSERT_COLUMNS, rows, client)
        del bars, prices

    return MaterializeResult(metadata={"partition": date_str, "rows_inserted": total_rows})


def _refresh_pnl_real_trade(context) -> MaterializeResult:
    source_table = "strategy_output_history_v2"
    target_table = "strategy_pnl_1min_real_trade_v2"

    date_str = context.partition_key
    start_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = start_dt + timedelta(days=1)
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    client = get_client()
    execute(
        f"DELETE FROM analytics.{target_table} "
        f"WHERE toDateTime(ts) >= toDateTime('{start_ts}') AND toDateTime(ts) < toDateTime('{end_ts}') "
        f"AND source='real_trade'",
        client
    )

    underlyings = _get_underlyings(source_table)
    total_rows = 0
    for underlying in underlyings:
        bars = fetch_new_bars_real_trade(source_table, underlying, start_ts, ts_end=end_ts)
        if not bars:
            continue
        ts_min = min(b["ts"] for b in bars)
        ts_max = max(b["ts"] for b in bars)
        prices = fetch_prices(underlying, ts_min, ts_max, client)
        anchors = fetch_anchors(target_table, underlying)
        assert_anchors_present(anchors, bars, start_date=PROD_REAL_TRADE_START_DATE, bar_ts_key="execution_ts")
        rows = compute_real_trade_pnl(bars, anchors, prices)
        _prepare_rows_for_clickhouse(rows)
        total_rows += insert_rows(f"analytics.{target_table}", REAL_TRADE_INSERT_COLUMNS, rows, client)

    return MaterializeResult(metadata={"partition": date_str, "rows_inserted": total_rows})
