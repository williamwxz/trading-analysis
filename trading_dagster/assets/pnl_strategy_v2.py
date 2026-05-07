"""
PnL Strategy v2 Assets — Daily Backfills

Partitioned daily assets for historical PnL backfills (prod, bt, real_trade).
Real-time PnL is handled by the pnl_consumer (Kafka → ClickHouse).
"""

import logging
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

import boto3

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
    query_scalar,
)
from ..utils.pnl_compute import (
    BT_START_DATE,
    PROD_INSERT_COLUMNS,
    PROD_REAL_TRADE_START_DATE,
    REAL_TRADE_INSERT_COLUMNS,
    TIMEFRAME_MAP,
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

def _parse_ts(s: str) -> datetime:
    """Parse a datetime string with or without fractional seconds."""
    return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")


def _prepare_rows_for_clickhouse(rows: list[list]) -> list[list]:
    """Ensure timestamp strings are converted back to datetime objects for clickhouse-connect.

    Mutates rows in-place to avoid doubling peak memory for large strategy batches.
    Handles both PROD_INSERT_COLUMNS (15 cols) and REAL_TRADE_INSERT_COLUMNS (18 cols).
    """
    for r in rows:
        # ts=7, updated_at=14 in both column layouts
        if isinstance(r[7], str):
            r[7] = _parse_ts(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts(r[14])
        # closing_ts=15, execution_ts=16 in REAL_TRADE_INSERT_COLUMNS only
        if len(r) > 15 and isinstance(r[15], str):
            r[15] = _parse_ts(r[15])
        if len(r) > 16 and isinstance(r[16], str):
            r[16] = _parse_ts(r[16])
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# Full Recompute Logic (unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────

_ECS_CLUSTER = "trading-analysis"
_ECS_REGION = "ap-northeast-1"


def _pause_ecs_service(service_name: str, cluster: str, region: str, boto_client) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=0)
    waiter = boto_client.get_waiter("services_stable")
    waiter.wait(cluster=cluster, services=[service_name])


def _resume_ecs_service(service_name: str, cluster: str, region: str, boto_client) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=1)


_CHUNK_DAYS = 7  # process this many days at a time to cap memory per underlying

_MAX_WORKERS = 2  # keep within 2-vCPU Dagster task allocation

_log = logging.getLogger(__name__)


def _get_underlying_resume_dt(
    underlying: str, target_table: str, client
) -> datetime | None:
    """Return resume point for this underlying, or None if no data exists."""
    result = query_scalar(
        f"SELECT max(ts) FROM analytics.{target_table}"
        f" WHERE underlying = '{underlying}'",
        client=client,
    )
    if result is None:
        return None
    if isinstance(result, str):
        result = _parse_ts(result)
    # Step back one chunk to re-anchor from a clean boundary.
    return (result.replace(tzinfo=UTC) - timedelta(days=_CHUNK_DAYS)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _process_underlying(
    underlying: str,
    target_table: str,
    source_table: str,
    label: str,
    insert_columns: list,
    mode: str,
    start_dt: datetime,
    end_dt: datetime,
    log_fn=None,
) -> int:
    """Process all time chunks for one underlying. Returns total rows inserted.

    Designed to run in a thread — owns its own ClickHouse client and anchors dict.
    log_fn: callable(str) routed to context.log.info so progress appears in Dagster UI.
    """
    _emit = log_fn or _log.info
    client = get_client()

    resume_dt = _get_underlying_resume_dt(underlying, target_table, client)
    if resume_dt is not None and resume_dt > start_dt:
        _emit(
            f"[{underlying}] resuming from {resume_dt.strftime('%Y-%m-%d')}"
            " (skipping already-inserted data)"
        )
        start_dt = resume_dt

    anchors: dict = {}
    total_rows = 0
    chunk_count = math.ceil((end_dt - start_dt).total_seconds() / 86400 / _CHUNK_DAYS)
    chunks_done = 0

    _emit(
        f"[{underlying}] starting: {start_dt.strftime('%Y-%m-%d')} → "
        f"{end_dt.strftime('%Y-%m-%d')}, {chunk_count} chunks"
    )

    chunk_start = start_dt
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=_CHUNK_DAYS), end_dt)
        chunk_start_ts = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
        chunk_end_ts = chunk_end.strftime("%Y-%m-%d %H:%M:%S")

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
  AND toDateTime(ts) >= toDateTime('{chunk_start_ts}') AND toDateTime(ts) < toDateTime('{chunk_end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
        elif mode == "bt":
            sql = f"""\
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe,
    argMin(weighting, revision_ts) AS weighting,
    toString(ts) AS ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position') AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'price') AS bar_price,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal') AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark') AS bar_benchmark,
    JSONExtractFloat(argMin(row_json, revision_ts), 'cumulative_pnl') AS cumulative_pnl
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(ts) >= toDateTime('{chunk_start_ts}') AND toDateTime(ts) < toDateTime('{chunk_end_ts}')
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying, config_timeframe, ts
ORDER BY strategy_table_name, ts
"""
        else:  # real_trade
            tf_expr = "multiIf(config_timeframe = '5m', 5, config_timeframe = '10m', 10, config_timeframe = '15m', 15, config_timeframe = '30m', 30, config_timeframe = '1h', 60, config_timeframe = '4h', 240, config_timeframe = '1d', 1440, 5)"
            sql = f"""\
SELECT
    r.strategy_table_name,
    r.strategy_id, r.strategy_name, r.underlying, r.config_timeframe, r.weighting,
    toString(r.ts) AS ts,
    toString(r.ts + toIntervalMinute({tf_expr})) AS closing_ts,
    toString(toStartOfMinute(r.revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
    toString(r.revision_ts) AS revision_ts,
    toString(nb.next_bar_ts + toIntervalMinute({tf_expr})) AS next_bar_closing_ts,
    JSONExtractFloat(r.row_json, 'position') AS position,
    JSONExtractFloat(r.row_json, 'price') AS bar_price,
    JSONExtractFloat(r.row_json, 'final_signal') AS final_signal,
    JSONExtractFloat(r.row_json, 'benchmark') AS bar_benchmark
FROM analytics.{source_table} r
LEFT JOIN (
    SELECT
        strategy_table_name,
        ts,
        leadInFrame(ts, 1, ts) OVER (
            PARTITION BY strategy_table_name ORDER BY ts
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_bar_ts
    FROM (
        SELECT strategy_table_name, ts
        FROM analytics.{source_table}
        WHERE underlying = '{underlying}'
          AND strategy_table_name NOT LIKE 'manual_probe%'
          AND toDateTime(ts) >= toDateTime('{chunk_start_ts}')
          AND toDateTime(ts) < toDateTime('{chunk_end_ts}')
        GROUP BY strategy_table_name, ts
    )
) nb ON r.strategy_table_name = nb.strategy_table_name AND r.ts = nb.ts
WHERE r.underlying = '{underlying}'
  AND r.strategy_table_name NOT LIKE 'manual_probe%'
  AND toDateTime(r.ts) >= toDateTime('{chunk_start_ts}') AND toDateTime(r.ts) < toDateTime('{chunk_end_ts}')
ORDER BY r.strategy_table_name, r.ts, r.revision_ts
"""

        raw_rows = query_dicts(sql, client)
        chunks_done += 1
        # Remap to normalised keys so compute_*_pnl always sees plain string keys
        # regardless of how clickhouse-connect qualifies column names (e.g. "r.revision_ts").
        if mode == "real_trade":
            rows_dict = [
                {
                    "strategy_table_name": r["strategy_table_name"],
                    "strategy_id": int(r["strategy_id"]),
                    "strategy_name": r["strategy_name"],
                    "underlying": r["underlying"],
                    "config_timeframe": r["config_timeframe"],
                    "weighting": float(r["weighting"]),
                    "ts": str(r["ts"]),
                    "closing_ts": str(r["closing_ts"]),
                    "execution_ts": str(r["execution_ts"]),
                    "revision_ts": str(r["revision_ts"]),
                    "next_bar_closing_ts": str(r["next_bar_closing_ts"]),
                    "position": float(r["position"]),
                    "bar_price": float(r["bar_price"]),
                    "final_signal": float(r["final_signal"]),
                    "bar_benchmark": float(r["bar_benchmark"]),
                }
                for r in raw_rows
            ]
        else:
            rows_dict = raw_rows
        if not rows_dict:
            chunk_start = chunk_end
            if chunks_done % 100 == 0:
                _emit(
                    f"[{underlying}] progress: {chunks_done}/{chunk_count} chunks done "
                    f"({total_rows:,} rows so far)"
                )
            continue

        all_prices = fetch_prices_multi(
            underlyings=[underlying],
            ts_min=chunk_start_ts,
            ts_max=chunk_end_ts,
            client=client,
            extend_minutes=0,
        )
        prices = all_prices.pop(underlying, {})

        if mode == "prod":
            for _stn, strategy_rows in iter_compute_prod_pnl(rows_dict, anchors, prices, source_label=label):
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[_stn] = (float(last[8]), float(last[11]), float(last[10]))
        elif mode == "bt":
            for bar in rows_dict:
                tf_minutes = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
                bar["execution_ts"] = (
                    _parse_ts(bar["ts"]) + timedelta(minutes=tf_minutes)
                ).strftime("%Y-%m-%d %H:%M:%S")
            by_stn: dict[str, list] = defaultdict(list)
            for bar in rows_dict:
                by_stn[bar["strategy_table_name"]].append(bar)
            for _stn, stn_bars in by_stn.items():
                strategy_rows = compute_bt_pnl(stn_bars, prices, anchors=anchors)
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[_stn] = (float(last[8]), float(last[11]), float(last[10]))
        elif mode == "real_trade":
            by_strategy: dict[str, list] = defaultdict(list)
            for bar in rows_dict:
                by_strategy[bar["strategy_table_name"]].append(bar)
            for stn, strategy_bars in by_strategy.items():
                strategy_rows = compute_real_trade_pnl(strategy_bars, anchors, prices)
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(f"analytics.{target_table}", insert_columns, strategy_rows, client)
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[stn] = (float(last[8]), float(last[11]), float(last[10]))
        else:
            raise ValueError(f"Unknown mode: {mode!r}. Expected 'prod', 'bt', or 'real_trade'.")

        del rows_dict, prices

        if chunks_done % 100 == 0:
            _emit(
                f"[{underlying}] progress: {chunks_done}/{chunk_count} chunks done "
                f"({total_rows:,} rows so far)"
            )

        chunk_start = chunk_end

    _emit(f"[{underlying}] complete: {total_rows:,} rows inserted")
    return total_rows


def _recompute_pnl_full(context: AssetExecutionContext, target_table: str, source_table: str, label: str, insert_columns: list, mode: str, start_date: str | None = None):
    """Recompute PnL for all underlyings from start_date to now.

    mode: "prod" uses iter_compute_prod_pnl; "real_trade" uses compute_real_trade_pnl;
          "bt" uses compute_bt_pnl with open prices.
    Rows are re-inserted with a fresh updated_at; ReplacingMergeTree deduplicates.
    No DELETE — safe to rerun alongside pnl_consumer.
    Processes _CHUNK_DAYS at a time to bound memory usage.
    """
    if start_date is None:
        start_date = PROD_REAL_TRADE_START_DATE
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.now(tz=UTC)

    underlyings = _get_underlyings(source_table)
    context.log.info(
        f"Full recompute {label}: {len(underlyings)} underlyings, "
        f"{start_date} → {end_dt.strftime('%Y-%m-%d %H:%M:%S')}, "
        f"chunk_days={_CHUNK_DAYS}, max_workers={_MAX_WORKERS}"
    )

    total_rows = 0

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(
                _process_underlying,
                underlying,
                target_table,
                source_table,
                label,
                insert_columns,
                mode,
                start_dt,
                end_dt,
                context.log.info,
            ): underlying
            for underlying in underlyings
        }
        for future in as_completed(futures):
            underlying = futures[future]
            rows = future.result()  # re-raises any exception from the worker thread
            total_rows += rows
            context.log.info(f"[{underlying}] complete: {rows:,} rows inserted")

    context.log.info(f"Full recompute {label} complete: {total_rows:,} total rows inserted")
    return MaterializeResult(metadata={"rows_inserted": total_rows, "start_ts": f"{start_date} 00:00:00", "end_ts": end_dt.strftime("%Y-%m-%d %H:%M:%S")})


# ─────────────────────────────────────────────────────────────────────────────
# 1. Production PnL (Daily Backfill)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_prod_v2_daily",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    partitions_def=daily_partitions,
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 300, "dagster/concurrency_limit": "pnl_prod_v2_daily"},
)
def pnl_prod_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned production PnL backfill. Run partitions sequentially — each day reads
    the previous day's anchor from the target table, so concurrent partition runs will corrupt
    the chain. Use max_partitions_per_run=1 when triggering a full backfill."""
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
    """Daily partitioned backtest PnL backfill. Anchors chain across day boundaries via
    fetch_anchors — run partitions sequentially (max_partitions_per_run=1)."""
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
    op_tags={"dagster/timeout": 300, "dagster/concurrency_limit": "pnl_real_trade_v2_daily"},
)
def pnl_real_trade_v2_daily_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Daily partitioned real trade PnL backfill."""
    return _refresh_pnl_real_trade(context)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Prod PnL (Full Recompute — unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_prod_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_prod_v2_full"},
)
def pnl_prod_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full prod recompute from PROD_REAL_TRADE_START_DATE to now, 7-day chunks, anchors in-memory.

    No DELETE — safe to rerun; ReplacingMergeTree deduplicates on updated_at.
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
# 5. Real Trade PnL (Full Recompute — unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_real_trade_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_real_trade_v2_full"},
)
def pnl_real_trade_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full real_trade recompute from PROD_REAL_TRADE_START_DATE to now, 7-day chunks, anchors in-memory.

    No DELETE — safe to rerun; ReplacingMergeTree deduplicates on updated_at.
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
# 6. Backtest PnL (Full Recompute — unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="pnl_bt_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_bt_v2_full"},
)
def pnl_bt_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Full BT recompute from BT_START_DATE to now, 7-day chunks, anchors in-memory.

    Use this instead of pnl_bt_v2_daily when the backtester rewrites multi-year history.
    No DELETE — safe to rerun; ReplacingMergeTree deduplicates on updated_at.
    Expect 6+ hours for a full 6-year recompute.
    """
    return _recompute_pnl_full(
        context,
        target_table="strategy_pnl_1min_bt_v2",
        source_table="strategy_output_history_bt_v2",
        label="backtest",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="bt",
        start_date=BT_START_DATE,
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
        assert_anchors_present(anchors, rows_dict, source_table=source_table)
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

    all_prices = fetch_prices_multi(
        underlyings, start_ts, end_ts, client, extend_minutes=0
    )

    total_rows = 0
    for underlying in underlyings:
        bars = fetch_new_bars_bt(source_table, underlying, start_ts, ts_end=end_ts)
        if not bars:
            all_prices.pop(underlying, None)
            continue

        anchors = fetch_anchors(target_table, underlying)
        assert_anchors_present(anchors, bars, source_table=source_table, bar_ts_key="execution_ts")
        prices = all_prices.pop(underlying, {})
        rows = compute_bt_pnl(bars, prices, anchors=anchors)
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
        ts_min = min(b["execution_ts"] for b in bars)
        ts_max = max(b["execution_ts"] for b in bars)
        prices = fetch_prices(underlying, ts_min, ts_max, client)
        anchors = fetch_anchors(target_table, underlying)
        assert_anchors_present(anchors, bars, source_table=source_table, bar_ts_key="execution_ts")
        rows = compute_real_trade_pnl(bars, anchors, prices)
        _prepare_rows_for_clickhouse(rows)
        total_rows += insert_rows(f"analytics.{target_table}", REAL_TRADE_INSERT_COLUMNS, rows, client)

    return MaterializeResult(metadata={"partition": date_str, "rows_inserted": total_rows})
