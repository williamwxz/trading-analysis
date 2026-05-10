"""
PnL Strategy v2 Assets — Full Recomputes

Unpartitioned assets for full historical PnL recomputes (prod, bt, real_trade).
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
    PROD_INSERT_COLUMNS,
    PROD_REAL_TRADE_START_DATE,
    REAL_TRADE_INSERT_COLUMNS,
    TIMEFRAME_MAP,
    compute_bt_pnl,
    compute_real_trade_pnl,
    fetch_anchors,
    fetch_prices_multi,
    iter_compute_prod_pnl,
)

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

# Map from 1min target table → its 1hour rollup table.
_HOUR_TABLE = {
    "strategy_pnl_1min_prod_v2": "strategy_pnl_1hour_prod_v2",
    "strategy_pnl_1min_bt_v2": "strategy_pnl_1hour_bt_v2",
    "strategy_pnl_1min_real_trade_v2": "strategy_pnl_1hour_real_trade_v2",
}


def _pause_ecs_service(service_name: str, cluster: str, boto_client) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=0)
    waiter = boto_client.get_waiter("services_stable")
    waiter.wait(cluster=cluster, services=[service_name])


def _resume_ecs_service(service_name: str, cluster: str, boto_client, desired_count: int = 1) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=desired_count)


def _refresh_hour_table(
    min_table: str,
    hour_table: str,
    window_start_ts: str,
    log_fn=None,
) -> None:
    """Re-aggregate 1hour rollup from the 1min table for the recomputed window.

    The Materialized View only fires on INSERT — it cannot fix 1hour rows that
    were written before the 1min recompute ran. After a recompute we must DELETE
    stale 1hour rows for the affected window and re-INSERT from the 1min source
    so the Grafana UNION query sees a consistent level across the ts boundary.

    Uses FINAL on the 1min source to get deduplicated rows after the recompute.
    """
    _emit = log_fn or _log.info
    client = get_client()

    execute(
        f"ALTER TABLE analytics.{hour_table} DELETE"
        f" WHERE ts >= toStartOfHour(toDateTime('{window_start_ts}'))",
        client=client,
    )
    _emit(f"[1hour] deleted {hour_table} rows >= hour({window_start_ts})")

    sql = f"""\
INSERT INTO analytics.{hour_table}
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version,
    toStartOfHour(minute_ts)           AS ts,
    argMax(cumulative_pnl, minute_ts)  AS cumulative_pnl,
    argMax(benchmark, minute_ts)       AS benchmark,
    argMax(position, minute_ts)        AS position,
    argMax(price, minute_ts)           AS price,
    argMax(final_signal, minute_ts)    AS final_signal,
    argMax(weighting, minute_ts)       AS weighting,
    now()                              AS updated_at
FROM (
    SELECT *, ts AS minute_ts
    FROM analytics.{min_table} FINAL
    WHERE ts >= toStartOfHour(toDateTime('{window_start_ts}'))
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfHour(minute_ts)
"""
    execute(sql, client=client)
    _emit(f"[1hour] re-aggregated {hour_table} from {window_start_ts}")


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
            for _stn, strategy_rows in iter_compute_prod_pnl(
                rows_dict, anchors, prices, source_label=label
            ):
                _prepare_rows_for_clickhouse(strategy_rows)
                n = insert_rows(
                    f"analytics.{target_table}", insert_columns, strategy_rows, client
                )
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
                n = insert_rows(
                    f"analytics.{target_table}", insert_columns, strategy_rows, client
                )
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
                n = insert_rows(
                    f"analytics.{target_table}", insert_columns, strategy_rows, client
                )
                total_rows += n
                if strategy_rows:
                    last = strategy_rows[-1]
                    anchors[stn] = (float(last[8]), float(last[11]), float(last[10]))
        else:
            raise ValueError(
                f"Unknown mode: {mode!r}. Expected 'prod', 'bt', or 'real_trade'."
            )

        del rows_dict, prices

        if chunks_done % 100 == 0:
            _emit(
                f"[{underlying}] progress: {chunks_done}/{chunk_count} chunks done "
                f"({total_rows:,} rows so far)"
            )

        chunk_start = chunk_end

    _emit(f"[{underlying}] complete: {total_rows:,} rows inserted")
    return total_rows


def _process_underlying_recent(
    underlying: str,
    target_table: str,
    source_table: str,
    label: str,
    insert_columns: list,
    mode: str,
    default_window_start: datetime,
    end_dt: datetime,
    log_fn=None,
) -> tuple[int, datetime]:
    """Delete from resume point and recompute for one underlying.

    Returns (rows_inserted, window_start) so the caller can track the earliest
    recompute boundary across all underlyings for the 1hour rollup refresh.

    Each underlying independently resumes from its own max(ts) in the target table
    (stepped back one chunk), or from default_window_start if no data exists.

    Order: compute window_start → load anchors (before window) → DELETE → recompute.
    Designed to run in a thread — owns its own ClickHouse client.
    """
    _emit = log_fn or _log.info
    client = get_client()

    resume_dt = _get_underlying_resume_dt(underlying, target_table, client)
    if resume_dt is not None:
        window_start = resume_dt
        _emit(f"[{underlying}] resuming from max(ts) − {_CHUNK_DAYS}d = {window_start.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        window_start = default_window_start
        _emit(f"[{underlying}] no existing data — starting from {window_start.strftime('%Y-%m-%d %H:%M:%S')}")

    window_start_ts = window_start.strftime("%Y-%m-%d %H:%M:%S")

    anchors = fetch_anchors(target_table, underlying, before_ts=window_start)
    _emit(f"[{underlying}] loaded {len(anchors)} anchors before {window_start_ts}")

    execute(
        f"ALTER TABLE analytics.{target_table} DELETE "
        f"WHERE underlying = '{underlying}' AND ts >= toDateTime('{window_start_ts}')",
        client=client,
    )
    _emit(f"[{underlying}] deleted rows >= {window_start_ts}")

    total_rows = 0
    chunk_count = math.ceil((end_dt - window_start).total_seconds() / 86400 / _CHUNK_DAYS)
    _emit(f"[{underlying}] recomputing {chunk_count} chunks from {window_start_ts}")

    chunk_start = window_start
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
        chunk_start = chunk_end

    _emit(f"[{underlying}] recent recompute complete: {total_rows:,} rows inserted")
    return total_rows, window_start


def _recompute_pnl_full(
    context: AssetExecutionContext,
    target_table: str,
    source_table: str,
    label: str,
    insert_columns: list,
    mode: str,
    start_date: str | None = None,
    max_workers: int = _MAX_WORKERS,
):
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
        f"chunk_days={_CHUNK_DAYS}, max_workers={max_workers}"
    )

    total_rows = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
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

    context.log.info(
        f"Full recompute {label} complete: {total_rows:,} total rows inserted"
    )
    return MaterializeResult(
        metadata={
            "rows_inserted": total_rows,
            "start_ts": f"{start_date} 00:00:00",
            "end_ts": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


def _recompute_pnl_recent(
    context: AssetExecutionContext,
    target_table: str,
    source_table: str,
    label: str,
    insert_columns: list,
    mode: str,
    ecs_service: str,
    ecs_resume_count: int = 1,
    max_workers: int = _MAX_WORKERS,
) -> MaterializeResult:
    """Delete last 3 days from target and recompute, seeded from anchors before the window.

    If the target table is empty (e.g. after a DROP + recreate), expands the window all
    the way back to PROD_REAL_TRADE_START_DATE so the full history is recomputed.

    Pauses the named ECS consumer service before any writes and resumes it in a finally
    block so it always restarts even if the recompute fails.

    Pass ecs_resume_count=0 for services that must stay stopped (e.g. pnl-consumer-bt).
    """
    end_dt = datetime.now(tz=UTC)
    # Each underlying resumes from its own max(ts) via _get_underlying_resume_dt.
    # default_window_start is used only when an underlying has no data at all.
    default_window_start = datetime.strptime(PROD_REAL_TRADE_START_DATE, "%Y-%m-%d").replace(tzinfo=UTC)

    underlyings = _get_underlyings(source_table)
    context.log.info(
        f"Recent recompute {label}: {len(underlyings)} underlyings, "
        f"end={end_dt.strftime('%Y-%m-%d %H:%M:%S')}, "
        f"ecs_service={ecs_service}, max_workers={max_workers}"
    )

    ecs = boto3.client("ecs", region_name=_ECS_REGION)
    total_rows = 0
    earliest_window_start: datetime | None = None
    try:
        _pause_ecs_service(ecs_service, _ECS_CLUSTER, ecs)
        context.log.info(f"Paused ECS service {ecs_service}")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _process_underlying_recent,
                    underlying,
                    target_table,
                    source_table,
                    label,
                    insert_columns,
                    mode,
                    default_window_start,
                    end_dt,
                    context.log.info,
                ): underlying
                for underlying in underlyings
            }
            for future in as_completed(futures):
                underlying = futures[future]
                rows, window_start = future.result()
                total_rows += rows
                context.log.info(f"[{underlying}] complete: {rows:,} rows inserted")
                if window_start is not None:
                    if earliest_window_start is None or window_start < earliest_window_start:
                        earliest_window_start = window_start

        # Re-aggregate the 1hour rollup table for the recomputed window.
        # The MV fires on INSERT but cannot fix pre-existing 1hour rows — we must
        # DELETE and re-INSERT from the now-correct 1min table to avoid a PnL level
        # jump at the recompute boundary in the Grafana UNION query.
        hour_table = _HOUR_TABLE.get(target_table)
        if hour_table and earliest_window_start is not None:
            window_start_ts = earliest_window_start.strftime("%Y-%m-%d %H:%M:%S")
            context.log.info(
                f"Refreshing {hour_table} from {window_start_ts} (earliest recompute boundary)"
            )
            _refresh_hour_table(target_table, hour_table, window_start_ts, context.log.info)
    finally:
        try:
            _resume_ecs_service(ecs_service, _ECS_CLUSTER, ecs, desired_count=ecs_resume_count)
            context.log.info(f"Resumed ECS service {ecs_service} (desiredCount={ecs_resume_count})")
        except Exception as resume_err:
            context.log.error(f"Failed to resume ECS service {ecs_service}: {resume_err}")

    context.log.info(f"Recent recompute {label} complete: {total_rows:,} total rows inserted")
    return MaterializeResult(metadata={
        "rows_inserted": total_rows,
        "default_window_start": default_window_start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_ts": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    })


# ─────────────────────────────────────────────────────────────────────────────
# 1. Prod PnL (Full Recompute — unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────


@asset(
    name="pnl_prod_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_prod_v2_full"},
)
def pnl_prod_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last 3 days and recompute prod PnL from anchors, pausing the prod consumer."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_prod_v2",
        source_table="strategy_output_history_v2",
        label="production",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="prod",
        ecs_service="trading-analysis-pnl-consumer-prod",
        ecs_resume_count=0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Real Trade PnL (Full Recompute — unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────


@asset(
    name="pnl_real_trade_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={
        "dagster/timeout": 86400,
        "dagster/concurrency_limit": "pnl_real_trade_v2_full",
    },
)
def pnl_real_trade_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last 3 days and recompute real_trade PnL from anchors, pausing the real-trade consumer."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_real_trade_v2",
        source_table="strategy_output_history_v2",
        label="real_trade",
        insert_columns=REAL_TRADE_INSERT_COLUMNS,
        mode="real_trade",
        ecs_service="trading-analysis-pnl-consumer-real-trade",
        max_workers=1,
        ecs_resume_count=0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Backtest PnL (Full Recompute — unpartitioned, sequential anchor chain)
# ─────────────────────────────────────────────────────────────────────────────


@asset(
    name="pnl_bt_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_bt_v2_full"},
)
def pnl_bt_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last 3 days and recompute BT PnL from anchors. bt consumer stays at desired_count=0."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_bt_v2",
        source_table="strategy_output_history_bt_v2",
        label="backtest",
        insert_columns=PROD_INSERT_COLUMNS,
        mode="bt",
        ecs_service="trading-analysis-pnl-consumer-bt",
        ecs_resume_count=0,
    )
