"""PnL Strategy v2 Assets — Full Recomputes

Unpartitioned assets for full historical PnL recomputes (prod, bt, real_trade).
Real-time PnL is handled by the pnl_consumer (Kafka → ClickHouse).

All computation logic delegates to libs.computation. This file handles only:
  - Dagster asset definitions and metadata
  - Chunked iteration over time windows per underlying
  - ECS consumer pause/resume
  - 1hour rollup refresh
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

import boto3
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from libs.computation import (
    INSERT_COLUMNS,
    AnchorRecord,
    AnchorState,
    active_prod_bar_at,
    active_rt_revision_at,
    build_carry_forward_row,
    build_pnl_row,
    build_prod_lookup,
    build_rt_lookup,
    check_strategy_drop,
    compute_bt_pnl,
    fetch_anchors,
    fetch_new_bars_bt,
    fetch_new_bars_prod,
    fetch_new_bars_real_trade,
    fetch_prices_multi,
    first_active_minute,
    last_active_minute,
)

from ..utils.clickhouse_client import (
    execute,
    get_client,
    insert_rows,
    query_rows,
    query_scalar,
)

PROD_REAL_TRADE_START_DATE = "2026-02-27"

_log = logging.getLogger(__name__)
_CHUNK_DAYS = 7
_CPU_COUNT = os.cpu_count() or 1
_MAX_WORKERS = max(1, _CPU_COUNT - 1)
_ECS_CLUSTER = "trading-analysis"
_ECS_REGION = "ap-northeast-1"

_HOUR_TABLE = {
    "strategy_pnl_1min_prod_v2": "strategy_pnl_1hour_prod_v2",
    "strategy_pnl_1min_bt_v2": "strategy_pnl_1hour_bt_v2",
    "strategy_pnl_1min_real_trade_v2": "strategy_pnl_1hour_real_trade_v2",
}


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def _prepare_rows_for_clickhouse(rows: list[list]) -> list[list]:
    """Convert ts (index 7) and updated_at (index 14) strings to datetime objects."""
    for r in rows:
        if isinstance(r[7], str):
            r[7] = _parse_ts(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts(r[14])
    return rows


def _get_underlyings(source_table: str) -> list[str]:
    rows = query_rows(
        f"SELECT DISTINCT underlying FROM analytics.{source_table} "
        f"WHERE underlying IS NOT NULL AND underlying != '' ORDER BY underlying"
    )
    return [str(r[0]) for r in rows]


def _get_underlying_resume_dt(
    underlying: str, target_table: str, client
) -> datetime | None:
    result = query_scalar(
        f"SELECT max(ts) FROM analytics.{target_table} WHERE underlying = '{underlying}'",
        client=client,
    )
    if result is None:
        return None
    if isinstance(result, str):
        result = _parse_ts(result)
    result = result.replace(tzinfo=UTC)
    # ClickHouse returns epoch (1970-01-01) for max(ts) on an empty table — treat as no rows.
    if result.year < 2000:
        return None
    return (result - timedelta(days=_CHUNK_DAYS)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _pause_ecs_service(service_name: str, cluster: str, boto_client) -> None:
    boto_client.update_service(cluster=cluster, service=service_name, desiredCount=0)
    boto_client.get_waiter("services_stable").wait(
        cluster=cluster, services=[service_name]
    )


def _resume_ecs_service(
    service_name: str, cluster: str, boto_client, desired_count: int = 1
) -> None:
    boto_client.update_service(
        cluster=cluster, service=service_name, desiredCount=desired_count
    )


def _refresh_hour_table(
    min_table: str, hour_table: str, window_start_ts: str, log_fn=None
) -> None:
    """Re-aggregate 1hour rollup from the 1min table for the recomputed window.

    Simple argMax GROUP BY over the recomputed window — no fill-forward needed
    because the full-recompute writes a 1-min row for every minute in the window,
    so every strategy has data in every hour slot it is active.

    The streaming gap problem (underlyings missing from some hour slots) is handled
    separately by the pnl_hourly_rollup_asset which runs every hour.
    """
    _emit = log_fn or _log.info
    client = get_client()
    execute(
        f"DELETE FROM analytics.{hour_table}"
        f" WHERE ts >= toStartOfHour(toDateTime('{window_start_ts}'))",
        client=client,
    )
    _emit(f"[1hour] deleted {hour_table} rows >= hour({window_start_ts})")
    execute(
        f"""\
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
    now()                              AS updated_at,
    any(strategy_instance_id)          AS strategy_instance_id
FROM (
    SELECT *, ts AS minute_ts
    FROM analytics.{min_table}
    WHERE ts >= toStartOfHour(toDateTime('{window_start_ts}'))
    ORDER BY strategy_table_name, strategy_id, strategy_name, underlying,
             config_timeframe, source, version, ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying,
               config_timeframe, source, version, ts
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfHour(minute_ts)
""",
        client=client,
    )
    _emit(f"[1hour] re-aggregated {hour_table} from {window_start_ts}")


def _get_source_strategies(
    source_table: str, underlying: str, ts_start: str, ts_end: str, client
) -> set[str]:
    """Return distinct strategy_table_names in source for the given window."""
    rows = query_rows(
        f"SELECT DISTINCT strategy_table_name FROM analytics.{source_table} "
        f"WHERE underlying = '{underlying}' "
        f"  AND strategy_table_name NOT LIKE 'manual_probe%' "
        f"  AND toDateTime(ts) >= toDateTime('{ts_start}') "
        f"  AND toDateTime(ts) < toDateTime('{ts_end}')",
        client=client,
    )
    return {str(r[0]) for r in rows}


def _check_output_completeness(
    target_table: str,
    underlying: str,
    ts_start: str,
    ts_end: str,
    source_stns: set[str],
    client,
    log_fn=None,
) -> None:
    """Fail if any source strategy is missing from the written output window.

    The output query extends ts_end by 1 day to account for bar expansion:
    a bar near ts_end may have its first output row at closing_ts = bar_ts + tf_minutes,
    which can land up to 1440 minutes (1d bars) past ts_end.

    Raises RuntimeError listing the missing strategies so the Dagster run is
    marked failed and the operator knows to investigate before the consumer restarts.
    """
    _emit = log_fn or _log.info
    if not source_stns:
        return
    rows = query_rows(
        f"SELECT DISTINCT strategy_table_name FROM analytics.{target_table} "
        f"WHERE underlying = '{underlying}' "
        f"  AND ts >= toDateTime('{ts_start}') "
        f"  AND ts < toDateTime('{ts_end}') + INTERVAL 1 DAY",
        client=client,
    )
    written_stns = {str(r[0]) for r in rows}
    missing = source_stns - written_stns
    if missing:
        raise RuntimeError(
            f"[{underlying}] Output completeness check failed: "
            f"{len(missing)} of {len(source_stns)} source strategies missing from "
            f"{target_table} in [{ts_start}, {ts_end}). "
            f"Missing: {sorted(missing)[:5]}{'...' if len(missing) > 5 else ''}"
        )
    _emit(
        f"[{underlying}] completeness OK: {len(written_stns)}/{len(source_stns)} strategies "
        f"written in [{ts_start}, {ts_end})"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Per-underlying recent recompute (delete + recompute from anchor)
# ─────────────────────────────────────────────────────────────────────────────


def _process_underlying_recent(
    underlying: str,
    target_table: str,
    source_table: str,
    label: str,
    mode: str,
    default_window_start: datetime,
    end_dt: datetime,
    log_fn=None,
) -> tuple[int, datetime]:
    """Recompute PnL for one underlying over [window_start, end_dt) minute by minute.

    Mirrors pnl_consumer semantics exactly: for each minute, resolve the currently
    active bar/revision from a pre-fetched snapshot, compute PnL, and write all rows
    for that minute atomically. Fails the job if any source strategy is absent from
    the written output (catches late-arriving source bars).
    """
    _emit = log_fn or _log.info
    client = get_client()
    is_rt = mode == "real_trade"

    resume_dt = _get_underlying_resume_dt(underlying, target_table, client)
    window_start = resume_dt if resume_dt is not None else default_window_start
    # Strip tzinfo — all internal datetimes are naive (parsed from ClickHouse strings).
    window_start = window_start.replace(tzinfo=None)
    end_dt = end_dt.replace(tzinfo=None)
    window_start_ts = window_start.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    _emit(f"[{underlying}] window_start={window_start_ts}")

    # Snapshot source strategies before DELETE — used for post-write completeness check.
    source_stns = _get_source_strategies(
        source_table, underlying, window_start_ts, end_ts, client
    )
    _emit(
        f"[{underlying}] source has {len(source_stns)} strategies in [{window_start_ts}, {end_ts})"
    )

    # Load anchors from the last committed row before window_start and seed AnchorState.
    anchors_raw: dict[str, tuple[float, float, float]] = fetch_anchors(
        target_table, underlying, before_ts=window_start, client=client
    )
    state = AnchorState()
    for stn, (pnl, price, position) in anchors_raw.items():
        state.set(stn, AnchorRecord(pnl=pnl, price=price, position=position))
    _emit(f"[{underlying}] loaded {len(state)} anchors")

    execute(
        f"DELETE FROM analytics.{target_table}"
        f" WHERE underlying = '{underlying}' AND ts >= toDateTime('{window_start_ts}')",
        client=client,
    )
    _emit(f"[{underlying}] deleted rows >= {window_start_ts}")

    # ── Fetch all bars/revisions and prices for the full window upfront ──────
    if mode == "prod":
        all_bars = fetch_new_bars_prod(
            source_table, underlying, window_start_ts, end_ts, client
        )
    elif mode == "bt":
        all_bars = fetch_new_bars_bt(
            source_table, underlying, window_start_ts, end_ts, client
        )
    else:
        all_bars = fetch_new_bars_real_trade(
            source_table, underlying, window_start_ts, end_ts, client
        )

    if not all_bars:
        _emit(f"[{underlying}] no bars in window — skipping")
        return 0, window_start

    # Prices include expansion past end_ts (1d bars can expand 1440 min past their open).
    prices_map = fetch_prices_multi(
        [underlying], window_start_ts, end_ts, client, extend_minutes=1440
    )
    prices = prices_map.pop(underlying, {})

    # ── Build per-strategy sorted lookup ─────────────────────────────────────
    if is_rt:
        lookup = build_rt_lookup(all_bars)
    else:
        lookup = build_prod_lookup(all_bars)

    minute_start = first_active_minute(lookup, is_rt)
    minute_end = last_active_minute(lookup, is_rt)
    if minute_start is None or minute_end is None:
        _emit(f"[{underlying}] empty lookup — skipping")
        return 0, window_start

    # Clamp: don't write before window_start; stop at end_dt (consumer takes over after).
    minute_cur = max(minute_start, window_start)
    minute_end = min(minute_end, end_dt)

    _emit(
        f"[{underlying}] iterating {int((minute_end - minute_cur).total_seconds() // 60)} minutes "
        f"[{minute_cur}, {minute_end})"
    )

    # ── Per-minute loop ───────────────────────────────────────────────────────
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0
    prev_active: set[str] | None = None
    all_stns = set(lookup.keys())

    while minute_cur < minute_end:
        ts_str = minute_cur.strftime("%Y-%m-%d %H:%M:%S")
        price = prices.get(ts_str)
        if price is None:
            minute_cur += timedelta(minutes=1)
            continue

        # Resolve active bar/revision per strategy at this minute.
        curr_active: set[str] = set()
        minute_rows: list[list] = []

        for stn in all_stns:
            if is_rt:
                entry = active_rt_revision_at(lookup, stn, minute_cur)
            else:
                entry = active_prod_bar_at(lookup, stn, minute_cur)
            if entry is None:
                continue
            curr_active.add(stn)

            bar = entry.rev if is_rt else entry.bar

            # Lazy-seed new strategies not yet present from fetch_anchors.
            if not state.has(stn):
                state.set(stn, AnchorRecord(pnl=0.0, price=price, position=0.0))

            meta = AnchorRecord(
                strategy_id=bar["strategy_id"],
                strategy_name=bar["strategy_name"],
                underlying=bar["underlying"],
                config_timeframe=bar["config_timeframe"],
                weighting=bar["weighting"],
                strategy_instance_id=bar.get("strategy_instance_id", ""),
                final_signal=bar["final_signal"],
                benchmark=bar["bar_benchmark"],
            )
            cpnl = state.compute_pnl(stn, price, bar["position"], meta=meta)
            minute_rows.append(
                build_pnl_row(stn, bar, price, cpnl, label, ts_str, now_str)
            )

        # Carry-forward: strategies previously seen (in state with bar metadata) but
        # with no active bar this minute hold their last known position until the next
        # bar arrives. This covers the gap when source bars are late (e.g. sid=11).
        for stn in list(state.keys()):
            if stn in curr_active:
                continue
            rec = state.get(stn)
            if not rec.strategy_instance_id:
                continue  # never saw a bar with metadata — skip
            cpnl = state.compute_pnl(stn, price, rec.position)
            minute_rows.append(
                build_carry_forward_row(stn, rec, price, cpnl, label, ts_str, now_str)
            )

        # Fail if a strategy that had an active bar at M-1 has no bar at M
        # AND still has future lookup entries (indicates a data hole, not late arrival).
        if prev_active is not None:
            check_strategy_drop(
                prev_active, curr_active, minute_cur, underlying, lookup, is_rt
            )

        if minute_rows:
            _prepare_rows_for_clickhouse(minute_rows)
            n = insert_rows(
                f"analytics.{target_table}", INSERT_COLUMNS, minute_rows, client
            )
            total_rows += n

        if curr_active:
            prev_active = curr_active
        minute_cur += timedelta(minutes=1)

    # Final completeness guard: every source strategy must appear in the output.
    _check_output_completeness(
        target_table, underlying, window_start_ts, end_ts, source_stns, client, _emit
    )

    _emit(f"[{underlying}] complete: {total_rows:,} rows")
    return total_rows, window_start


# ─────────────────────────────────────────────────────────────────────────────
# BT-specific recompute (parallel per-strategy, no minute-loop)
# ─────────────────────────────────────────────────────────────────────────────

_BT_STRATEGY_WORKERS = _CPU_COUNT * 2


def _compute_bt_strategy(
    stn: str,
    bars: list[dict],
    prices: dict[str, float],
) -> list[list]:
    """Compute and prepare rows for a single BT strategy. Safe to run in parallel."""
    rows = compute_bt_pnl(bars, prices)
    _prepare_rows_for_clickhouse(rows)
    return rows


def _process_underlying_bt(
    underlying: str,
    target_table: str,
    source_table: str,
    default_window_start: datetime,
    end_dt: datetime,
    log_fn=None,
) -> tuple[int, datetime]:
    """BT recompute for one underlying: parallel per-strategy, single bulk insert.

    Since BT bars are independent (no cross-bar anchor chaining), each strategy
    can be computed concurrently. All rows are bulk-inserted in one call per underlying.
    """
    _emit = log_fn or _log.info
    client = get_client()

    resume_dt = _get_underlying_resume_dt(underlying, target_table, client)
    window_start = resume_dt if resume_dt is not None else default_window_start
    window_start = window_start.replace(tzinfo=None)
    end_dt = end_dt.replace(tzinfo=None)
    window_start_ts = window_start.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    _emit(f"[{underlying}] bt window_start={window_start_ts}")

    source_stns = _get_source_strategies(
        source_table, underlying, window_start_ts, end_ts, client
    )
    _emit(
        f"[{underlying}] bt source has {len(source_stns)} strategies in [{window_start_ts}, {end_ts})"
    )

    execute(
        f"DELETE FROM analytics.{target_table}"
        f" WHERE underlying = '{underlying}' AND ts >= toDateTime('{window_start_ts}')",
        client=client,
    )
    _emit(f"[{underlying}] bt deleted rows >= {window_start_ts}")

    all_bars = fetch_new_bars_bt(
        source_table, underlying, window_start_ts, end_ts, client
    )
    if not all_bars:
        _emit(f"[{underlying}] bt no bars in window — skipping")
        return 0, window_start

    prices_map = fetch_prices_multi(
        [underlying], window_start_ts, end_ts, client, extend_minutes=1440
    )
    prices = prices_map.pop(underlying, {})

    # Split bars by strategy for parallel computation.
    by_strategy: dict[str, list[dict]] = {}
    for bar in all_bars:
        by_strategy.setdefault(bar["strategy_table_name"], []).append(bar)

    _emit(f"[{underlying}] bt computing {len(by_strategy)} strategies in parallel")
    all_rows: list[list] = []
    with ThreadPoolExecutor(max_workers=_BT_STRATEGY_WORKERS) as pool:
        futures = {
            pool.submit(_compute_bt_strategy, stn, bars, prices): stn
            for stn, bars in by_strategy.items()
        }
        for future in as_completed(futures):
            all_rows.extend(future.result())

    total_rows = 0
    if all_rows:
        total_rows = insert_rows(
            f"analytics.{target_table}", INSERT_COLUMNS, all_rows, client
        )

    _check_output_completeness(
        target_table, underlying, window_start_ts, end_ts, source_stns, client, _emit
    )
    _emit(f"[{underlying}] bt complete: {total_rows:,} rows")
    return total_rows, window_start


def _recompute_bt_recent(
    context: AssetExecutionContext,
    target_table: str,
    source_table: str,
    max_workers: int = _MAX_WORKERS,
) -> MaterializeResult:
    end_dt = datetime.now(tz=UTC)
    # BT cold start: only fetch the last _CHUNK_DAYS of bars, not all history.
    # Each bar carries cumulative_pnl in row_json so any window start is valid.
    default_window_start = (end_dt - timedelta(days=_CHUNK_DAYS)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    underlyings = _get_underlyings(source_table)
    context.log.info(f"BT recompute: {len(underlyings)} underlyings")

    total_rows = 0
    earliest_window_start: datetime | None = None
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _process_underlying_bt,
                u,
                target_table,
                source_table,
                default_window_start,
                end_dt,
                context.log.info,
            ): u
            for u in underlyings
        }
        for future in as_completed(futures):
            rows, ws = future.result()
            total_rows += rows
            if earliest_window_start is None or ws < earliest_window_start:
                earliest_window_start = ws

    hour_table = _HOUR_TABLE.get(target_table)
    if hour_table and earliest_window_start is not None:
        ws_str = earliest_window_start.strftime("%Y-%m-%d %H:%M:%S")
        context.log.info(f"Refreshing {hour_table} from {ws_str}")
        _refresh_hour_table(target_table, hour_table, ws_str, context.log.info)

    context.log.info(f"BT recompute complete: {total_rows:,} rows")
    return MaterializeResult(
        metadata={
            "rows_inserted": total_rows,
            "end_ts": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# Asset drivers
# ─────────────────────────────────────────────────────────────────────────────


def _get_source_min_ts(source_table: str) -> datetime | None:
    """Return min(ts) across all underlyings in source_table, or None if empty."""
    result = query_scalar(
        f"SELECT min(ts) FROM analytics.{source_table}"
        f" WHERE strategy_table_name NOT LIKE 'manual_probe%'"
    )
    if result is None:
        return None
    if isinstance(result, str):
        result = _parse_ts(result)
    result = result.replace(tzinfo=UTC)
    if result.year < 2000:
        return None
    return result.replace(hour=0, minute=0, second=0, microsecond=0)


def _recompute_pnl_recent(
    context: AssetExecutionContext,
    target_table: str,
    source_table: str,
    label: str,
    mode: str,
    ecs_service: str,
    ecs_resume_count: int = 1,
    max_workers: int = _MAX_WORKERS,
) -> MaterializeResult:
    end_dt = datetime.now(tz=UTC)
    # When the target table is empty (cold start / full wipe), fall back to
    # min(ts) from the source so we rebuild the full history, not just 7 days.
    source_min_ts = _get_source_min_ts(source_table)
    default_window_start = source_min_ts if source_min_ts is not None else datetime.strptime(
        PROD_REAL_TRADE_START_DATE, "%Y-%m-%d"
    ).replace(tzinfo=UTC)
    underlyings = _get_underlyings(source_table)
    context.log.info(
        f"Recent recompute {label}: {len(underlyings)} underlyings, ecs={ecs_service}"
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
                    u,
                    target_table,
                    source_table,
                    label,
                    mode,
                    default_window_start,
                    end_dt,
                    context.log.info,
                ): u
                for u in underlyings
            }
            for future in as_completed(futures):
                rows, ws = future.result()
                total_rows += rows
                if earliest_window_start is None or ws < earliest_window_start:
                    earliest_window_start = ws

        hour_table = _HOUR_TABLE.get(target_table)
        if hour_table and earliest_window_start is not None:
            ws_str = earliest_window_start.strftime("%Y-%m-%d %H:%M:%S")
            context.log.info(f"Refreshing {hour_table} from {ws_str}")
            _refresh_hour_table(target_table, hour_table, ws_str, context.log.info)
    finally:
        try:
            _resume_ecs_service(
                ecs_service, _ECS_CLUSTER, ecs, desired_count=ecs_resume_count
            )
            context.log.info(
                f"Resumed ECS service {ecs_service} (desiredCount={ecs_resume_count})"
            )
        except Exception as e:
            context.log.error(f"Failed to resume ECS service {ecs_service}: {e}")

    context.log.info(f"Recent recompute {label} complete: {total_rows:,} rows")
    return MaterializeResult(
        metadata={
            "rows_inserted": total_rows,
            "end_ts": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# Dagster assets
# ─────────────────────────────────────────────────────────────────────────────


@asset(
    name="pnl_prod_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_prod_v2_full"},
)
def pnl_prod_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Delete last chunk and recompute prod PnL from anchors, pausing the prod consumer."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_prod_v2",
        source_table="strategy_output_history_v2",
        label="production",
        mode="prod",
        ecs_service="trading-analysis-pnl-consumer-prod",
        ecs_resume_count=0,
    )


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
    """Delete last chunk and recompute real_trade PnL from anchors, pausing the real-trade consumer."""
    return _recompute_pnl_recent(
        context,
        target_table="strategy_pnl_1min_real_trade_v2",
        source_table="strategy_output_history_v2",
        label="real_trade",
        mode="real_trade",
        ecs_service="trading-analysis-pnl-consumer-real-trade",
        max_workers=1,
        ecs_resume_count=0,
    )


@asset(
    name="pnl_bt_v2_full",
    group_name="strategy_pnl",
    deps=["binance_futures_backfill"],
    compute_kind="clickhouse",
    op_tags={"dagster/timeout": 86400, "dagster/concurrency_limit": "pnl_bt_v2_full"},
)
def pnl_bt_v2_full_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Recompute bt PnL in parallel per strategy — no anchor chaining, no ECS pause needed."""
    return _recompute_bt_recent(
        context,
        target_table="strategy_pnl_1min_bt_v2",
        source_table="strategy_output_history_bt_v2",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Hourly rollup asset + schedule
# ─────────────────────────────────────────────────────────────────────────────

_ROLLUP_PAIRS = [
    ("strategy_pnl_1min_prod_v2", "strategy_pnl_1hour_prod_v2"),
    ("strategy_pnl_1min_real_trade_v2", "strategy_pnl_1hour_real_trade_v2"),
    ("strategy_pnl_1min_bt_v2", "strategy_pnl_1hour_bt_v2"),
]


_ROLLUP_LOOKBACK_HOURS = 6


def _rollup_recent_hours(min_table: str, hour_table: str, log_fn=None) -> str | None:
    """Re-aggregate the last 6 hours from a 1-min table into the 1-hour table.

    Uses max(ts) - 6h as the window start. Re-inserting existing hour slots is
    safe because the 1-hour tables use ReplacingMergeTree(updated_at) — duplicate
    rows are deduplicated on merge.  The 6-hour overlap ensures any partially
    written slot from a previous run gets corrected.

    Returns the window_start string used, or None if the table is empty.
    """
    _emit = log_fn or _log.info
    client = get_client()

    max_ts = query_scalar(
        f"SELECT max(ts) FROM analytics.{min_table}", client=client
    )
    if max_ts is None:
        _emit(f"[hourly_rollup] {min_table} is empty — skipping")
        return None

    if isinstance(max_ts, str):
        max_ts = datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")
    max_ts = max_ts.replace(tzinfo=None)

    window_start = max_ts - timedelta(hours=_ROLLUP_LOOKBACK_HOURS)
    window_start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
    _emit(f"[hourly_rollup] {hour_table}: window [{window_start_str}, {max_ts}]")

    execute(
        f"DELETE FROM analytics.{hour_table}"
        f" WHERE ts >= toStartOfHour(toDateTime('{window_start_str}'))",
        client=client,
    )
    execute(
        f"""\
INSERT INTO analytics.{hour_table}
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version,
    toStartOfHour(ts)                  AS ts,
    argMax(cumulative_pnl, ts)         AS cumulative_pnl,
    argMax(benchmark, ts)              AS benchmark,
    argMax(position, ts)               AS position,
    argMax(price, ts)                  AS price,
    argMax(final_signal, ts)           AS final_signal,
    argMax(weighting, ts)              AS weighting,
    now()                              AS updated_at,
    any(strategy_instance_id)          AS strategy_instance_id
FROM (
    SELECT *
    FROM analytics.{min_table}
    WHERE ts >= toStartOfHour(toDateTime('{window_start_str}'))
    ORDER BY strategy_table_name, strategy_id, strategy_name, underlying,
             config_timeframe, source, version, ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying,
               config_timeframe, source, version, ts
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfHour(ts)
""",
        client=client,
    )
    _emit(f"[hourly_rollup] {hour_table}: done")
    return window_start_str


@asset(
    name="pnl_hourly_rollup",
    group_name="strategy_pnl",
    compute_kind="clickhouse",
    description="Re-aggregate the last completed hour from each _1min_ PnL table into the corresponding _1hour_ table.",
)
def pnl_hourly_rollup_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Re-aggregate the last 6 hours from each _1min_ PnL table into _1hour_ tables."""
    windows: dict[str, str] = {}
    for min_table, hour_table in _ROLLUP_PAIRS:
        window_start = _rollup_recent_hours(min_table, hour_table, context.log.info)
        if window_start:
            windows[hour_table] = window_start
    return MaterializeResult(metadata=windows)


pnl_hourly_rollup_job = define_asset_job(
    name="pnl_hourly_rollup_job",
    selection=["pnl_hourly_rollup"],
)

pnl_hourly_rollup_schedule = ScheduleDefinition(
    name="pnl_hourly_rollup_schedule",
    cron_schedule="5 * * * *",  # 5 minutes past each hour — candles for :59 will be flushed
    job=pnl_hourly_rollup_job,
    execution_timezone="UTC",
)
