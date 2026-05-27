"""Backfill prod PnL rows for strategies with missing or incomplete history.

Detects two failure modes:
  1. Start gaps: a strategy's first prod row is more than 2 hours after its
     first source execution_ts (closing_ts = ts + tf_minutes), or it is absent
     entirely from strategy_pnl_1min_prod_v2.
  2. Mid-history gaps: a strategy has prod rows before AND after some date range
     but has zero rows during that range.  Detected by comparing the count of
     distinct covered days to the expected number of days between the strategy's
     first and last prod row.

For both failure modes the fix is the same: DELETE all existing prod rows for
the affected strategy, then recompute its full history from scratch using
build_prod_lookup + active_prod_bar_at.

Pauses trading-analysis-pnl-consumer before writing, resumes in finally.

Usage:
    python scripts/backfill_prod_missing_strategies.py [--dry-run]
"""

import argparse
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

import boto3
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import clickhouse_connect

from libs.computation import (
    INSERT_COLUMNS,
    AnchorRecord,
    AnchorState,
    active_prod_bar_at,
    build_pnl_row,
    build_prod_lookup,
    fetch_new_bars_prod,
    fetch_prices_multi,
    first_active_minute,
    last_active_minute,
)
from libs.computation.minute_loop import TIMEFRAME_MAP

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE_TABLE = "strategy_output_history_v2"
TARGET_TABLE = "strategy_pnl_1min_prod_v2"
HOUR_TABLE = "strategy_pnl_1hour_prod_v2"
LABEL = "production"
ECS_CLUSTER = "trading-analysis"
ECS_SERVICE = "trading-analysis-pnl-consumer"
ECS_REGION = "ap-northeast-1"
BATCH_SIZE = 50_000


def _parse_ts(s) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
    )


def find_start_gap_strategies(client) -> dict[str, list[str]]:
    """Return {underlying: [strategy_table_name, ...]} for strategies with missing early history.

    A strategy is flagged if:
      - It exists in the source but has no rows in the prod table, OR
      - Its earliest prod row is more than 2 hours after its earliest source
        closing_ts (approximated as revision_ts + 59s, same proxy as RT script).
    """
    sql = """
    WITH
    src AS (
        SELECT
            strategy_table_name,
            underlying,
            min(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS first_exec_ts
        FROM analytics.strategy_output_history_v2
        WHERE strategy_table_name NOT LIKE 'manual_probe%'
        GROUP BY strategy_table_name, underlying
    ),
    prod AS (
        SELECT
            strategy_table_name,
            min(ts) AS first_prod_ts
        FROM analytics.strategy_pnl_1min_prod_v2
        GROUP BY strategy_table_name
    )
    SELECT src.strategy_table_name, src.underlying
    FROM src
    LEFT JOIN prod ON src.strategy_table_name = prod.strategy_table_name
    WHERE
        -- completely absent
        prod.strategy_table_name IS NULL
        -- gap at start: prod begins more than 2h after first execution
        OR prod.first_prod_ts > src.first_exec_ts + INTERVAL 2 HOUR
    ORDER BY src.underlying, src.strategy_table_name
    """
    rows = client.query(sql).result_rows
    by_underlying: dict[str, list[str]] = {}
    for stn, und in rows:
        by_underlying.setdefault(und, []).append(stn)
    return by_underlying


def find_midgap_strategies(client) -> dict[str, list[str]]:
    """Return {underlying: [strategy_table_name, ...]} for strategies with mid-history holes.

    A strategy has a mid-history gap if the count of distinct days it has prod
    rows is less than the number of calendar days between its first and last
    prod row.  This means at least one day in its prod coverage window has
    zero rows.
    """
    sql = """
    SELECT
        strategy_table_name,
        any(underlying) AS underlying
    FROM analytics.strategy_pnl_1min_prod_v2
    GROUP BY strategy_table_name
    HAVING
        count(DISTINCT toStartOfDay(ts))
        < dateDiff('day', min(ts), max(ts)) + 1
    ORDER BY underlying, strategy_table_name
    """
    rows = client.query(sql).result_rows
    by_underlying: dict[str, list[str]] = {}
    for stn, und in rows:
        by_underlying.setdefault(und, []).append(stn)
    return by_underlying


def get_ts_range(stns: list[str], client) -> tuple[str, str]:
    """Return (ts_start, ts_end) covering full history for the given strategies."""
    stn_list = ", ".join(f"'{s}'" for s in stns)
    sql = f"""
    SELECT
        toString(min(toStartOfMinute(revision_ts + INTERVAL 59 SECOND))),
        toString(now())
    FROM analytics.{SOURCE_TABLE}
    WHERE strategy_table_name IN ({stn_list})
      AND strategy_table_name NOT LIKE 'manual_probe%'
    """
    row = client.query(sql).result_rows[0]
    return str(row[0]), str(row[1])


def prepare_rows(rows: list[list]) -> list[list]:
    for r in rows:
        if isinstance(r[7], str):
            r[7] = _parse_ts(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts(r[14])
    return rows


def backfill_underlying(underlying: str, stns: list[str], dry_run: bool, client) -> int:
    log.info(f"[{underlying}] backfilling {len(stns)} strategies (full history)")

    ts_start, ts_end = get_ts_range(stns, client)
    log.info(f"[{underlying}] window: [{ts_start}, {ts_end}]")

    all_bars = fetch_new_bars_prod(SOURCE_TABLE, underlying, ts_start, ts_end, client)
    stn_set = set(stns)
    all_bars = [b for b in all_bars if b["strategy_table_name"] in stn_set]
    if not all_bars:
        log.info(f"[{underlying}] no bars found — skipping")
        return 0

    prices_map = fetch_prices_multi([underlying], ts_start, ts_end, client, extend_minutes=1440)
    prices = prices_map.pop(underlying, {})

    lookup = build_prod_lookup(all_bars)
    minute_start = first_active_minute(lookup, is_rt=False)
    minute_end = last_active_minute(lookup, is_rt=False)
    if minute_start is None or minute_end is None:
        log.info(f"[{underlying}] empty lookup — skipping")
        return 0

    log.info(
        f"[{underlying}] iterating "
        f"{int((minute_end - minute_start).total_seconds() // 60)} minutes"
    )

    # Delete all existing rows for these strategies so recompute is clean.
    if not dry_run:
        stn_list = ", ".join(f"'{s}'" for s in stn_set)
        client.command(
            f"DELETE FROM analytics.{TARGET_TABLE}"
            f" WHERE strategy_table_name IN ({stn_list})"
        )
        log.info(f"[{underlying}] deleted existing rows for {len(stn_set)} strategies")

    state = AnchorState()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0
    minute_cur = minute_start

    batch: list[list] = []

    while minute_cur < minute_end:
        ts_str = minute_cur.strftime("%Y-%m-%d %H:%M:%S")
        price = prices.get(ts_str)
        if price is None:
            minute_cur += timedelta(minutes=1)
            continue

        for stn in stn_set:
            entry = active_prod_bar_at(lookup, stn, minute_cur)
            if entry is None:
                continue
            bar = entry.bar
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
            batch.append(build_pnl_row(stn, bar, price, cpnl, LABEL, ts_str, now_str))

        if len(batch) >= BATCH_SIZE:
            if not dry_run:
                prepare_rows(batch)
                client.insert(f"analytics.{TARGET_TABLE}", batch, column_names=INSERT_COLUMNS)
            total_rows += len(batch)
            log.info(f"[{underlying}] flushed {total_rows:,} rows so far")
            batch = []

        minute_cur += timedelta(minutes=1)

    if batch:
        if not dry_run:
            prepare_rows(batch)
            client.insert(f"analytics.{TARGET_TABLE}", batch, column_names=INSERT_COLUMNS)
        total_rows += len(batch)

    log.info(f"[{underlying}] done: {total_rows:,} rows {'(dry-run)' if dry_run else 'inserted'}")
    return total_rows


def refresh_hour_table(ts_start: str, client) -> None:
    log.info(f"Refreshing {HOUR_TABLE} from {ts_start}")
    client.command(
        f"DELETE FROM analytics.{HOUR_TABLE}"
        f" WHERE ts >= toStartOfHour(toDateTime('{ts_start}'))"
    )
    client.command(f"""
INSERT INTO analytics.{HOUR_TABLE}
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
    FROM analytics.{TARGET_TABLE}
    WHERE ts >= toStartOfHour(toDateTime('{ts_start}'))
    ORDER BY strategy_table_name, strategy_id, strategy_name, underlying,
             config_timeframe, source, version, ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying,
               config_timeframe, source, version, ts
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfHour(minute_ts)
""")
    log.info("Hour table refresh done")


def pause_consumer(boto_client) -> None:
    log.info(f"Pausing {ECS_SERVICE}")
    boto_client.update_service(cluster=ECS_CLUSTER, service=ECS_SERVICE, desiredCount=0)
    boto_client.get_waiter("services_stable").wait(cluster=ECS_CLUSTER, services=[ECS_SERVICE])
    log.info(f"Paused {ECS_SERVICE}")


def resume_consumer(boto_client) -> None:
    log.info(f"Resuming {ECS_SERVICE}")
    boto_client.update_service(cluster=ECS_CLUSTER, service=ECS_SERVICE, desiredCount=1)
    log.info(f"Resumed {ECS_SERVICE}")


def get_boto_client():
    session = boto3.Session(profile_name="AdministratorAccess-068704208855")
    return session.client("ecs", region_name=ECS_REGION)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not insert")
    args = parser.parse_args()

    client = get_client()

    start_gap = find_start_gap_strategies(client)
    midgap = find_midgap_strategies(client)

    # Merge both maps; a strategy may appear in both — deduplicate per underlying.
    combined: dict[str, set[str]] = {}
    for und, stns in start_gap.items():
        combined.setdefault(und, set()).update(stns)
    for und, stns in midgap.items():
        combined.setdefault(und, set()).update(stns)

    if not combined:
        log.info("No strategies with gaps found — nothing to do")
        return

    gap_strategies = {und: sorted(stns) for und, stns in combined.items()}

    total_stns = sum(len(v) for v in gap_strategies.values())
    start_count = sum(len(v) for v in start_gap.values())
    mid_count = sum(len(v) for v in midgap.values())
    log.info(
        f"Found {total_stns} unique strategies with gaps across "
        f"{len(gap_strategies)} underlyings "
        f"({start_count} start-gap, {mid_count} mid-gap, may overlap)"
    )
    for und, stns in sorted(gap_strategies.items()):
        log.info(f"  [{und}] {len(stns)} strategies")

    ecs = get_boto_client()

    if not args.dry_run:
        pause_consumer(ecs)

    try:
        earliest_start: str | None = None
        grand_total = 0
        for underlying, stns in sorted(gap_strategies.items()):
            ts_start, _ = get_ts_range(stns, client)
            if earliest_start is None or ts_start < earliest_start:
                earliest_start = ts_start
            rows = backfill_underlying(underlying, stns, args.dry_run, client)
            grand_total += rows

        log.info(f"Total rows {'computed' if args.dry_run else 'inserted'}: {grand_total:,}")

        if not args.dry_run and earliest_start:
            refresh_hour_table(earliest_start, client)
    finally:
        if not args.dry_run:
            resume_consumer(ecs)

    log.info("Backfill complete")


if __name__ == "__main__":
    main()
