"""Window-scoped real_trade PnL backfill for the carry-forward metadata-wipe gap.

Detects strategies whose RT row count inside [--window-start, --window-end] is
materially below the expected per-minute coverage (one row per minute per active
strategy), then recomputes their PnL for that window only.

Use this to repair the 2026-05-26 → 2026-05-27 gap caused by the bootstrap walk
wiping AnchorRecord metadata (strategy_instance_id / underlying / ...), which
made the live consumer drop carry-forward rows.

The PnL chain is seeded from the last RT row immediately before the window:
that row's (cumulative_pnl, price, position) becomes the anchor, then we walk
minute-by-minute through the window using build_rt_lookup + active_rt_revision_at.
Existing rows OUTSIDE [window_start, window_end] are NOT touched — only the
gap window is recomputed and overwritten.

Pauses trading-analysis-pnl-consumer-real-trade before writing, resumes after.

Usage:
    python scripts/backfill_rt_window_gap.py \\
        --window-start '2026-05-26 01:00:00' \\
        --window-end   '2026-05-27 23:30:00' \\
        [--dry-run] [--threshold 0.9] [--no-pause]
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import clickhouse_connect

from libs.computation import (
    INSERT_COLUMNS,
    AnchorRecord,
    AnchorState,
    active_rt_revision_at,
    build_pnl_row,
    build_rt_lookup,
    fetch_new_bars_real_trade,
    fetch_prices_multi,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE_TABLE = "strategy_output_history_v2"
TARGET_TABLE = "strategy_pnl_1min_real_trade_v2"
HOUR_TABLE = "strategy_pnl_1hour_real_trade_v2"
LABEL = "real_trade"
ECS_CLUSTER = "trading-analysis"
ECS_SERVICE = "trading-analysis-pnl-consumer-real-trade"
ECS_REGION = "ap-northeast-1"
BATCH_SIZE = 50_000


def _parse_ts(s) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def _parse_ts_utc(s) -> datetime:
    # UTC-aware variant for values that will be inserted into ClickHouse DateTime
    # columns. The table stores naive UTC values; running from a non-UTC host
    # (e.g. a laptop in PDT) with naive datetimes silently shifts every row by
    # the host's UTC offset because clickhouse-connect treats naive as local.
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
    )


def find_window_gap_strategies(
    client,
    window_start: str,
    window_end: str,
    threshold: float,
) -> dict[str, list[str]]:
    """Return {underlying: [strategy_table_name, ...]} for strategies missing
    rows in [window_start, window_end].

    A strategy is "affected" when its actual RT row count in the window is below
    ``threshold`` * window_minutes. The default threshold (0.9) catches
    strategies missing >10% of expected minute coverage in the window without
    flagging strategies that just happen to be naturally inactive at the edges.
    """
    sql = f"""
WITH expected AS (
    SELECT dateDiff('minute',
        toDateTime('{window_start}'),
        toDateTime('{window_end}')
    ) AS minutes
),
src AS (
    -- Strategies active in source. 7-day lookback ensures 1d-timeframe strategies
    -- (which emit one bar/day and may have last bar >24h ago) are included.
    SELECT DISTINCT strategy_table_name, any(underlying) AS underlying
    FROM analytics.{SOURCE_TABLE}
    WHERE ts >= toDateTime('{window_start}') - INTERVAL 7 DAY
      AND ts <  toDateTime('{window_end}')
      AND strategy_table_name NOT LIKE 'manual_probe%'
    GROUP BY strategy_table_name
),
rt AS (
    SELECT strategy_table_name, count() AS n_rows
    FROM analytics.{TARGET_TABLE}
    WHERE ts >= toDateTime('{window_start}')
      AND ts <  toDateTime('{window_end}')
    GROUP BY strategy_table_name
)
SELECT
    src.strategy_table_name AS strategy_table_name,
    src.underlying          AS underlying,
    ifNull(rt.n_rows, 0)    AS n_rt_rows,
    expected.minutes        AS expected
FROM src
LEFT JOIN rt USING (strategy_table_name)
CROSS JOIN expected
WHERE ifNull(rt.n_rows, 0) < expected.minutes * {threshold}
ORDER BY src.underlying, src.strategy_table_name
"""
    rows = client.query(sql).result_rows
    by_underlying: dict[str, list[str]] = {}
    for stn, und, n_rt, n_exp in rows:
        by_underlying.setdefault(und, []).append(stn)
        log.debug(f"  affected: {und} {stn} has {n_rt}/{n_exp} rows in window")
    return by_underlying


def fetch_window_seed_anchors(
    client,
    stns: list[str],
    window_start: str,
) -> dict[str, dict]:
    """Return {strategy_table_name: {pnl, price, position}} from the last RT row
    just before ``window_start``. Missing strategies start from a zero anchor.

    Position is read from the RT table (not history) so the seed matches what
    the chain was at the boundary; the consumer normally uses
    strategy_output_history_v2 for live position lookup but for window-seed we
    must use the value that was persisted in the table at boundary, not the
    latest revision (which may have arrived during the gap).
    """
    if not stns:
        return {}
    stn_list = ", ".join(f"'{s}'" for s in stns)
    sql = f"""
SELECT
    strategy_table_name,
    argMax(cumulative_pnl, (ts, updated_at)) AS pnl,
    argMax(price,          (ts, updated_at)) AS price,
    argMax(position,       (ts, updated_at)) AS position
FROM analytics.{TARGET_TABLE}
WHERE strategy_table_name IN ({stn_list})
  AND ts < toDateTime('{window_start}')
  AND ts >= toDateTime('{window_start}') - INTERVAL 7 DAY
GROUP BY strategy_table_name
"""
    out: dict[str, dict] = {}
    for row in client.query(sql).result_rows:
        stn, pnl, price, position = row
        out[stn] = {"pnl": float(pnl or 0.0), "price": float(price), "position": float(position)}
    return out


def prepare_rows(rows: list[list]) -> list[list]:
    for r in rows:
        if isinstance(r[7], str):
            r[7] = _parse_ts_utc(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts_utc(r[14])
    return rows


def backfill_underlying(
    underlying: str,
    stns: list[str],
    window_start: str,
    window_end: str,
    dry_run: bool,
    client,
) -> int:
    log.info(f"[{underlying}] backfilling window for {len(stns)} strategies")

    # Source bars: extend lookback so 1d-bar revisions whose execution_ts lands
    # inside the window are still discovered. fetch_new_bars_real_trade already
    # extends back by 1440 minutes; we add another day for safety.
    bar_ts_start = (
        datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S") - timedelta(days=2)
    ).strftime("%Y-%m-%d %H:%M:%S")

    all_bars = fetch_new_bars_real_trade(SOURCE_TABLE, underlying, bar_ts_start, window_end, client)
    stn_set = set(stns)
    all_bars = [b for b in all_bars if b["strategy_table_name"] in stn_set]
    if not all_bars:
        log.info(f"[{underlying}] no bars found in window — skipping")
        return 0

    # Prices for the window
    prices_map = fetch_prices_multi(
        [underlying], window_start, window_end, client, extend_minutes=0
    )
    prices = prices_map.pop(underlying, {})

    lookup = build_rt_lookup(all_bars)

    # Seed AnchorState from the last RT row before window_start
    seeds = fetch_window_seed_anchors(client, list(stn_set), window_start)
    state = AnchorState()
    for stn in stn_set:
        seed = seeds.get(stn, {"pnl": 0.0, "price": 0.0, "position": 0.0})
        state.set(
            stn,
            AnchorRecord(
                pnl=seed["pnl"],
                price=seed["price"],
                position=seed["position"],
            ),
        )

    minute_start = _parse_ts(window_start)
    minute_end = _parse_ts(window_end)

    if not dry_run:
        stn_list = ", ".join(f"'{s}'" for s in stn_set)
        # ALTER TABLE...DELETE (heavyweight mutation) — uses ALTER DELETE grant.
        # Lightweight DELETE FROM requires ALTER UPDATE(_row_exists) which dagster lacks.
        client.command(
            f"ALTER TABLE analytics.{TARGET_TABLE} DELETE "
            f"WHERE strategy_table_name IN ({stn_list}) "
            f"  AND ts >= toDateTime('{window_start}') "
            f"  AND ts <  toDateTime('{window_end}') "
            f"SETTINGS mutations_sync = 2"
        )
        log.info(f"[{underlying}] deleted existing window rows for {len(stn_set)} strategies")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0
    batch: list[list] = []
    minute_cur = minute_start

    while minute_cur < minute_end:
        ts_str = minute_cur.strftime("%Y-%m-%d %H:%M:%S")
        price = prices.get(ts_str)
        if price is None:
            minute_cur += timedelta(minutes=1)
            continue

        for stn in stn_set:
            entry = active_rt_revision_at(lookup, stn, minute_cur)
            if entry is None:
                # No revision active at this minute. Hold seed-position via carry-forward
                # only if the seed was non-empty; otherwise skip (strategy not yet started).
                rec = state.get(stn)
                if rec.position == 0.0 and rec.pnl == 0.0 and rec.price == 0.0:
                    continue
                # Carry-forward with current position; no meta change.
                cpnl = state.compute_pnl(stn, price, rec.position)
                batch.append(
                    build_pnl_row(
                        stn,
                        {
                            "strategy_id": rec.strategy_id,
                            "strategy_name": rec.strategy_name,
                            "underlying": rec.underlying or underlying,
                            "config_timeframe": rec.config_timeframe,
                            "weighting": rec.weighting,
                            "strategy_instance_id": rec.strategy_instance_id,
                            "final_signal": rec.final_signal,
                            "bar_benchmark": rec.benchmark,
                            "position": rec.position,
                        },
                        price,
                        cpnl,
                        LABEL,
                        ts_str,
                        now_str,
                    )
                )
                continue

            rev = entry.rev
            meta = AnchorRecord(
                strategy_id=rev["strategy_id"],
                strategy_name=rev["strategy_name"],
                underlying=rev["underlying"],
                config_timeframe=rev["config_timeframe"],
                weighting=rev["weighting"],
                strategy_instance_id=rev.get("strategy_instance_id", ""),
                final_signal=rev["final_signal"],
                benchmark=rev["bar_benchmark"],
            )
            cpnl = state.compute_pnl(stn, price, rev["position"], meta=meta)
            batch.append(build_pnl_row(stn, rev, price, cpnl, LABEL, ts_str, now_str))

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


def refresh_hour_table(window_start: str, window_end: str, client) -> None:
    log.info(f"Refreshing {HOUR_TABLE} for [{window_start}, {window_end})")
    client.command(
        f"ALTER TABLE analytics.{HOUR_TABLE} DELETE "
        f"WHERE ts >= toStartOfHour(toDateTime('{window_start}')) "
        f"  AND ts <  toStartOfHour(toDateTime('{window_end}')) + INTERVAL 1 HOUR "
        f"SETTINGS mutations_sync = 2"
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
    WHERE ts >= toStartOfHour(toDateTime('{window_start}'))
      AND ts <  toStartOfHour(toDateTime('{window_end}')) + INTERVAL 1 HOUR
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
    profile = os.environ.get("AWS_PROFILE", "AdministratorAccess-068704208855")
    session = boto3.Session(profile_name=profile)
    return session.client("ecs", region_name=ECS_REGION)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--window-start",
        required=True,
        help="Inclusive window start, format 'YYYY-MM-DD HH:MM:SS'",
    )
    parser.add_argument(
        "--window-end",
        required=True,
        help="Exclusive window end, format 'YYYY-MM-DD HH:MM:SS'",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.9,
        help="Strategies with rt_row_count / window_minutes below this are recomputed (default 0.9)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not insert/delete")
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Skip pause/resume of the RT consumer (use when consumer is already stopped)",
    )
    args = parser.parse_args()

    # Validate format
    _parse_ts(args.window_start)
    _parse_ts(args.window_end)

    client = get_client()

    gap_strategies = find_window_gap_strategies(
        client, args.window_start, args.window_end, args.threshold
    )
    if not gap_strategies:
        log.info("No strategies with window gaps found — nothing to do")
        return

    total_stns = sum(len(v) for v in gap_strategies.values())
    log.info(
        f"Found {total_stns} affected strategies across {len(gap_strategies)} underlyings"
    )
    for und, stns in sorted(gap_strategies.items()):
        log.info(f"  [{und}] {len(stns)} strategies")

    ecs = get_boto_client() if not args.no_pause and not args.dry_run else None
    if ecs:
        pause_consumer(ecs)

    try:
        grand_total = 0
        for underlying, stns in sorted(gap_strategies.items()):
            rows = backfill_underlying(
                underlying, stns, args.window_start, args.window_end, args.dry_run, client
            )
            grand_total += rows

        log.info(f"Total rows {'computed' if args.dry_run else 'inserted'}: {grand_total:,}")

        if not args.dry_run:
            refresh_hour_table(args.window_start, args.window_end, client)
    finally:
        if ecs:
            resume_consumer(ecs)

    log.info("Backfill complete")


if __name__ == "__main__":
    main()
