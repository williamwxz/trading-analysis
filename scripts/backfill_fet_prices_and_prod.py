"""Backfill FETUSDT 1-min prices then recompute FET prod PnL.

FETUSDT prices in futures_price_1min only start 2026-05-02; FET strategies
have been live since 2026-03-09. This script:
  1. Fetches FETUSDT 1-min OHLCV from Binance futures for the missing window.
  2. Inserts into analytics.futures_price_1min.
  3. Deletes all existing FET prod rows and recomputes full history.
  4. Refreshes strategy_pnl_1hour_prod_v2 for FET.

Pauses trading-analysis-pnl-consumer-prod before writing, resumes after.

Usage:
    python scripts/backfill_fet_prices_and_prod.py [--dry-run]
"""

import argparse
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta

import boto3
import ccxt
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE_TABLE = "strategy_output_history_v2"
TARGET_TABLE = "strategy_pnl_1min_prod_v2"
HOUR_TABLE = "strategy_pnl_1hour_prod_v2"
PRICE_TABLE = "futures_price_1min"
LABEL = "production"
UNDERLYING = "FET"
INSTRUMENT = "FETUSDT"
PRICE_FETCH_START = "2026-03-09 00:00:00"
ECS_CLUSTER = "trading-analysis"
ECS_SERVICE = "trading-analysis-pnl-consumer-prod"
ECS_REGION = "ap-northeast-1"
BATCH_SIZE = 50_000


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
    )


def _parse_ts(s) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def fetch_and_insert_prices(dry_run: bool, client) -> int:
    """Fetch FETUSDT 1-min prices from Binance and insert into futures_price_1min."""
    # Determine what's already present.
    row = client.query(
        f"SELECT min(ts), max(ts) FROM analytics.{PRICE_TABLE}"
        f" WHERE instrument = '{INSTRUMENT}'"
    ).result_rows[0]
    existing_min = row[0]
    existing_max = row[1]
    log.info(f"Existing {INSTRUMENT} prices: {existing_min} → {existing_max}")

    fetch_start = datetime.strptime(PRICE_FETCH_START, "%Y-%m-%d %H:%M:%S")
    # Only fetch the gap — stop just before what we already have.
    if existing_min is not None:
        fetch_end = existing_min - timedelta(minutes=1)
    else:
        fetch_end = datetime.utcnow()

    if fetch_start >= fetch_end:
        log.info("No price gap to fill.")
        return 0

    log.info(f"Fetching {INSTRUMENT} prices from {fetch_start} to {fetch_end}")

    exchange = ccxt.binanceusdm()
    exchange.load_markets()
    symbol = "FET/USDT:USDT"

    total_inserted = 0
    cur = fetch_start
    while cur <= fetch_end:
        since_ms = int(cur.replace(tzinfo=UTC).timestamp() * 1000)
        ohlcv = []
        for attempt in range(1, 4):
            try:
                ohlcv = exchange.fetch_ohlcv(symbol, "1m", since=since_ms, limit=1000)
                break
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                log.warning(f"Attempt {attempt}/3 failed: {e}")
                if attempt < 3:
                    time.sleep(5)
                else:
                    log.error(f"All retries failed at {cur}; skipping batch")
                    ohlcv = []

        if not ohlcv:
            cur += timedelta(minutes=1000)
            continue

        rows = []
        for ts_ms, open_, high, low, close, volume in ohlcv:
            ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).replace(tzinfo=None)
            if ts_dt > fetch_end:
                break
            rows.append(["binance", INSTRUMENT, ts_dt, open_, high, low, close, volume])

        if rows and not dry_run:
            client.insert(
                f"analytics.{PRICE_TABLE}",
                rows,
                column_names=["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"],
            )
        total_inserted += len(rows)

        last_ts = datetime.fromtimestamp(ohlcv[-1][0] / 1000, tz=UTC).replace(tzinfo=None)
        log.info(f"  fetched up to {last_ts}, total so far: {total_inserted:,}")
        cur = last_ts + timedelta(minutes=1)

        # Rate limit courtesy pause.
        time.sleep(0.2)

    log.info(f"Price backfill done: {total_inserted:,} rows {'(dry-run)' if dry_run else 'inserted'}")
    return total_inserted


def get_fet_strategies(client) -> list[str]:
    rows = client.query(
        f"SELECT DISTINCT strategy_table_name FROM analytics.{SOURCE_TABLE}"
        f" WHERE underlying = '{UNDERLYING}'"
        f" AND strategy_table_name NOT LIKE 'manual_probe%'"
    ).result_rows
    return [r[0] for r in rows]


def get_ts_range(stns: list[str], client) -> tuple[str, str]:
    stn_list = ", ".join(f"'{s}'" for s in stns)
    row = client.query(f"""
        SELECT
            toString(min(toStartOfMinute(revision_ts + INTERVAL 59 SECOND))),
            toString(now())
        FROM analytics.{SOURCE_TABLE}
        WHERE strategy_table_name IN ({stn_list})
          AND strategy_table_name NOT LIKE 'manual_probe%'
    """).result_rows[0]
    return str(row[0]), str(row[1])


def prepare_rows(rows: list[list]) -> list[list]:
    for r in rows:
        if isinstance(r[7], str):
            r[7] = _parse_ts(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts(r[14])
    return rows


def backfill_fet_prod(stns: list[str], dry_run: bool, client) -> int:
    log.info(f"[{UNDERLYING}] backfilling {len(stns)} strategies (full history)")

    ts_start, ts_end = get_ts_range(stns, client)
    log.info(f"[{UNDERLYING}] window: [{ts_start}, {ts_end}]")

    all_bars = fetch_new_bars_prod(SOURCE_TABLE, UNDERLYING, ts_start, ts_end, client)
    stn_set = set(stns)
    all_bars = [b for b in all_bars if b["strategy_table_name"] in stn_set]
    if not all_bars:
        log.info(f"[{UNDERLYING}] no bars found — skipping")
        return 0

    prices_map = fetch_prices_multi([UNDERLYING], ts_start, ts_end, client, extend_minutes=1440)
    prices = prices_map.pop(UNDERLYING, {})
    log.info(f"[{UNDERLYING}] price entries loaded: {len(prices):,}")

    lookup = build_prod_lookup(all_bars)
    minute_start = first_active_minute(lookup, is_rt=False)
    minute_end = last_active_minute(lookup, is_rt=False)
    if minute_start is None or minute_end is None:
        log.info(f"[{UNDERLYING}] empty lookup — skipping")
        return 0

    log.info(
        f"[{UNDERLYING}] iterating "
        f"{int((minute_end - minute_start).total_seconds() // 60)} minutes"
    )

    if not dry_run:
        stn_list = ", ".join(f"'{s}'" for s in stn_set)
        client.command(
            f"DELETE FROM analytics.{TARGET_TABLE}"
            f" WHERE strategy_table_name IN ({stn_list})"
        )
        log.info(f"[{UNDERLYING}] deleted existing rows for {len(stn_set)} strategies")

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
            log.info(f"[{UNDERLYING}] flushed {total_rows:,} rows so far")
            batch = []

        minute_cur += timedelta(minutes=1)

    if batch:
        if not dry_run:
            prepare_rows(batch)
            client.insert(f"analytics.{TARGET_TABLE}", batch, column_names=INSERT_COLUMNS)
        total_rows += len(batch)

    log.info(f"[{UNDERLYING}] done: {total_rows:,} rows {'(dry-run)' if dry_run else 'inserted'}")
    return total_rows


def refresh_hour_table(ts_start: str, client) -> None:
    log.info(f"Refreshing {HOUR_TABLE} for {UNDERLYING} from {ts_start}")
    client.command(
        f"DELETE FROM analytics.{HOUR_TABLE}"
        f" WHERE underlying = '{UNDERLYING}'"
        f"   AND ts >= toStartOfHour(toDateTime('{ts_start}'))"
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
    WHERE underlying = '{UNDERLYING}'
      AND ts >= toStartOfHour(toDateTime('{ts_start}'))
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
    stns = get_fet_strategies(client)
    log.info(f"Found {len(stns)} FET strategies in source")

    ecs = get_boto_client()

    if not args.dry_run:
        pause_consumer(ecs)

    try:
        # Step 1: fill price gap
        fetch_and_insert_prices(args.dry_run, client)

        # Reload client after price insert so fetch_prices_multi sees new rows.
        client2 = get_client()

        # Step 2: recompute FET prod PnL
        ts_start, _ = get_ts_range(stns, client2)
        rows = backfill_fet_prod(stns, args.dry_run, client2)
        log.info(f"Total rows {'computed' if args.dry_run else 'inserted'}: {rows:,}")

        if not args.dry_run:
            refresh_hour_table(ts_start, client2)
    finally:
        if not args.dry_run:
            resume_consumer(ecs)

    log.info("FET backfill complete")


if __name__ == "__main__":
    main()
