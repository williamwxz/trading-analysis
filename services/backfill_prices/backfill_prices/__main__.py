"""Daily backfill for analytics.futures_price_1min.

For each instrument, detects missing minutes in the last LOOKBACK_HOURS,
fetches them from Binance Futures via ccxt, and inserts via raw `INSERT VALUES`
(the clickhouse-connect bulk-insert API silently dropped rows on the Cloud's
SharedReplacingMergeTree during operational testing — root cause TBD, raw SQL
is the reliable workaround).

Runs as a one-shot ECS Fargate task triggered daily by EventBridge Schedule.
Runs in ap-northeast-1 (Tokyo) so Binance Futures isn't geo-blocked.

Env vars (mostly injected by ECS task def):
    CLICKHOUSE_HOST/PORT/USER/PASSWORD/SECURE
    LOOKBACK_HOURS         (default 48)
    INSTRUMENTS            (comma-separated; default matches streaming consumer)
    INSERT_BATCH_SIZE      (default 200 rows per INSERT VALUES statement)
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import ccxt
import clickhouse_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── config ──────────────────────────────────────────────────────────────────

DEFAULT_INSTRUMENTS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT,AVAXUSDT,DOGEUSDT,XRPUSDT,FETUSDT"
)
INSTRUMENTS: list[str] = [
    s.strip() for s in os.environ.get("INSTRUMENTS", DEFAULT_INSTRUMENTS).split(",") if s.strip()
]
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "48"))
INSERT_BATCH_SIZE = int(os.environ.get("INSERT_BATCH_SIZE", "200"))

TABLE = "analytics.futures_price_1min"
EXCHANGE_LABEL = "binance"  # written into the `exchange` column for parity with streaming

# ccxt fetch tuning
FETCH_LIMIT = 1000      # Binance Futures returns up to 1000 1m klines per call
FETCH_RETRIES = 3
FETCH_SLEEP_SECS = 0.25  # between paginated calls; well under rate limit


# ── helpers ─────────────────────────────────────────────────────────────────


def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
    )


def get_ccxt_exchange() -> ccxt.binanceusdm:
    ex = ccxt.binanceusdm()
    ex.load_markets()
    return ex


def _instrument_to_symbol(instrument: str) -> str:
    """BTCUSDT → BTC/USDT:USDT (Binance USDⓈ-M futures symbol)."""
    assert instrument.endswith("USDT"), f"unexpected instrument: {instrument}"
    base = instrument[:-4]
    return f"{base}/USDT:USDT"


def find_missing_minutes(
    client, instrument: str, window_start: datetime, window_end: datetime
) -> list[datetime]:
    """Return the list of UTC minutes in [window_start, window_end) that have
    no row in `futures_price_1min` for this instrument."""
    sql = f"""
    WITH expected AS (
        SELECT arrayJoin(arrayMap(
            x -> addMinutes(toDateTime('{window_start:%Y-%m-%d %H:%M:%S}'), x),
            range(toUInt32(dateDiff('minute',
                toDateTime('{window_start:%Y-%m-%d %H:%M:%S}'),
                toDateTime('{window_end:%Y-%m-%d %H:%M:%S}'))))
        )) AS ts
    )
    SELECT e.ts FROM expected e
    LEFT ANTI JOIN (
        SELECT ts FROM {TABLE}
        WHERE instrument = '{instrument}'
          AND ts >= toDateTime('{window_start:%Y-%m-%d %H:%M:%S}')
          AND ts <  toDateTime('{window_end:%Y-%m-%d %H:%M:%S}')
    ) p ON e.ts = p.ts
    ORDER BY e.ts
    """
    return [row[0] for row in client.query(sql).result_rows]


def fetch_ohlcv_range(
    exchange: ccxt.binanceusdm, symbol: str, start: datetime, end: datetime
) -> list[tuple[datetime, float, float, float, float, float]]:
    """Fetch 1-min OHLCV from Binance Futures in [start, end). Paginated.

    Returns list of (ts, open, high, low, close, volume) tuples,
    ts as naive UTC datetime (matches ClickHouse DateTime semantics).
    """
    out: list[tuple] = []
    cur = start
    while cur < end:
        since_ms = int(cur.replace(tzinfo=timezone.utc).timestamp() * 1000)
        ohlcv: list = []
        for attempt in range(1, FETCH_RETRIES + 1):
            try:
                ohlcv = exchange.fetch_ohlcv(
                    symbol, "1m", since=since_ms, limit=FETCH_LIMIT
                )
                break
            except (ccxt.NetworkError, ccxt.ExchangeError) as exc:
                log.warning(
                    "fetch_ohlcv %s attempt %d/%d at %s failed: %s",
                    symbol, attempt, FETCH_RETRIES, cur, exc,
                )
                if attempt < FETCH_RETRIES:
                    time.sleep(2 ** attempt)
        if not ohlcv:
            # Skip the window we tried; advance by max page width so we don't loop.
            log.warning("no data returned for %s at %s; skipping page", symbol, cur)
            cur += timedelta(minutes=FETCH_LIMIT)
            continue
        for ts_ms, o, h, l, c, v in ohlcv:
            ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(
                tzinfo=None
            )
            if ts_dt >= end:
                break
            out.append((ts_dt, float(o), float(h), float(l), float(c), float(v)))
        last_ts = datetime.fromtimestamp(ohlcv[-1][0] / 1000, tz=timezone.utc).replace(
            tzinfo=None
        )
        next_cur = last_ts + timedelta(minutes=1)
        if next_cur <= cur:  # safety: no progress
            break
        cur = next_cur
        time.sleep(FETCH_SLEEP_SECS)
    return out


def insert_rows_raw_sql(
    client, instrument: str, rows: list[tuple], batch_size: int = INSERT_BATCH_SIZE
) -> int:
    """INSERT via raw VALUES SQL (bulk-insert API silently drops on this Cloud).

    Returns inserted row count.
    """
    if not rows:
        return 0
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = ",".join(
            f"('{EXCHANGE_LABEL}','{instrument}',toDateTime('{ts:%Y-%m-%d %H:%M:%S}'),{o},{h},{l},{c},{v})"
            for ts, o, h, l, c, v in batch
        )
        sql = (
            f"INSERT INTO {TABLE} "
            f"(exchange, instrument, ts, open, high, low, close, volume) "
            f"VALUES {values}"
        )
        # insert_deduplicate=0 prevents the engine from skipping us based on
        # block fingerprints from earlier failed attempts.
        client.command(sql, settings={"insert_deduplicate": 0})
        inserted += len(batch)
    return inserted


def backfill_instrument(client, exchange, instrument: str, window_start, window_end):
    """Detect and fill missing minutes for one instrument."""
    missing = find_missing_minutes(client, instrument, window_start, window_end)
    if not missing:
        log.info("[%s] no gaps", instrument)
        return 0
    log.info(
        "[%s] %d missing minutes (first=%s, last=%s)",
        instrument, len(missing), missing[0], missing[-1],
    )

    # Fetch a single contiguous range covering the gap span (cheaper than
    # one-call-per-gap-island when gaps cluster, which they usually do).
    fetch_start = missing[0]
    fetch_end = missing[-1] + timedelta(minutes=1)
    symbol = _instrument_to_symbol(instrument)
    fetched = fetch_ohlcv_range(exchange, symbol, fetch_start, fetch_end)
    log.info("[%s] fetched %d candles from Binance", instrument, len(fetched))

    # Filter to only the actually-missing minutes (we may have refetched some
    # already-present minutes; ReplacingMergeTree would dedupe, but skipping
    # them up front avoids wasted writes).
    missing_set = set(missing)
    to_insert = [row for row in fetched if row[0] in missing_set]
    inserted = insert_rows_raw_sql(client, instrument, to_insert)
    log.info("[%s] inserted %d rows", instrument, inserted)
    return inserted


# ── main ────────────────────────────────────────────────────────────────────


def main() -> int:
    log.info(
        "starting daily backfill: instruments=%s lookback_hours=%d",
        INSTRUMENTS, LOOKBACK_HOURS,
    )
    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    # Bound window_end to the previous full minute — partial-minute candles
    # from Binance can be ambiguous (close may revise once the minute closes).
    window_end = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    window_start = window_end - timedelta(hours=LOOKBACK_HOURS)
    log.info("scan window: [%s, %s)", window_start, window_end)

    client = get_ch_client()
    exchange = get_ccxt_exchange()

    grand_total = 0
    failures: list[str] = []
    for instrument in INSTRUMENTS:
        try:
            grand_total += backfill_instrument(
                client, exchange, instrument, window_start, window_end
            )
        except Exception as exc:  # noqa: BLE001 — log + continue, don't fail the whole run
            log.exception("[%s] backfill failed", instrument)
            failures.append(f"{instrument}: {type(exc).__name__}: {exc}")

    log.info("backfill complete: %d rows inserted total", grand_total)
    if failures:
        log.error("FAILURES (%d instruments):", len(failures))
        for f in failures:
            log.error("  %s", f)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
