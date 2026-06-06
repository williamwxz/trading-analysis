"""Backfill missing minutes in analytics.futures_price_1min from Binance Futures.

For each instrument, detects missing minutes in the scan window, fetches them
from Binance Futures via ccxt, and inserts via raw `INSERT VALUES` (the
clickhouse-connect bulk-insert API silently dropped rows on the Cloud's
SharedReplacingMergeTree during operational testing — root cause TBD, raw SQL
is the reliable workaround).

Runs daily as an AWS Lambda (image-package) triggered by EventBridge Rule. The
Lambda is VPC-attached and egresses through the same fck-nat EIP as the
streaming consumer so Binance sees a stable IP. Tokyo region — Binance Futures
geo-blocks US IPs.

Env vars (all read inside functions, not captured at module load, so warm
Lambda containers pick up per-invocation overrides set by handler.py):
    CLICKHOUSE_HOST/PORT/USER/PASSWORD/SECURE
    LOOKBACK_HOURS         (default 48; ignored if WINDOW_START is set)
    WINDOW_START           (optional ISO ts, UTC; overrides LOOKBACK_HOURS)
    WINDOW_END             (optional ISO ts, UTC; defaults to "now-1min")
    INSTRUMENTS            (comma-separated; default matches streaming consumer)
    INSERT_BATCH_SIZE      (default 200 rows per INSERT VALUES statement)

Ad-hoc historical backfill (Lambda invoke with event payload):
    aws lambda invoke --function-name trading-analysis-backfill-prices \\
        --payload '{"window_start":"2026-06-04","window_end":"2026-06-06"}' \\
        --cli-binary-format raw-in-base64-out /dev/stdout

Empty event → daily rolling lookback (what the EventBridge Rule sends).
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

# ── constants (non-env; safe to capture at module load) ─────────────────────

DEFAULT_INSTRUMENTS = (
    "BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT,AVAXUSDT,DOGEUSDT,XRPUSDT,FETUSDT"
)
TABLE = "analytics.futures_price_1min"
EXCHANGE_LABEL = "binance"  # written into the `exchange` column for parity with streaming

# ccxt fetch tuning
FETCH_LIMIT = 1000      # Binance Futures returns up to 1000 1m klines per call
FETCH_RETRIES = 3
FETCH_SLEEP_SECS = 0.25  # between paginated calls; well under rate limit


# ── env accessors (re-read per call so warm Lambdas pick up overrides) ──────


def _env_instruments() -> list[str]:
    raw = os.environ.get("INSTRUMENTS", DEFAULT_INSTRUMENTS)
    return [s.strip() for s in raw.split(",") if s.strip()]


def _env_lookback_hours() -> int:
    return int(os.environ.get("LOOKBACK_HOURS", "48"))


def _env_insert_batch_size() -> int:
    return int(os.environ.get("INSERT_BATCH_SIZE", "200"))


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


def _parse_iso(s: str) -> datetime:
    """Accept 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', or 'YYYY-MM-DDTHH:MM:SS[Z]'."""
    s = s.replace("T", " ").rstrip("Z").strip()
    if len(s) == 10:  # date only
        s += " 00:00:00"
    return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")


def resolve_window() -> tuple[datetime, datetime]:
    """Resolve the [start, end) scan window. WINDOW_START/END env vars take
    precedence — both naive UTC. Without them, falls back to the rolling
    LOOKBACK_HOURS ending one minute before now."""
    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    default_end = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    ws_env = os.environ.get("WINDOW_START", "").strip()
    we_env = os.environ.get("WINDOW_END", "").strip()
    if ws_env:
        window_start = _parse_iso(ws_env)
        window_end = _parse_iso(we_env) if we_env else default_end
    else:
        window_end = _parse_iso(we_env) if we_env else default_end
        window_start = window_end - timedelta(hours=_env_lookback_hours())
    if window_start >= window_end:
        raise ValueError(
            f"WINDOW_START ({window_start}) must be < WINDOW_END ({window_end})"
        )
    return window_start, window_end


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
    client, instrument: str, rows: list[tuple], batch_size: int | None = None
) -> int:
    """INSERT via raw VALUES SQL (bulk-insert API silently drops on this Cloud).

    Returns inserted row count.
    """
    if not rows:
        return 0
    if batch_size is None:
        batch_size = _env_insert_batch_size()
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
    instruments = _env_instruments()
    window_start, window_end = resolve_window()
    mode = (
        "explicit-window"
        if os.environ.get("WINDOW_START") or os.environ.get("WINDOW_END")
        else "rolling-lookback"
    )
    log.info(
        "starting backfill (%s): instruments=%s window=[%s, %s)",
        mode, instruments, window_start, window_end,
    )

    client = get_ch_client()
    exchange = get_ccxt_exchange()

    grand_total = 0
    failures: list[str] = []
    for instrument in instruments:
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
