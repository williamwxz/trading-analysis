"""
Binance Futures OHLCV 1-min Polling Asset

Polls Binance Futures API for 1-minute klines and inserts into
analytics.futures_price_1min in ClickHouse Cloud.

Replaces the Amber Data → Redpanda → Kafka MV pipeline from falcon-lakehouse
with simple REST API polling — minimal infrastructure, same result.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import List

import requests
from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..utils.clickhouse_client import get_client, insert_rows, query_scalar

BINANCE_FAPI = "https://fapi.binance.com"

# Instruments to poll — all USDT perpetual futures used by strategies
INSTRUMENTS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

INSERT_COLUMNS = ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"]


def _fetch_klines(instrument: str, start_ms: int, limit: int = 1000) -> List[list]:
    """Fetch 1m klines from Binance Futures API."""
    resp = requests.get(
        f"{BINANCE_FAPI}/fapi/v1/klines",
        params={
            "symbol": instrument,
            "interval": "1m",
            "startTime": start_ms,
            "limit": limit,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _klines_to_rows(instrument: str, klines: list) -> List[list]:
    """Convert Binance kline response to ClickHouse rows."""
    rows = []
    for k in klines:
        ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
        rows.append([
            "binance",
            instrument,
            ts_str,
            float(k[1]),  # open
            float(k[2]),  # high
            float(k[3]),  # low
            float(k[4]),  # close
            float(k[5]),  # volume
        ])
    return rows


@asset(
    name="binance_futures_ohlcv_1min",
    group_name="market_data",
    automation_condition=(
        AutomationCondition.on_cron("*/2 * * * *") & ~AutomationCondition.in_progress()
    ),
    description=(
        "Polls Binance Futures API for 1-min OHLCV klines and inserts into "
        "analytics.futures_price_1min. Incremental — starts from last known ts per instrument."
    ),
    compute_kind="binance_api",
)
def binance_futures_ohlcv_1min_asset(
    context: AssetExecutionContext,
) -> MaterializeResult:
    client = get_client()
    total_inserted = 0
    instruments_updated = []

    for instrument in INSTRUMENTS:
        # Find last known timestamp
        last_ts = query_scalar(
            f"SELECT max(ts) FROM analytics.futures_price_1min "
            f"WHERE exchange = 'binance' AND instrument = '{instrument}'",
            client,
        )

        if last_ts and str(last_ts) != "1970-01-01 00:00:00":
            start_ms = int(last_ts.timestamp() * 1000) + 60_000  # next minute
        else:
            # Bootstrap: start from 7 days ago
            start_ms = int((datetime.now(tz=timezone.utc) - timedelta(days=7)).timestamp() * 1000)

        context.log.info(f"[{instrument}] Polling from {datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)}")

        try:
            klines = _fetch_klines(instrument, start_ms, limit=1000)
            if not klines:
                context.log.info(f"[{instrument}] No new klines")
                continue

            rows = _klines_to_rows(instrument, klines)
            insert_rows("analytics.futures_price_1min", INSERT_COLUMNS, rows, client)
            total_inserted += len(rows)
            instruments_updated.append(f"{instrument}({len(rows)})")
            context.log.info(f"[{instrument}] Inserted {len(rows)} klines")
        except Exception as exc:
            context.log.warning(f"[{instrument}] Failed: {exc}")

        # Rate limit: 100ms between instruments
        time.sleep(0.1)

    return MaterializeResult(
        metadata={
            "total_inserted": MetadataValue.int(total_inserted),
            "instruments_updated": MetadataValue.text(", ".join(instruments_updated) or "none"),
        }
    )


__all__ = ["binance_futures_ohlcv_1min_asset"]
