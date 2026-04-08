"""
Binance Futures OHLCV 1-min Polling Asset

Polls Binance Futures API via ccxt ExchangePriceDataService for 1-minute klines
and inserts into analytics.futures_price_1min in ClickHouse Cloud.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import List
import pandas as pd

from dagster import (
    AutomationCondition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..utils.clickhouse_client import get_client, insert_rows, query_scalar
from ..utils.exchange_price_service import ExchangePriceDataService

# Instruments to poll — all USDT perpetual futures used by strategies 
# Clickhouse format: "BTCUSDT"
INSTRUMENTS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

INSERT_COLUMNS = ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"]

def _get_ccxt_symbol(instrument: str) -> str:
    """Convert 'BTCUSDT' to 'BTC/USDT'."""
    if "/" in instrument:
        return instrument
    if instrument.endswith("USDT"):
        return f"{instrument[:-4]}/USDT"
    return instrument


def _df_to_rows(instrument: str, df: pd.DataFrame) -> List[list]:
    """Convert ExchangePriceDataService DataFrame to ClickHouse rows."""
    rows = []
    # df columns expected: ['open', 'high', 'low', 'close', 'volume', 'timestamp']
    for _, row in df.iterrows():
        ts_str = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
        rows.append([
            "binance",
            instrument,
            ts_str,
            float(row['open']),
            float(row['high']),
            float(row['low']),
            float(row['close']),
            float(row['volume']),
        ])
    return rows


@asset(
    name="binance_futures_ohlcv_1min",
    group_name="market_data",
    automation_condition=(
        AutomationCondition.on_cron("*/5 * * * *") & ~AutomationCondition.in_progress()
    ),
    description=(
        "Polls Binance Futures API via CCXT for 1-min OHLCV klines and inserts into "
        "analytics.futures_price_1min. Incremental — starts from last known ts per instrument."
    ),
    compute_kind="exchange_price_service",
)
def binance_futures_ohlcv_1min_asset(
    context: AssetExecutionContext,
) -> MaterializeResult:
    client = get_client()
    total_inserted = 0
    instruments_updated = []

    for instrument in INSTRUMENTS:
        # Find last known timestamp in Clickhouse
        last_ts = query_scalar(
            f"SELECT max(ts) FROM analytics.futures_price_1min "
            f"WHERE exchange = 'binance' AND instrument = '{instrument}'",
            client,
        )

        if last_ts and str(last_ts) != "1970-01-01 00:00:00":
            # Add 1 minute to start_time to avoid inserting duplicate row for the same minute
            start_dt = last_ts.replace(tzinfo=timezone.utc) + timedelta(minutes=1)
        else:
            # Bootstrap: start from 7 days ago
            start_dt = datetime.now(tz=timezone.utc) - timedelta(days=7)

        # To avoid querying Future data
        if start_dt > datetime.now(tz=timezone.utc):
            context.log.info(f"[{instrument}] Data is fully up to date")
            continue

        start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        context.log.info(f"[{instrument}] Polling from {start_iso}")

        try:
            ccxt_symbol = _get_ccxt_symbol(instrument)
            df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
                symbol=ccxt_symbol,
                exchange_name="binance_perp",
                timeframe="1m",
                date_since=start_iso,
                limit=1000
            )

            if df is None or df.empty:
                context.log.info(f"[{instrument}] No new klines")
                continue

            rows = _df_to_rows(instrument, df)
            
            # Clickhouse insertion
            insert_rows("analytics.futures_price_1min", INSERT_COLUMNS, rows, client)
            total_inserted += len(rows)
            instruments_updated.append(f"{instrument}({len(rows)})")
            context.log.info(f"[{instrument}] Inserted {len(rows)} klines")
        except Exception as exc:
            context.log.warning(f"[{instrument}] Failed: {exc}")

        # Rate limit between instruments
        time.sleep(0.5)

    return MaterializeResult(
        metadata={
            "total_inserted": MetadataValue.int(total_inserted),
            "instruments_updated": MetadataValue.text(", ".join(instruments_updated) or "none"),
        }
    )


__all__ = ["binance_futures_ohlcv_1min_asset"]

