"""
Binance Futures OHLCV 1-min Partitioned Asset

Fetches 1-minute klines from Binance Futures API, partitioned by day.
Inserts into analytics.futures_price_1min in ClickHouse Cloud.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import List
import pandas as pd

from dagster import (
    DailyPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..utils.clickhouse_client import get_client, insert_rows, query_scalar, execute_query
from ..utils.exchange_price_service import ExchangePriceDataService

# Start date for partitions - can be adjusted
START_DATE = "2024-01-01"

# Instruments to poll
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
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    description=(
        "Fetches daily partitions of 1-min OHLCV klines from Binance Futures. "
        "Each partition covers 24 hours of data."
    ),
    compute_kind="binance",
)
def binance_futures_ohlcv_1min_asset(
    context: AssetExecutionContext,
) -> MaterializeResult:
    # Get the date for this partition (e.g., '2024-04-05')
    partition_date_str = context.partition_key
    
    # Calculate time window for the partition
    start_dt = datetime.strptime(partition_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    
    # CCXT requires ISO8601
    start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    client = get_client()
    total_inserted = 0
    instruments_updated = []

    context.log.info(f"Processing partition {partition_date_str} ({start_dt} to {end_dt})")

    for instrument in INSTRUMENTS:
        # 1. Clean up existing data for this partition to ensure idempotency
        # Note: In a production setup with high volume, you might use 'ReplacingMergeTree' 
        # but for simple 1-min bars, a DELETE is safe.
        execute_query(
            f"DELETE FROM analytics.futures_price_1min "
            f"WHERE exchange = 'binance' AND instrument = '{instrument}' "
            f"AND ts >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}' "
            f"AND ts < '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}'",
            client
        )

        # 2. Fetch data in batches (Daily 1-min data = 1440 rows, Binance limit = 1000)
        current_start_iso = start_iso
        instrument_inserted = 0
        
        while True:
            df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
                symbol=_get_ccxt_symbol(instrument),
                exchange_name="binance_perp",
                timeframe="1m",
                date_since=current_start_iso,
                limit=1000
            )

            if df is None or df.empty:
                break

            # Filter only rows within our partition window
            df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] < end_dt)]
            
            if df.empty:
                break

            rows = _df_to_rows(instrument, df)
            insert_rows("analytics.futures_price_1min", INSERT_COLUMNS, rows, client)
            
            instrument_inserted += len(rows)
            total_inserted += len(rows)
            
            # If we fetched fewer than 1000, or we reached the end of the day, stop
            if len(df) < 1000 or df.iloc[-1]['timestamp'] >= (end_dt - timedelta(minutes=1)):
                break
            
            # Move window forward for the next batch
            next_ts = df.iloc[-1]['timestamp'] + timedelta(minutes=1)
            current_start_iso = next_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            time.sleep(0.1)

        instruments_updated.append(f"{instrument}({instrument_inserted})")
        context.log.info(f"[{instrument}] Inserted {instrument_inserted} klines for {partition_date_str}")

    return MaterializeResult(
        metadata={
            "partition_date": MetadataValue.text(partition_date_str),
            "total_inserted": MetadataValue.int(total_inserted),
            "instruments_updated": MetadataValue.text(", ".join(instruments_updated) or "none"),
        }
    )
