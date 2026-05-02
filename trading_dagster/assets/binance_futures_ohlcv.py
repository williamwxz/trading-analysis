"""
Binance Futures OHLCV Assets

binance_futures_backfill: Daily partitioned asset for historical data.
Real-time market data is handled by the pnl_consumer (Kafka → ClickHouse).
"""

import time
from datetime import datetime, timedelta, timezone
from typing import List
import pandas as pd

from dagster import (
    DailyPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
    asset,
)

from ..utils.clickhouse_client import get_client, insert_rows, query_scalar, execute_query
from ..utils.exchange_price_service import ExchangePriceDataService

# Configuration
START_DATE = "2020-06-14"
INSTRUMENTS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]
TARGET_TABLE = "analytics.futures_price_1min"
INSERT_COLUMNS = ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"]

def _get_ccxt_symbol(instrument: str) -> str:
    if "/" in instrument: return instrument
    # For Binance USDM Futures, CCXT v2+ uses 'BTC/USDT:USDT' unified format
    if instrument.endswith("USDT"):
        return f"{instrument[:-4]}/USDT:USDT"
    return instrument

def _df_to_rows(instrument: str, df: pd.DataFrame) -> List[list]:
    # Ensure timestamp is datetime objects
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    rows = []
    for _, row in df.iterrows():
        ts = row['timestamp']
        # clickhouse-connect requires a datetime object for DateTime columns, not a string
        if not isinstance(ts, datetime):
            ts = ts.to_pydatetime()
        rows.append([
            "binance", instrument, ts,
            float(row['open']), float(row['high']), float(row['low']), float(row['close']), float(row['volume']),
        ])
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# 1. Backfill Asset (Historical)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="binance_futures_backfill",
    group_name="market_data",
    partitions_def=DailyPartitionsDefinition(start_date=START_DATE),
    description="Historical daily backfill asset. Trigger manually for specific dates.",
    compute_kind="binance",
)
def binance_futures_backfill_asset(context: AssetExecutionContext) -> MaterializeResult:
    partition_date_str = context.partition_key
    start_dt = datetime.strptime(partition_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)
    
    client = get_client()
    total_inserted = 0

    start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    for instrument in INSTRUMENTS:
        # Skip if full day already present
        existing = query_scalar(
            f"SELECT count() FROM {TARGET_TABLE} "
            f"WHERE exchange='binance' AND instrument='{instrument}' "
            f"AND ts >= '{start_dt_str}' AND ts < '{end_dt_str}'",
            client
        )
        if (existing or 0) >= 1440:
            context.log.info(f"[{instrument}] Full day already present ({existing} rows), skipping.")
            continue

        # Idempotency: clear existing partial data for this day/instrument
        execute_query(
            f"DELETE FROM {TARGET_TABLE} WHERE exchange='binance' AND instrument='{instrument}' "
            f"AND ts >= '{start_dt_str}' AND ts < '{end_dt_str}'",
            client
        )

        current_start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        instrument_inserted = 0
        while True:
            df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
                symbol=_get_ccxt_symbol(instrument), exchange_name="binance_perp",
                timeframe="1m", date_since=current_start_iso, limit=1000
            )
            if df is None or df.empty: break

            # Ensure timestamp is datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            # Filter to partition window
            df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] < end_dt)]
            if df.empty: break

            insert_rows(TARGET_TABLE, INSERT_COLUMNS, _df_to_rows(instrument, df), client)
            total_inserted += len(df)
            instrument_inserted += len(df)

            if len(df) < 1000 or df.iloc[-1]['timestamp'] >= (end_dt - timedelta(minutes=1)): break
            current_start_iso = (df.iloc[-1]['timestamp'] + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            time.sleep(0.1)

        context.log.info(f"[{instrument}] Finished partition {partition_date_str}, inserted {instrument_inserted} rows.")

    return MaterializeResult(metadata={"date": partition_date_str, "total_rows": total_inserted})
