"""
Binance Futures OHLCV Assets — Dual Strategy

1. binance_futures_backfill: Daily partitioned asset for historical data.
2. binance_futures_ohlcv_minutely: Unpartitioned asset for live 5-min updates.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import pandas as pd

from dagster import (
    DailyPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    AutomationCondition,
    asset,
)

from ..utils.clickhouse_client import get_client, insert_rows, query_scalar, execute_query
from ..utils.exchange_price_service import ExchangePriceDataService

# Configuration
START_DATE = "2024-01-01"
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
        rows.append([
            "binance", instrument, row['timestamp'].strftime("%Y-%m-%d %H:%M:%S"),
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

    for instrument in INSTRUMENTS:
        # Idempotency: clear existing data for this day/instrument
        execute_query(
            f"DELETE FROM {TARGET_TABLE} WHERE exchange='binance' AND instrument='{instrument}' "
            f"AND ts >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}' AND ts < '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}'",
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

# ─────────────────────────────────────────────────────────────────────────────
# 2. Live Asset (Real-time Polling)
# ─────────────────────────────────────────────────────────────────────────────

@asset(
    name="binance_futures_ohlcv_minutely",
    group_name="market_data",
    automation_condition=AutomationCondition.on_cron("*/5 * * * *") & ~AutomationCondition.in_progress(),
    description="Live 5-min polling asset. Fetches latest data since last max(ts).",
    compute_kind="binance",
)
def binance_futures_ohlcv_minutely_asset(context: AssetExecutionContext) -> MaterializeResult:
    client = get_client()
    total_inserted = 0
    instruments_updated = []

    for instrument in INSTRUMENTS:
        # Find latest timestamp in DB
        # Cast to String to avoid any type issues with query_scalar
        max_ts = query_scalar(
            f"SELECT toString(max(ts)) FROM {TARGET_TABLE} WHERE instrument = '{instrument}'", client
        )
        
        # Check if max_ts is valid. CCXT 'date_since' will use this.
        if not max_ts or str(max_ts) in ["1970-01-01 00:00:00", "None", "0000-00-00 00:00:00"]:
            # Default to last 10 minutes if DB is empty or has placeholder
            start_dt = datetime.now(timezone.utc) - timedelta(minutes=10)
            context.log.info(f"[{instrument}] No valid data found in DB. Bootstrapping from 10 mins ago.")
        else:
            # Start 1 minute after the latest record
            try:
                start_dt = datetime.strptime(str(max_ts), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) + timedelta(minutes=1)
            except Exception as e:
                context.log.warning(f"[{instrument}] Error parsing max_ts '{max_ts}': {e}. Falling back to 10 mins.")
                start_dt = datetime.now(timezone.utc) - timedelta(minutes=10)

        context.log.info(f"[{instrument}] Live poll from {start_dt}")

        df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
            symbol=_get_ccxt_symbol(instrument), exchange_name="binance_perp",
            timeframe="1m", date_since=start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), limit=1000
        )

        if df is not None and not df.empty:
            rows = _df_to_rows(instrument, df)
            insert_rows(TARGET_TABLE, INSERT_COLUMNS, rows, client)
            total_inserted += len(rows)
            instruments_updated.append(f"{instrument}({len(rows)})")

    return MaterializeResult(
        metadata={
            "total_inserted": MetadataValue.int(total_inserted),
            "instruments": MetadataValue.text(", ".join(instruments_updated) or "none"),
        }
    )
