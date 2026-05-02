"""
Dagster Definitions — trading-analysis

Entry point for the Dagster instance. Registers all assets and automation sensors.
"""

from dagster import Definitions

from ..assets.binance_futures_ohlcv import (
    binance_futures_backfill_asset,
    binance_futures_ohlcv_minutely_asset,
)
from ..assets.pnl_strategy_v2 import (
    pnl_prod_v2_live_asset,
    pnl_prod_v2_daily_asset,
    pnl_bt_v2_daily_asset,
    pnl_real_trade_v2_live_asset,
    pnl_real_trade_v2_daily_asset,
)
from ..assets.pnl_rollup import pnl_1hour_rollup_asset
from ..assets.pnl_safety_scan import pnl_daily_safety_scan_asset
from ..assets.clickhouse_connectivity_check import clickhouse_connectivity_check_asset
from ..assets.postgres_cleanup import postgres_cleanup_asset
from ..sensors.automation_sensors import build_automation_sensors


all_assets = [
    # Market data
    binance_futures_backfill_asset,
    binance_futures_ohlcv_minutely_asset,
    # Strategy PnL v2 (Live)
    pnl_prod_v2_live_asset,
    pnl_real_trade_v2_live_asset,
    # Strategy PnL v2 (Daily Backfills)
    pnl_prod_v2_daily_asset,
    pnl_bt_v2_daily_asset,
    pnl_real_trade_v2_daily_asset,
    # Rollups & Scans
    pnl_1hour_rollup_asset,
    pnl_daily_safety_scan_asset,
    # Infra checks
    clickhouse_connectivity_check_asset,
    postgres_cleanup_asset,
]

all_sensors = build_automation_sensors()

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
)
