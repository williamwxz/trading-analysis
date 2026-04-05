"""
Dagster Definitions — trading-analysis

Entry point for the Dagster instance. Registers all assets and automation sensors.

Simplified from falcon-lakehouse:
- No StarRocks, no Amber Data, no bronze layer
- V2-only PnL pipeline + Binance OHLCV polling
- Two sensor groups: market_data and strategy_pnl
"""

from dagster import Definitions

from ..assets.binance_futures_ohlcv import binance_futures_ohlcv_1min_asset
from ..assets.pnl_prod_v2_refresh import pnl_prod_v2_refresh_asset
from ..assets.pnl_bt_v2_refresh import pnl_bt_v2_refresh_asset
from ..assets.pnl_real_trade_v2_refresh import pnl_real_trade_v2_refresh_asset
from ..assets.pnl_rollup import pnl_1hour_rollup_asset
from ..assets.pnl_safety_scan import pnl_daily_safety_scan_asset
from ..sensors.automation_sensors import build_automation_sensors


all_assets = [
    # Market data
    binance_futures_ohlcv_1min_asset,
    # Strategy PnL v2
    pnl_prod_v2_refresh_asset,
    pnl_bt_v2_refresh_asset,
    pnl_real_trade_v2_refresh_asset,
    pnl_1hour_rollup_asset,
    pnl_daily_safety_scan_asset,
]

all_sensors = build_automation_sensors()

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
)
