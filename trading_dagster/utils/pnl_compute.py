"""Re-exports from libs.computation — all computation logic lives there.

Kept for backward-compatible imports from pnl_strategy_v2.py.
"""

from libs.computation.fetch_bars import (
    fetch_anchors,
    fetch_new_bars_bt,
    fetch_new_bars_prod,
    fetch_new_bars_real_trade,
)
from libs.computation.fetch_prices import fetch_prices_multi
from libs.computation.pnl_formula import (
    PROD_INSERT_COLUMNS,
    REAL_TRADE_INSERT_COLUMNS,
    TIMEFRAME_MAP,
    compute_bt_pnl,
    compute_prod_pnl,
    compute_real_trade_pnl,
    iter_compute_prod_pnl,
)

# Legacy constant still referenced in pnl_strategy_v2.py
PROD_REAL_TRADE_START_DATE = "2026-02-27"

__all__ = [
    "PROD_INSERT_COLUMNS",
    "REAL_TRADE_INSERT_COLUMNS",
    "TIMEFRAME_MAP",
    "PROD_REAL_TRADE_START_DATE",
    "compute_bt_pnl",
    "compute_prod_pnl",
    "compute_real_trade_pnl",
    "iter_compute_prod_pnl",
    "fetch_anchors",
    "fetch_new_bars_bt",
    "fetch_new_bars_prod",
    "fetch_new_bars_real_trade",
    "fetch_prices_multi",
]
