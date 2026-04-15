"""
Backfill Job

Manually-triggered job that runs a full daily backfill:
  binance_futures_backfill → pnl_prod_v2_daily + pnl_real_trade_v2_daily → pnl_1hour_rollup

Usage: Dagster UI → Jobs → backfill_job → Launchpad → select partition dates → Launch.
"""

from dagster import AssetSelection, define_asset_job

backfill_job = define_asset_job(
    name="backfill_job",
    selection=AssetSelection.assets(
        "binance_futures_backfill",
        "pnl_prod_v2_daily",
        "pnl_real_trade_v2_daily",
        # pnl_1hour_rollup is unpartitioned; it runs its internal watermark-based
        # catch-up across all underlyings regardless of which partition dates are
        # selected above. This is intentional — it brings hourly rollup tables
        # up to date after each backfill run.
        "pnl_1hour_rollup",
    ),
    description=(
        "Manual backfill: price fetch → prod PnL → real trade PnL → hourly rollup. "
        "Select one or more daily partition dates in the Launchpad."
    ),
)
