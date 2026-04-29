"""
Backfill Jobs

Manually-triggered jobs for daily partitioned backfills.

pnl_1hour_rollup is excluded from all jobs: it is unpartitioned and mixing
it with partitioned assets breaks the Dagster UI launchpad. Run it separately
via the asset materialization UI after a backfill completes.

Usage: Dagster UI → Jobs → <job_name> → Launchpad → select partition dates → Launch.
"""

from dagster import AssetSelection, define_asset_job

backfill_job = define_asset_job(
    name="backfill_job",
    selection=AssetSelection.assets(
        "binance_futures_backfill",
        "pnl_prod_v2_daily",
        "pnl_real_trade_v2_daily",
    ),
    description=(
        "Manual backfill: price fetch → prod PnL → real trade PnL. "
        "Select one or more daily partition dates in the Launchpad."
    ),
    tags={"dagster/max_concurrent_runs": "5"},
)

bt_backfill_job = define_asset_job(
    name="bt_backfill_job",
    selection=AssetSelection.assets(
        "binance_futures_backfill",
        "pnl_bt_v2_daily",
    ),
    description=(
        "Manual backfill: price fetch → backtest PnL. "
        "Select one or more daily partition dates in the Launchpad."
    ),
    tags={"dagster/max_concurrent_runs": "5"},
)
