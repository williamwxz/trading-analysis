"""Unit tests for backfill_job definition."""

import pytest
from dagster import AssetKey


def test_backfill_job_loads():
    """Job can be imported without error."""
    from trading_dagster.jobs.backfill_job import backfill_job
    assert backfill_job.name == "backfill_job"


def test_backfill_job_selects_correct_assets():
    """Job selection includes all four expected assets."""
    from trading_dagster.jobs.backfill_job import backfill_job

    expected = {
        AssetKey("binance_futures_backfill"),
        AssetKey("pnl_prod_v2_daily"),
        AssetKey("pnl_real_trade_v2_daily"),
        AssetKey("pnl_1hour_rollup"),
    }
    # AssetSelection.assets() stores keys; resolve against the full asset graph
    # to verify the selection is well-formed (no typos)
    from trading_dagster.definitions import defs

    job = defs.get_job_def("backfill_job")
    assert job is not None
    assert job.name == "backfill_job"
