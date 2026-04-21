# Backfill Job Design

**Date:** 2026-04-15  
**Status:** Approved

## Overview

A single manually-triggered Dagster job (`backfill_job`) that chains Binance price fetch → strategy PnL computation → hourly rollup for a user-selected set of daily partitions.

## Goal

Allow operators to select one or more partition dates in the Dagster UI and run a full backfill pipeline — price data, production PnL, real trade PnL, and hourly rollup — as a single coordinated job.

## Assets Included

| Asset | Type | Group |
|---|---|---|
| `binance_futures_backfill` | Partitioned (daily) | market_data |
| `pnl_prod_v2_daily` | Partitioned (daily) | strategy_pnl |
| `pnl_real_trade_v2_daily` | Partitioned (daily) | strategy_pnl |
| `pnl_1hour_rollup` | Unpartitioned | strategy_pnl |

## Execution Order

Dagster resolves execution order from asset dependencies:

1. `binance_futures_backfill` — fetches 1-min OHLCV from Binance for the partition date
2. `pnl_prod_v2_daily` + `pnl_real_trade_v2_daily` — run in parallel, both depend on step 1
3. `pnl_1hour_rollup` — unpartitioned, runs once after partitioned steps finish

## Implementation

### New file: `trading_dagster/jobs/backfill_job.py`

```python
from dagster import define_asset_job, AssetSelection

backfill_job = define_asset_job(
    name="backfill_job",
    selection=AssetSelection.assets(
        "binance_futures_backfill",
        "pnl_prod_v2_daily",
        "pnl_real_trade_v2_daily",
        "pnl_1hour_rollup",
    ),
    description="Manual backfill: price fetch → prod PnL → real trade PnL → hourly rollup.",
)
```

### Update: `trading_dagster/definitions/__init__.py`

- Import `backfill_job` from `..jobs.backfill_job`
- Add `jobs=[backfill_job]` to `Definitions(...)`

### New file: `trading_dagster/jobs/__init__.py`

Empty or re-exporting `backfill_job`.

## Usage

1. Open Dagster UI → Jobs → `backfill_job` → Launchpad
2. Select partition date(s) or a date range
3. Click Launch — Dagster schedules the four assets in dependency order

## Partitions

The job inherits `DailyPartitionsDefinition(start_date="2024-01-01")` from the partitioned assets in the selection. No explicit partition config needed on the job definition.

## Out of Scope

- Backtest PnL (`pnl_bt_v2_daily`) — commented out and not currently active
- Scheduling / automation — job is manual-only; existing automation sensors are unchanged
