# Design: Full Recompute PnL Assets (pnl_prod_v2_full, pnl_real_trade_v2_full)

**Date:** 2026-05-04  
**Status:** Approved

## Problem

`pnl_prod_v2_daily` and `pnl_real_trade_v2_daily` are Dagster daily-partitioned assets. Each partition calls `fetch_anchors()` which reads the last committed row from the target ClickHouse table to seed the cumulative PnL anchor. This makes partitions implicitly dependent on the previous partition's output — running them in parallel produces incorrect PnL because a partition may read a stale or missing anchor.

Enforcing sequential partition execution in Dagster is awkward and error-prone.

## Solution

Add two new unpartitioned Dagster assets that recompute PnL sequentially from the start date to now in a single long-running job:

- `pnl_prod_v2_full` — production PnL full recompute
- `pnl_real_trade_v2_full` — real trade PnL full recompute

Backtest (`pnl_bt_v2_daily`) is excluded — BT PnL uses `cumulative_pnl` directly from the bar with no anchor chaining, so it has no sequential dependency problem.

The existing partitioned assets remain registered and untouched. The new assets are the go-forward recompute path.

## Architecture

### New Assets

Both assets live in `trading_dagster/assets/pnl_strategy_v2.py` alongside the existing ones, and are registered in `trading_dagster/definitions/__init__.py`.

```
pnl_prod_v2_full
  deps: binance_futures_backfill
  group: strategy_pnl
  compute_kind: clickhouse
  partitions_def: None (unpartitioned)
  automation_condition: None (manual trigger only)
  op_tags: None (no timeout)

pnl_real_trade_v2_full
  deps: binance_futures_backfill
  group: strategy_pnl
  compute_kind: clickhouse
  partitions_def: None (unpartitioned)
  automation_condition: None (manual trigger only)
  op_tags: None (no timeout)
```

### Compute Flow

Each asset runs a single function `_recompute_pnl_full(context, target_table, source_table, label)`:

1. **Determine time range** — `start_ts = PROD_REAL_TRADE_START_DATE`, `end_ts = now()` (UTC)

2. **Fetch all underlyings** — `_get_underlyings(source_table)`, same as partitioned path

3. **Per underlying loop** (sequential):
   a. Fetch all strategy bars from `source_table` ordered by `(strategy_table_name, ts ASC)` for the full time range
   b. Fetch all prices from `futures_price_1min` for the full time range via `fetch_prices_multi()`
   c. Bootstrap anchors from the target table via `fetch_anchors()` — on a clean table this returns empty (cold start, anchor = 0)
   d. Run `iter_compute_prod_pnl()` (prod) or `compute_real_trade_pnl()` (real_trade) over all bars in order — the existing functions already chain anchors minute-by-minute
   e. Batch insert every `BATCH_MINUTES = 1440` minutes worth of rows into the target table with a fresh `updated_at`
   f. Log progress per batch for observability

4. **No DELETE** — rows are re-inserted with a newer `updated_at`. `ReplacingMergeTree(updated_at)` deduplicates on background merge. Reruns are safe.

### Deduplication

`strategy_pnl_1min_prod_v2` and `strategy_pnl_1min_real_trade_v2` use `ReplacingMergeTree(updated_at)`. Re-inserting rows with a newer `updated_at` causes ClickHouse to keep the latest version after background merge. No explicit `DELETE` is required, making reruns eventually consistent and safe to run alongside the live `pnl_consumer`.

### Existing Code Reuse

| Existing function | Reused by new assets |
|---|---|
| `fetch_anchors()` | Bootstrap starting anchor per strategy |
| `iter_compute_prod_pnl()` | Prod PnL row generation (yields per strategy, memory-efficient) |
| `compute_real_trade_pnl()` | Real trade PnL row generation (returns flat list, inserted in one batch per underlying) |
| `fetch_prices_multi()` | Price data for full time range |
| `_get_underlyings()` | Instrument list from source table |
| `_prepare_rows_for_clickhouse()` | datetime conversion before insert |
| `insert_rows()` | Batch ClickHouse insert |

No new utility functions needed.

## What Changes

| File | Change |
|---|---|
| `trading_dagster/assets/pnl_strategy_v2.py` | Add `_recompute_pnl_full()`, `pnl_prod_v2_full_asset`, `pnl_real_trade_v2_full_asset` |
| `trading_dagster/definitions/__init__.py` | Register the two new assets |

## What Stays the Same

- `pnl_prod_v2_daily`, `pnl_bt_v2_daily`, `pnl_real_trade_v2_daily` — untouched, still registered
- `pnl_1hour_*_rollup` assets — untouched
- `pnl_consumer` ECS service — untouched
- All existing tests

## Constraints

- **No timeout** — `op_tags` with `dagster/timeout` is intentionally omitted. The job may run for hours over 60+ days of history.
- **Manual trigger only** — no `AutomationCondition`. Triggered from Dagster UI or API when a full recompute is needed.
- **Sequential within underlying** — bars are processed in `ORDER BY ts ASC` to preserve anchor chain correctness.
- **Memory** — `fetch_prices_multi()` for the full date range may be large. If memory pressure is observed, an internal day-chunked loop can be added later without changing the asset interface.

## Out of Scope

- Removing or deprecating the existing partitioned assets (separate decision)
- Backtest full recompute (no anchor dependency, not needed)
- Automated scheduling of full recompute
