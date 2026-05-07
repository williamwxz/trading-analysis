# Design: `*_full` Assets — 3-Day Delete + Recompute

## Summary

Change the three `*_full` Dagster assets (`pnl_prod_v2_full`, `pnl_real_trade_v2_full`, `pnl_bt_v2_full`) to delete the last 3 days of data from their target table and recompute only that 3-day window, seeded with anchors read from just before the window.

The existing `_recompute_pnl_full` (full-history, no DELETE) is left intact as a fallback for emergency full reruns.

## Architecture

Add a new function `_recompute_pnl_recent` in `trading_dagster/assets/pnl_strategy_v2.py` alongside `_recompute_pnl_full`. Each `*_full` asset is updated to call `_recompute_pnl_recent` instead.

Add a helper `_process_underlying_recent` (mirrors the existing `_process_underlying`) that handles the per-underlying work in a thread pool.

`fetch_anchors` in `trading_dagster/utils/pnl_compute.py` gains an optional `before_ts` parameter so anchors can be scoped to rows strictly before `window_start`.

## Data Flow

For each underlying, `_process_underlying_recent` runs these steps in order:

1. **Load anchors** — `fetch_anchors(target_table, underlying, before_ts=window_start)` returns `{strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}` for the last committed row with `ts < window_start`. Falls back to zero anchors if none exist (cold start).
2. **DELETE** — `ALTER TABLE analytics.{target_table} DELETE WHERE underlying = '{underlying}' AND ts >= toDateTime('{window_start}')` removes the stale tail.
3. **Recompute** — runs `_process_underlying` logic over `[window_start, now)` using the loaded anchors and the same 7-day chunking.

`window_start` is computed once in `_recompute_pnl_recent` as `(now - 3 days)` truncated to UTC midnight: `datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=3)`.

## Changes

### `trading_dagster/utils/pnl_compute.py`

- `fetch_anchors(target_table, underlying, before_ts=None)` — adds `AND ts < toDateTime('{before_ts}')` to the WHERE clause when `before_ts` is provided.

### `trading_dagster/assets/pnl_strategy_v2.py`

- `_process_underlying_recent(underlying, target_table, source_table, label, insert_columns, mode, window_start, end_dt, _emit)` — new thread worker. Calls `fetch_anchors` with `before_ts`, runs the DELETE, then recomputes the window using the same chunked logic as `_process_underlying`.
- `_recompute_pnl_recent(context, target_table, source_table, label, insert_columns, mode)` — new top-level function. Computes `window_start`, discovers underlyings, fans out to `_process_underlying_recent` via `ThreadPoolExecutor`.
- `pnl_prod_v2_full_asset` — calls `_recompute_pnl_recent` instead of `_recompute_pnl_full`.
- `pnl_real_trade_v2_full_asset` — calls `_recompute_pnl_recent` instead of `_recompute_pnl_full`.
- `pnl_bt_v2_full_asset` — calls `_recompute_pnl_recent` instead of `_recompute_pnl_full`.

## Error Handling

- The DELETE runs inside the thread worker after anchors are loaded. If anchor loading fails, the thread raises and the asset fails before any DELETE occurs — no partial state.
- ClickHouse `ALTER TABLE ... DELETE` is asynchronous by default. A `OPTIMIZE TABLE ... FINAL` or a brief wait is not needed here because the subsequent INSERT does not read back the deleted rows; it only reads from the source table. The DELETE and INSERT target the same table but the read path (source) is different.
- If `before_ts` anchors return empty (strategy has no history before the window), the existing zero-anchor cold-start behaviour applies unchanged.

## What Is Not Changed

- `_recompute_pnl_full` and `_process_underlying` — untouched, available for manual full reruns.
- `_refresh_pnl_partitioned`, `_refresh_pnl_bt`, `_refresh_pnl_real_trade` — daily backfill assets unaffected.
- Asset names, group names, deps, op_tags, concurrency limits — unchanged.
