# Design: `*_full` Assets — 3-Day Delete + Recompute

## Summary

Change the three `*_full` Dagster assets (`pnl_prod_v2_full`, `pnl_real_trade_v2_full`, `pnl_bt_v2_full`) to:
1. Pause the corresponding ECS pnl-consumer service via the AWS ECS API
2. Delete the last 3 days of data from their target table
3. Recompute only that 3-day window, seeded with anchors read from just before the window
4. Resume the ECS service

The existing `_recompute_pnl_full` (full-history, no DELETE) is left intact as a fallback for emergency full reruns.

## ECS Consumer Mapping

Each `*_full` asset pauses only its own consumer service:

| Asset | ECS Service |
|---|---|
| `pnl_prod_v2_full` | `trading-analysis-pnl-consumer-prod` |
| `pnl_real_trade_v2_full` | `trading-analysis-pnl-consumer-real-trade` |
| `pnl_bt_v2_full` | `trading-analysis-pnl-consumer-bt` (already `desired_count=0`, no-op) |

ECS cluster: `trading-analysis`, region: `ap-northeast-1`.

Pause = `update_service(desiredCount=0)`. Resume = `update_service(desiredCount=1)`.

## Architecture

Add a new function `_recompute_pnl_recent` in `trading_dagster/assets/pnl_strategy_v2.py` alongside `_recompute_pnl_full`. Each `*_full` asset is updated to call `_recompute_pnl_recent` instead, passing the ECS service name.

Add a helper `_process_underlying_recent` (mirrors the existing `_process_underlying`) that handles per-underlying work in a thread pool.

`fetch_anchors` in `trading_dagster/utils/pnl_compute.py` gains an optional `before_ts` parameter so anchors can be scoped to rows strictly before `window_start`.

## Data Flow

`_recompute_pnl_recent` orchestrates at the top level (not in the thread worker):

1. **Pause consumer** — `boto3` ECS `update_service(desiredCount=0)` on the mapped service. Wait for running task count to reach 0 via `ecs.get_waiter("services_stable")`.
2. **Discover underlyings** — `_get_underlyings(source_table)`.
3. **Fan out** — `ThreadPoolExecutor` runs `_process_underlying_recent` per underlying.
4. **Resume consumer** — `update_service(desiredCount=1)` in a `finally` block so it always runs even if recompute fails.

For each underlying, `_process_underlying_recent` runs:

1. **Load anchors** — `fetch_anchors(target_table, underlying, before_ts=window_start)` returns `{strategy_table_name: (anchor_pnl, anchor_price, anchor_position)}` for the last committed row with `ts < window_start`. Falls back to zero anchors if none exist (cold start).
2. **DELETE** — `ALTER TABLE analytics.{target_table} DELETE WHERE underlying = '{underlying}' AND ts >= toDateTime('{window_start}')` removes the stale tail.
3. **Recompute** — chunks from `[window_start, now)` using the loaded anchors and the same 7-day chunking logic as `_process_underlying`.

`window_start` is computed once in `_recompute_pnl_recent` as `(now - 3 days)` truncated to UTC midnight: `datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=3)`.

## Changes

### `trading_dagster/utils/pnl_compute.py`

- `fetch_anchors(target_table, underlying, before_ts=None)` — adds `AND ts < toDateTime('{before_ts}')` to the WHERE clause when `before_ts` is provided.

### `trading_dagster/assets/pnl_strategy_v2.py`

- `_pause_ecs_service(service_name, cluster, region, boto_client)` / `_resume_ecs_service(...)` — thin helpers that call `update_service` and wait for stability.
- `_process_underlying_recent(underlying, target_table, source_table, label, insert_columns, mode, window_start, end_dt, _emit)` — thread worker. Loads anchors with `before_ts`, runs DELETE, recomputes.
- `_recompute_pnl_recent(context, target_table, source_table, label, insert_columns, mode, ecs_service)` — top-level function. Pauses consumer, computes `window_start`, fans out threads, resumes consumer in `finally`.
- `pnl_prod_v2_full_asset` — calls `_recompute_pnl_recent(..., ecs_service="trading-analysis-pnl-consumer-prod")`.
- `pnl_real_trade_v2_full_asset` — calls `_recompute_pnl_recent(..., ecs_service="trading-analysis-pnl-consumer-real-trade")`.
- `pnl_bt_v2_full_asset` — calls `_recompute_pnl_recent(..., ecs_service="trading-analysis-pnl-consumer-bt")`.

`boto3` is already available in the environment (ECS Fargate task role has ECS permissions via Terraform).

## Error Handling

- **Consumer always resumes**: `update_service(desiredCount=1)` is in a `finally` block. If the recompute raises, the consumer is restarted before the asset fails.
- **Anchors before DELETE**: If anchor loading fails, the thread raises before any DELETE occurs — no partial state.
- **bt consumer is already stopped**: `desired_count=0` in Terraform; pause/resume are no-ops (ECS ignores setting desiredCount to its current value).
- **ClickHouse DELETE is async**: No wait needed — the subsequent INSERT reads from the source table (`strategy_output_history_*`), not the target.
- **Zero anchors**: If no rows exist before `window_start`, the existing zero-anchor cold-start behaviour applies unchanged.

## What Is Not Changed

- `_recompute_pnl_full` and `_process_underlying` — untouched, available for manual full reruns.
- `_refresh_pnl_partitioned`, `_refresh_pnl_bt`, `_refresh_pnl_real_trade` — daily backfill assets unaffected.
- Asset names, group names, deps, op_tags, concurrency limits — unchanged.
- IAM permissions — the Dagster task role already has `ecs:UpdateService` and `ecs:DescribeServices` via the `TerraformProvision` policy in Terraform.
