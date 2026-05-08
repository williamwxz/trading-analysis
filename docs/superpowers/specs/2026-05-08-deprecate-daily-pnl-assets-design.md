# Design: Hard-delete daily PnL Dagster assets

**Date:** 2026-05-08

## Summary

Remove the three daily-partitioned PnL backfill assets and their exclusive helper functions. The `_full` recompute assets (`pnl_prod_v2_full`, `pnl_bt_v2_full`, `pnl_real_trade_v2_full`) replace all historical recompute use cases and are unaffected.

## Changes

### `trading_dagster/assets/pnl_strategy_v2.py`

Delete the following asset functions and their section comments:
- `pnl_prod_v2_daily_asset` (asset `pnl_prod_v2_daily`)
- `pnl_bt_v2_daily_asset` (asset `pnl_bt_v2_daily`)
- `pnl_real_trade_v2_daily_asset` (asset `pnl_real_trade_v2_daily`)

Delete the following helper functions (only called by the above):
- `_refresh_pnl_partitioned`
- `_refresh_pnl_bt`
- `_refresh_pnl_real_trade`

Remove module-level symbols only used by the deleted code:
- `daily_partitions` (`DailyPartitionsDefinition` instance for prod/real_trade)
- `bt_daily_partitions` (`DailyPartitionsDefinition` instance for bt)
- `DailyPartitionsDefinition` import (only used by the two partition instances above)
- `BT_START_DATE` import (only used by `bt_daily_partitions`)

Keep:
- `PROD_REAL_TRADE_START_DATE` — still used by `_recompute_pnl_recent` (line 580)

### `trading_dagster/definitions/__init__.py`

- Remove imports: `pnl_bt_v2_daily_asset`, `pnl_prod_v2_daily_asset`, `pnl_real_trade_v2_daily_asset`
- Remove from `all_assets` list

### `dagster.yaml`

Remove the tag concurrency limit block for `pnl_bt_v2_daily`:
```yaml
      - key: "dagster/asset_key"
        value: "pnl_bt_v2_daily"
        limit: 3
```

## Out of scope

- `_full` recompute assets and `_recompute_pnl_recent` — untouched
- `binance_futures_backfill_asset` — untouched
- ClickHouse tables — no schema changes; historical data in `strategy_pnl_1min_*` tables is preserved
