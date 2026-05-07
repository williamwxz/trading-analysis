# Design: Grafana Dynamic Table Resolution (1min ↔ 1hour)

**Date:** 2026-05-06  
**Status:** Approved

## Problem

All Grafana dashboards currently query `strategy_pnl_1min_*` tables unconditionally. For long time ranges this is unnecessarily expensive — the 1hour materialized views exist and are better suited. For short ranges (≤6h) the 1min tables are needed for resolution.

## Solution

Add a hidden Grafana template variable `resolution` to each dashboard. It evaluates to `'1min'` or `'1hour'` based on the selected time range start. All panel SQL references the variable via `${resolution:raw}` in the table name.

## The `resolution` Variable

Added to `templating.list` in each of the 5 dashboard JSON files.

| Field | Value |
|---|---|
| Name | `resolution` |
| Type | `query` |
| Datasource | ClickHouse |
| Refresh | `2` (On time range change) |
| Hide | `2` (hidden from UI) |
| Multi | false |
| includeAll | false |

Query:
```sql
SELECT if(
  toDateTime(${__from:date:seconds}) >= now() - INTERVAL 6 HOUR,
  '1min',
  '1hour'
)
```

`${__from:date:seconds}` is a Grafana built-in that provides the time range start as Unix seconds. ClickHouse's `toDateTime()` expects seconds (not milliseconds). Setting Refresh to "On time range change" ensures the variable re-evaluates every time the user adjusts the picker.

## SQL Substitution Rules

Four string substitutions applied to `rawSql` fields across all 5 dashboards:

| Old | New |
|---|---|
| `analytics.strategy_pnl_1min_prod_v2` | `analytics.strategy_pnl_${resolution:raw}_prod_v2` |
| `analytics.strategy_pnl_1min_bt_v2` | `analytics.strategy_pnl_${resolution:raw}_bt_v2` |
| `analytics.strategy_pnl_1hour_bt_v2` | `analytics.strategy_pnl_${resolution:raw}_bt_v2` |
| `analytics.strategy_pnl_1min_real_trade_v2` | `analytics.strategy_pnl_${resolution:raw}_real_trade_v2` |

The `:raw` modifier prevents Grafana from quoting the interpolated value, which would break the table name syntax.

## Affected Dashboards and Query Counts

| Dashboard | prod queries | bt queries | real_trade queries |
|---|---|---|---|
| l1-instance | 2 | 8 (6×1min + 2×1hour) | 5 |
| l2-sid-underlying | 8 | 5 | 5 |
| l3-sid | 6 | 4 | 4 |
| l4-underlying | 5 | 4 | 4 |
| l5-portfolio | 7 | 7 | 6 |

Total: ~85 table references replaced across 5 files.

## Behaviour at Runtime

| Time range start | `resolution` value | Tables queried |
|---|---|---|
| Within last 6 hours | `1min` | `strategy_pnl_1min_{prod,bt,real_trade}_v2` |
| Older than 6 hours | `1hour` | `strategy_pnl_1hour_{prod,bt,real_trade}_v2` |

The switch is automatic — no user action required beyond changing the time range picker.

## Implementation Notes

- Changes are purely in `infra/grafana/dashboards/*.json`
- No ClickHouse schema changes needed — all four `1hour_*` tables already exist
- CI/CD pushes dashboard JSON to Grafana Cloud on every push to `main` (`deploy-grafana-cloud` job)
- The `resolution` variable JSON object must be inserted before any variable that references it (order matters in `templating.list`)
