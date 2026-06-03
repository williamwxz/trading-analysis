# Daily PnL Rollup Tables — Design

**Date:** 2026-06-01
**Status:** Approved, partially applied (DDL + backfill done in ClickHouse Cloud; code changes pending)

## Goal

Add a daily-granularity PnL layer mirroring the existing 1min and 1hour layers, for all three variants (`prod`, `bt`, `real_trade`). A daily row represents the **end-of-day snapshot** (last observation in the UTC day) of every PnL field, consistent with how each hourly row represents the last 1min observation in that hour.

## Non-Goals

- No Dagster job for daily rollup. Refresh is **MV-only** — the ClickHouse Materialized View handles all writes.
- No changes to `pnl_consumer` or `services/streaming/`. The 1min → 1hour → 1day cascade is driven entirely by MV triggers in ClickHouse.
- No partition or sort-key changes elsewhere. Monthly partitions and `(strategy_table_name, ts)` order key stay consistent across all three granularities.

## Architecture

### Data flow

```
pnl_consumer writes → analytics.strategy_pnl_1min_<variant>_v2
   ↓ (MV: strategy_pnl_1hour_<variant>_mv fires on every 1min INSERT)
   analytics.strategy_pnl_1hour_<variant>_v2
   ↓ (MV: strategy_pnl_1day_<variant>_mv fires on every 1hour INSERT — including
      writes from the upstream 1hour MV and from Dagster batch corrections)
   analytics.strategy_pnl_1day_<variant>_v2
```

ClickHouse cascades MV triggers: writes performed by `strategy_pnl_1hour_<variant>_mv` are themselves INSERTs to its target table, which fires `strategy_pnl_1day_<variant>_mv`. The same applies to direct writes to the 1hour table from:

- `pnl_hourly_rollup` Dagster asset (hourly schedule, 6h lookback re-aggregation)
- `_refresh_hour_rollup_for_window` in each `pnl_<mode>_v2_full` asset (per-refresh DELETE + re-INSERT for the rolling window)

This means **today's daily row stays fresh** as upstream writes land throughout the day — no separate batch trigger is required.

### Tables

Three new tables under `analytics.`:

- `strategy_pnl_1day_prod_v2`
- `strategy_pnl_1day_bt_v2`
- `strategy_pnl_1day_real_trade_v2`

Schema identical to the 1hour tables — same 16 columns:

```
strategy_table_name  String,
strategy_id          Int32,
strategy_name        String,
underlying           String,
config_timeframe     String,
source               String,
version              String,
ts                   DateTime,              -- toStartOfDay(upstream_ts), UTC
cumulative_pnl       Nullable(Float64),
benchmark            Float64,
position             Float64,
price                Float64,
final_signal         Float64,
weighting            Float64,
updated_at           DateTime DEFAULT now(),
strategy_instance_id String   DEFAULT ''
```

- **Engine:** `ReplacingMergeTree(updated_at)`
- **Partition:** `toYYYYMM(ts)` — monthly partitions hold ~30 rows per strategy
- **Sort:** `ORDER BY (strategy_table_name, ts)`

### Materialized Views

One MV per variant, reading from the corresponding 1hour table:

```sql
CREATE MATERIALIZED VIEW analytics.strategy_pnl_1day_<variant>_mv
TO analytics.strategy_pnl_1day_<variant>_v2
AS
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version,
    toStartOfDay(src_ts)            AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_<variant>_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfDay(src_ts);
```

**Per-block semantics:** each MV fire aggregates only the inserted block, not the full 1hour table — cheap regardless of write frequency. Multiple writes per day land in the same daily slot; `ReplacingMergeTree(updated_at)` deduplicates on background merge.

**argMax-of-argMax soundness:** each 1hour row already represents the last 1min observation in that hour (built by argMax over the hour). Taking `argMax(field, ts)` again across hours within a day yields the last hourly row, which equals the last 1min observation of the day — an end-of-day snapshot consistent with the 1hour pattern.

### Read pattern

Daily slots accumulate multiple ReplacingMergeTree rows (one per upstream write) until merges run. All queries against the daily tables must use:

```sql
SELECT ... FROM analytics.strategy_pnl_1day_<variant>_v2 FINAL WHERE ...
-- or, to avoid the in-memory merge cost on large queries:
SELECT ... FROM analytics.strategy_pnl_1day_<variant>_v2
WHERE ...
ORDER BY strategy_table_name, ts, updated_at DESC
LIMIT 1 BY strategy_table_name, ts
```

Same pattern already used against the 1hour tables.

## Backfill

One-time SQL backfill required after MV creation to populate history. Per variant:

```sql
INSERT INTO analytics.strategy_pnl_1day_<variant>_v2
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version,
    toStartOfDay(src_ts)            AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_<variant>_v2 FINAL)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfDay(src_ts);
```

**Backfill applied (2026-06-01 against ClickHouse Cloud):**

| Variant | 1hour rows | Daily rows after backfill | Range | Elapsed |
|---------|-----------|--------------------------|-------|---------|
| prod | 1,118,018 | 47,606 | 2026-02-28 → 2026-06-02 | 1.1s |
| bt | 20,736,215 | 864,933 | 2020-06-14 → 2026-06-02 | 18.8s |
| real_trade | 1,096,089 | 47,481 | 2026-03-09 → 2026-06-02 | 1.1s |

Idempotent — re-running the backfill writes the same rows with higher `updated_at`, harmlessly winning the ReplacingMergeTree merge.

## Coverage audit

Extend `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` to audit the daily layer using the same Phase 3 (position-correctness) check the hour audit already performs.

**Existing hour-audit shape** (lines 965–986 + 300–341 + 790-ish):

- `_HOUR_TABLES` lists `(hour_table, min_table, variant)` triples.
- `_fetch_q_stat_hour(hour_table, underlying, client)` returns `dict[stn → list[(hour_ts, position)]]`.
- `_check_phase3_hour` walks each hour row and verifies its position equals the latest 1-min position change with `effective_ts < hour_ts + 1h`. The `+1h` cutoff is hard-coded.
- No Phase 1 (per-bucket presence) check on hours — coverage is derived from 1-min presence.

**Mirror for daily**, generalized via bucket width:

1. **Parameterize the bucket cutoff.** Rename `_check_phase3_hour` → `_check_phase3_bucketed(bucket: timedelta, ...)` and replace the hard-coded `timedelta(hours=1)` with the passed-in `bucket`. Same for `_fetch_q_stat_hour` → `_fetch_q_stat_bucketed(bucket_expr: str, ...)` where `bucket_expr` is `"toStartOfHour(ts)"` or `"toStartOfDay(ts)"`. Existing call sites pass `timedelta(hours=1)` / `"toStartOfHour(ts)"`.
2. **Add `_DAY_TABLES`** alongside `_HOUR_TABLES`:
   ```python
   _DAY_TABLES: list[tuple[str, str, Mode]] = [
       ("strategy_pnl_1day_prod_v2",       "strategy_pnl_1min_prod_v2",       "prod"),
       ("strategy_pnl_1day_bt_v2",         "strategy_pnl_1min_bt_v2",         "bt"),
       ("strategy_pnl_1day_real_trade_v2", "strategy_pnl_1min_real_trade_v2", "real_trade"),
   ]
   ```
   Sources reference the 1min change-stream (`min_table`) because Phase 3 verifies position against the canonical minute-level transition log — the same source the hour audit uses. The 1hour table is not the source of truth for positions.
3. **Add a `_audit_day_table(day_table, min_table, client)`** function that calls the bucketed helpers with `timedelta(days=1)` / `"toStartOfDay(ts)"`.
4. **Add a loop** in `pnl_coverage_audit_asset` (mirroring lines 1020–1028) that iterates `_DAY_TABLES`.

No new Phase 1 logic — daily coverage gaps are implied by the 1min/1hour audits already in place.

**Unit test:** extend `_check_phase3_*` unit tests (if any exist) to cover the day-bucket case with a synthetic transition log. If none exist, add one minimal case asserting that a synthetic day_position derived as the latest min change within the day passes, and a deliberately-stale day_position fails.

## CLAUDE.md updates

Two clarifications in `CLAUDE.md`:

1. **Data flow → Real-time path:** replace the misleading line
   ```
   → analytics.strategy_pnl_1hour_prod_v2  (hourly snapshot upsert on each flush)
   ```
   with:
   ```
   → analytics.strategy_pnl_1hour_prod_v2  (via MV cascade from 1min INSERTs)
   → analytics.strategy_pnl_1day_prod_v2   (via MV cascade from 1hour INSERTs)
   ```

2. **ClickHouse Patterns:** add a note that daily tables, like hourly, require `FINAL` or `LIMIT 1 BY` reads.

3. **Architecture overview:** add `strategy_pnl_1day_{prod,bt,real_trade}_v2` to the rollup table list in the Batch path section.

## Out of Scope

- No Dagster asset / schedule for daily rollup. The MV cascade is sufficient.
- No partition strategy changes — monthly partitions stay.
- No streaming or pnl_consumer code changes.
- No Grafana dashboard updates in this design — separate work to add 1day datasource auto-selection in `scripts/patch_dashboards_resolution.py` if needed later.

## Implementation order

1. Schema: append daily section to `infra/schemas/clickhouse_cloud.sql` (already applied to ClickHouse Cloud; this commit captures the schema-of-truth).
2. Coverage audit: extend `_PAIRS` in `pnl_coverage_audit.py`; verify behavior in unit test.
3. CLAUDE.md: apply the three clarifications above.
4. Verification: query each daily table with `FINAL`, sanity-check that latest row matches latest hourly observation for a sample strategy.
