# PnL Coverage & Position Audit — Design

**Status:** Draft for review
**Owner:** William
**Date:** 2026-05-26

## Problem

The full-recompute assets (`pnl_prod_v2_full`, `pnl_bt_v2_full`, `pnl_real_trade_v2_full`) recompute a 7-day rolling window per underlying, anchored at `max(ts) - 7d` in the target table ([pnl_strategy_v2.py:94-111](services/dagster/trading_dagster/assets/pnl_strategy_v2.py:94)). The cold-start fallback that uses `min(ts)` from the source table fires **only** when the target has zero rows for that underlying ([pnl_strategy_v2.py:612](services/dagster/trading_dagster/assets/pnl_strategy_v2.py:612)).

Consequence: once *any* row exists for an underlying, the recompute never revisits earlier history. Historical gaps before the rolling window are never backfilled and never detected.

Concrete case observed: `FET` in `strategy_output_history_v2` runs from **2026-03-05**, but `strategy_pnl_1min_prod_v2` starts at **2026-05-02**. Two months of PnL are missing and `_check_output_completeness` ([pnl_strategy_v2.py:196](services/dagster/trading_dagster/assets/pnl_strategy_v2.py:196)) cannot see the gap because it only checks strategies whose bars fall in the current 7-day window.

We need an independent daily check that walks every `(underlying, strategy)` from its true start to "now" and fails the Dagster run on any missing minute or position mismatch.

## Goals

- Detect coverage gaps (start, end, internal holes) per `(underlying, strategy_table_name)` in all six PnL tables.
- Detect position drift between target and source per `(underlying, strategy_table_name)`.
- Fail the Dagster run with a readable summary when violations are found.
- Bound ClickHouse per-query memory to < 100 MB (hard cap 500 MB).
- Run daily; complete in under 10 minutes.

## Non-Goals

- Auto-remediation. The job alerts only; backfills remain a manual Dagster materialization (or a follow-on change to the recompute window logic).
- Live (streaming) monitoring — covered separately by the consumer's own logic.
- PnL value (`cumulative_pnl`) verification. The position-boundary check is a strong proxy; full PnL re-computation in dry-run is out of scope for v1.

## Calculation invariants (what "correct" means)

For each `(underlying U, strategy_table_name S)`:

- **prod** (`strategy_pnl_1min_prod_v2` ← `strategy_output_history_v2`):
  - Each source bar = `argMin(row_json, revision_ts)` — first revision per `(stn, ts)`.
  - First expected output ts = first source bar's `closing_ts = ts + tf_minutes`.
  - Position changes at each bar's `closing_ts`; constant between changes.
- **bt** (`strategy_pnl_1min_bt_v2` ← `strategy_output_history_bt_v2`):
  - Same as prod, but bars sourced from the bt history table.
- **real_trade** (`strategy_pnl_1min_real_trade_v2` ← `strategy_output_history_v2`):
  - All revisions filtered by `(bar_ts, revision_ts) > prev_accepted.(bar_ts, revision_ts)` (see `AnchorState.should_apply_revision`).
  - First expected output ts = first accepted revision's `execution_ts = toStartOfMinute(revision_ts + 59s)`.
  - Position changes at each accepted revision's `execution_ts`.

Common to all three:
- `price` must exist in `analytics.futures_price_1min` at that minute, else the recompute skips it. The audit must subtract these from "expected minutes" or it will produce false positives.
- Last position holds for `tf_minutes` past the last bar/revision's `closing_ts` — the trailing window.

For the `_1hour_` rollup tables: each `(U, S, hour)` slot equals `argMax(value, minute_ts)` over the minutes in `[hour, hour+1h)` in the corresponding 1-min table.

## Approach: Hybrid (Phase 1 cheap, Phase 2 drilldown, Phase 3 position)

### Phase 1 — per-strategy coverage stats (always runs)

For each `(target, U)`, fetch lean projections of source, target, and price data with bounded queries (details in "Query plan" below). Compute in Python per `(U, S)`:

- `expected_min_ts`:
  - prod/bt: `min(source.ts) + tf_minutes` from `Q_src`.
  - real_trade: `min(execution_ts)` from accepted revisions after applying `build_rt_lookup`.
- `expected_max_ts = now() − 5 min` (freshness threshold).
- `expected_minutes = |price_set ∩ [expected_min_ts, expected_max_ts]|`.
- `actual_min_ts`, `actual_max_ts`, `actual_rows` from `Q_stat`.

Flag the strategy if any of:
- `actual_min_ts > expected_min_ts + 5 min` → **start_gap**
- `actual_max_ts < now() − 10 min` → **stale_end** (i.e., `expected_max_ts − 5 min`; combined 10-min tolerance covers freshness threshold + clock skew)
- `actual_rows < expected_minutes` → **internal_holes** (no tolerance; price gaps already subtracted)

### Phase 2 — gap drilldown (only on flagged strategies)

The per-strategy `Q_gap` query is run upfront alongside `Q_trans` because both share the same window-function cost and `Q_gap` results are tiny. For each flagged strategy, walk `Q_gap` rows to identify the worst gaps and report the top 10 by duration, subtracting any minutes covered by missing prices.

### Phase 3 — position correctness

**For 1-min tables (boundary diff):**

For each `(target_1min, U, S)`:

1. Build `source_changes`: list of `(effective_ts, position)` at transitions. prod/bt: from `Q_src`'s first-revision bars, emit at `closing_ts` whenever position differs from previous. real_trade: from `build_rt_lookup`-filtered revisions, emit at `execution_ts`.
2. Build `target_changes`: from `Q_trans`, already filtered to transition rows.
3. Walk both lists with two pointers. Mismatch criteria:
   - Length differs.
   - At index `i`: `|source.effective_ts − target.ts| > 1 min` OR `source.position != target.position`.

**For 1-hour rollup tables (slot-level position match):**

Boundary diff is not meaningful because position is `argMax(position, minute_ts)` over `[hour, hour+1h)` — multiple intra-hour transitions collapse to one. Instead, for each `(U, S, hour)` row in the 1-hour table, verify `position` equals the position carried at the latest minute `≤ hour + 1h` per `Q_trans` of the corresponding 1-min table. The 1-hour audit reuses the per-stn `Q_trans` result already fetched during the 1-min audit for the same `(U, S)` — no additional ClickHouse query.

### Architecture

- Single Dagster asset `pnl_coverage_audit_asset` in `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`.
- Registered in `services/dagster/trading_dagster/definitions/__init__.py`.
- Scheduled via a new `pnl_coverage_audit_schedule` (cron `0 6 * * *`, UTC) alongside `pnl_hourly_rollup_schedule`.
- Read-only — never writes to PnL tables.
- Parallelism: `ThreadPoolExecutor(max_workers=4)` over `(target_table, underlying)` units; matches the existing pattern in `pnl_strategy_v2.py`.

## Query plan

Source-table sharing:
- `Q_src` for prod and `Q_src_rt` for real_trade both read `analytics.strategy_output_history_v2`, but cannot share results — prod needs `argMin` first revision, real_trade needs all revisions. Issued separately per U.
- `Q_src` for bt reads `analytics.strategy_output_history_bt_v2` — distinct table.
- `Q_px` reads `analytics.futures_price_1min` and is fetched **once per U**, reused for all three target tables.

Per `(source_table, U)`:

```sql
-- Q_src (prod/bt source: first revision only)
SELECT
  strategy_table_name,
  ts,
  argMin(JSONExtract(row_json, 'position', 'Float64'), revision_ts) AS position,
  any(JSONExtractString(row_json, 'config_timeframe')) AS tf
FROM analytics.{source_table}
WHERE underlying = {U}
  AND strategy_table_name NOT LIKE 'manual_probe%'
GROUP BY strategy_table_name, ts
ORDER BY strategy_table_name, ts
SETTINGS max_memory_usage = 524288000
```

```sql
-- Q_src_rt (real_trade source: all revisions, filter in Python)
SELECT
  strategy_table_name,
  ts,
  revision_ts,
  JSONExtract(row_json, 'position', 'Float64') AS position,
  JSONExtractString(row_json, 'config_timeframe') AS tf
FROM analytics.strategy_output_history_v2
WHERE underlying = {U}
  AND strategy_table_name NOT LIKE 'manual_probe%'
ORDER BY strategy_table_name, ts, revision_ts
SETTINGS max_memory_usage = 524288000
```

```sql
-- Q_px (price minutes for U, shared across all 3 target tables)
SELECT ts
FROM analytics.futures_price_1min
WHERE instrument = {U}USDT
  AND ts >= {global_start_ts}
ORDER BY ts
SETTINGS max_memory_usage = 524288000
```

Per `(target_table, U)`:

```sql
-- Q_stat
SELECT strategy_table_name, min(ts), max(ts), count()
FROM analytics.{target_table}
WHERE underlying = {U}
GROUP BY strategy_table_name
SETTINGS max_memory_usage = 524288000
```

Per `(target_table, U, S)` — issued for **every** strategy returned by `Q_stat`:

```sql
-- Q_trans (position transitions)
SELECT ts, position
FROM (
  SELECT ts, position,
         lagInFrame(position) OVER (ORDER BY ts) AS prev_pos,
         row_number()         OVER (ORDER BY ts) AS rn
  FROM analytics.{target_table}
  WHERE underlying = {U} AND strategy_table_name = {S}
)
WHERE position != prev_pos OR rn = 1
ORDER BY ts
SETTINGS max_memory_usage = 524288000
```

```sql
-- Q_gap (ts gaps > 60s, only when target row count > 1)
SELECT ts AS gap_end, ts_secs - prev_ts_secs AS gap_secs
FROM (
  SELECT ts,
         toUnixTimestamp(ts) AS ts_secs,
         lagInFrame(toUnixTimestamp(ts)) OVER (ORDER BY ts) AS prev_ts_secs
  FROM analytics.{target_table}
  WHERE underlying = {U} AND strategy_table_name = {S}
)
WHERE prev_ts_secs > 0 AND ts_secs - prev_ts_secs > 60
ORDER BY gap_secs DESC
LIMIT 50
SETTINGS max_memory_usage = 524288000
```

## Memory bound per query

| Query | Granularity | Peak rows scanned | Peak CH memory |
|---|---|---|---|
| `Q_stat` | per-U | up to ~14 M (ETH) | < 1 MB (GROUP BY state for ≤ ~150 groups) |
| `Q_src` | per-U | ~200 k | ~5 MB (argMin state holds a scalar) |
| `Q_src_rt` | per-U | ~200 k | ~10 MB (lean projection) |
| `Q_px` | per-U | ~165 k | < 5 MB |
| `Q_trans` | per-stn | ~100 k | < 5 MB (single window partition) |
| `Q_gap` | per-stn | ~100 k | < 5 MB |

Every query carries `SETTINGS max_memory_usage = 524288000` (500 MB hard cap) — defense in depth.

## Failure reporting

The asset collects all violations into `defaultdict(list)`. On completion:

- If any violations: `context.log.info(...)` the full detail, then raise `RuntimeError` with a per-table summary (top 10 worst per category).
- If clean: emit `MaterializeResult(metadata=...)` with row counts and check durations.

Summary format:

```
PnL COVERAGE & POSITION AUDIT FAILED

[strategy_pnl_1min_prod_v2]
  Phase 1 (coverage):   47 / 150 strategies flagged
  Phase 2 (gaps):       12,341,290 missing minutes (excl. price gaps)
  Phase 3 (positions):  0 / 150 strategies with mismatches
  Top 10 worst:
    FET sid=19|sno=279|…  start_gap   expected=2026-03-05 09:01  actual=2026-05-02 20:44  (58d 11h)
    FET sid=19|sno=276|…  start_gap   …
    AVAX sid=…             stale_end   expected=2026-05-26 12:00  actual=2026-05-26 11:23  (37m)
    …

[strategy_pnl_1min_real_trade_v2]
  …
```

## Component layout

```
services/dagster/trading_dagster/assets/pnl_coverage_audit.py
├── pnl_coverage_audit_asset                                       (asset entry point)
├── _audit_table(target_table, source_table, mode, …)              (per-target driver)
├── _audit_underlying(target_table, source_table, mode, U, px_set) (per-(target, U) driver)
├── _fetch_q_src_prod_bt(source_table, U, client)                  (Q_src)
├── _fetch_q_src_rt(U, client)                                     (Q_src_rt)
├── _fetch_q_px(U, global_start_ts, client)                        (Q_px)
├── _fetch_q_stat(target_table, U, client)                         (Q_stat)
├── _fetch_q_trans(target_table, U, S, client)                     (Q_trans)
├── _fetch_q_gap(target_table, U, S, client)                       (Q_gap)
├── _compute_source_changes_prod_bt(bars_for_strategy, tf_minutes) (phase 3 source side)
├── _compute_source_changes_rt(revs_for_strategy, tf_minutes)      (uses build_rt_lookup)
├── _check_phase1(U, S, actual_*, expected_*, px_set)              (returns Violation | None)
├── _check_phase2(gaps, px_set)                                    (returns list of gap descriptors)
├── _check_phase3(source_changes, target_changes)                  (returns Mismatch | None)
└── _format_report(violations) → str
```

Existing helpers reused from `libs/computation/`:
- `build_rt_lookup` — applies the `(bar_ts, revision_ts) > prev_accepted` acceptance rule.
- `TIMEFRAME_MAP` — `tf` string → minutes.

## Schedule & wiring

```python
pnl_coverage_audit_job = define_asset_job(
    name="pnl_coverage_audit_job",
    selection=["pnl_coverage_audit"],
)

pnl_coverage_audit_schedule = ScheduleDefinition(
    name="pnl_coverage_audit_schedule",
    cron_schedule="0 6 * * *",
    job=pnl_coverage_audit_job,
    execution_timezone="UTC",
)
```

The asset is **not** added to the existing 30-second automation sensor. It runs only on its own schedule.

## Testing

Unit tests in `services/dagster/tests/test_pnl_coverage_audit.py`. ClickHouse calls are mocked via `monkeypatch` (same pattern as `test_pnl_compute.py`).

- `test_phase1_detects_start_gap` — actual_min > expected_min → flagged.
- `test_phase1_detects_stale_end` — actual_max < now − 5m → flagged.
- `test_phase1_detects_internal_holes` — actual_rows < expected_minutes → flagged.
- `test_phase1_price_gap_exemption` — missing minutes that align with price gaps are not counted.
- `test_phase2_finds_worst_gaps` — `Q_gap` rows are sorted by duration.
- `test_phase3_position_match_prod` — equal sequences → pass.
- `test_phase3_position_mismatch_count` — different lengths → fail.
- `test_phase3_position_mismatch_value` — same length, differing position → fail.
- `test_phase3_position_ts_within_tolerance` — ts within 1 min → pass.
- `test_phase3_rt_uses_accepted_revisions` — discarded revisions don't appear in source_changes.
- `test_audit_reports_top_n` — summary contains the worst 10 entries.

## Open questions

None at this stage.

## Out of scope (potential follow-ups)

- Fix the underlying root cause: extend `_get_underlying_resume_dt` to detect historical gaps and trigger a full backfill, not just a 7-day rolling recompute. This audit job alerts the operator to do a manual full-recompute for now.
- Slack / PagerDuty alerting on Dagster failure — not required for the initial cut.
- PnL value verification (full dry-run recompute and value diff).
