# `scripts/audit_pnl.py` — Consolidated PnL Audit & Repair

**Status:** Approved (pending user review of this doc)
**Date:** 2026-05-30
**Replaces:** `backfill_fet_prices_and_prod.py`, `backfill_prod_missing_strategies.py`, `backfill_rt_missing_strategies.py`, `backfill_rt_midgap_strategies.py`, `backfill_rt_window_gap.py`
**Complements:** `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (Dagster scheduled asset stays as-is)

## Goal

One CLI script that audits the PnL pipeline for prod / bt / real_trade — finds missing strategies, coverage gaps, position errors, and 1h↔1min drift — and optionally repairs them. Consolidates five backfill scripts into one tool with a unified report.

Benchmark is explicitly out of scope for v1; a separate discussion will scope it.

## CLI

```
python scripts/audit_pnl.py [options]

  --type {prod,bt,real_trade,all}    Default: all
  --underlying SYM                    Filter to one underlying (default: all)
  --fix                               Apply fixes for detected issues
  --fix-window 'START' 'END'          Explicit-window repair (--type required)
                                      START/END format 'YYYY-MM-DD HH:MM:SS'
  --report PATH                       Markdown report path
                                      (default: ./audit_reports/<UTC-ts>.md)
  --state-file PATH                   JSONL run-history file
                                      (default: ./audit_reports/history.jsonl)
  --show-history [N]                  Print the last N runs from the state
                                      file and exit (default 5). No audit.
  --no-pause                          Skip ECS consumer pause/resume (--fix)
  --dry-run                           Detect + compute fixes, no writes
```

Exit code: `0` clean, `1` violations found (whether or not `--fix` was applied).

## Checks (per type)

| # | Check | Detection (per strategy) | Applies to |
|---|---|---|---|
| 1 | Missing strategy | source has rows, target has zero | prod, bt, rt |
| 2 | Start gap | `min(target.ts) > min(source.execution_ts) + 2h` | prod, bt, rt |
| 3 | Mid-history gap | `count(DISTINCT toStartOfDay(ts)) < dateDiff('day', min(ts), max(ts)) + 1` in target | prod, bt, rt |
| 4 | Position mismatch | Per-minute `target.position != active_source.position` (streamed, sampled) | prod, bt, rt |
| 5 | 1h ↔ 1min sync | For each hour slot in target_hour: `position != latest 1min position with effective_ts ≤ hour + 1h` | prod, bt, rt |
| 6 | Stale end | `max(target.ts) < now() - 10m` | prod, rt (skipped for bt) |

Detection SQL and position-streaming logic are lifted directly from `pnl_coverage_audit.py`. No new computation primitives in `libs/`.

### Per-strategy `failure_ts` (for repair)

When `--fix` runs, each affected strategy gets one `failure_ts` = `min(failure_ts)` across all categories that fired for it:

| Category | `failure_ts` |
|---|---|
| Missing strategy | source's first `execution_ts` |
| Start gap | source's first `execution_ts` |
| Mid-history gap | first missing minute in the earliest hole (detected with `lagInFrame` over target ts) |
| Position mismatch | earliest mismatched ts (first sample from streaming check) |
| Stale end | `max(target.ts) + 1m` |
| 1h ↔ 1min drift | first mismatched hour slot's `ts` |

## Fix strategy — differs by type

### Prod / real_trade: `[failure_ts, now]` per strategy

`cumulative_pnl` chains forward from the previous anchor, so any error from `failure_ts` poisons everything after it. Fix must re-derive forward to now.

Per strategy:
1. **Seed anchor** — read `(cumulative_pnl, price, position)` from the most recent target row at `ts < failure_ts`. Zero anchor (`pnl=0, price=first_available_price, position=0`) if no prior row exists.
2. **Delete forward** — `DELETE FROM <target> WHERE strategy_table_name = X AND ts >= failure_ts`.
3. **Recompute** — fetch source bars for the strategy in `[failure_ts, now]`, build the appropriate lookup (`build_prod_lookup` or `build_rt_lookup`), walk minute-by-minute with `AnchorState` seeded from step 1, emit rows via `build_pnl_row`.
4. **Insert** — batched (50k rows) via `client.insert(..., column_names=INSERT_COLUMNS)`.

Per type:
- Pause the matching consumer ONCE per run (`pnl-consumer-prod` or `pnl-consumer-real-trade`), repair all affected strategies of that type, refresh the hour table, then resume.
- Multiple underlyings batched in one consumer-pause window.

### BT: `[window_start, window_end]` per strategy

`cumulative_pnl` is extracted from source `row_json`, so errors don't propagate forward. Fix is window-local.

Per strategy:
1. Derive `[window_start, window_end]` from the violation:
   - Mid-history gap → `[first_missing_minute, last_missing_minute]`
   - Position mismatch → `[earliest_mismatched_ts, latest_mismatched_ts_in_sample]`
   - Start gap / missing → `[first_source_execution_ts, now]`
2. `DELETE FROM strategy_pnl_1min_bt_v2 WHERE strategy_table_name = X AND ts >= window_start AND ts <= window_end`.
3. Recompute that window with `fetch_new_bars_bt` + `compute_bt_pnl` (which extracts `cumulative_pnl` from `row_json`). Insert.
4. No anchor seeding required. No consumer pause (no streaming consumer for bt).

### Hour-table refresh

Per type, after all 1-min writes complete (or unconditionally when only hour-sync failures fired and there were no 1-min fixes):
```sql
DELETE FROM <hour_table> WHERE ts >= toStartOfHour(toDateTime('<earliest_failure_ts>'))
INSERT INTO <hour_table> SELECT ... argMax(...) FROM <min_table> WHERE ts >= ... GROUP BY ..., toStartOfHour(minute_ts)
```
Scope: minimum `failure_ts` (prod/rt) or minimum `window_start` (bt) across all repaired strategies of that type. SQL pattern reused verbatim from the existing backfill scripts.

### `--fix-window 'START' 'END' --type TYPE`

Escape hatch for incident-driven repairs where the operator already knows the time range. Detects strategies below 90% coverage in the window (current `backfill_rt_window_gap.py` logic, generalized to all types):

- **prod / rt**: seed-anchor + carry-forward within the window only. Existing rows outside the window are untouched.
- **bt**: same as auto-fix but with operator-supplied window. No anchor seeding.

Mutually exclusive with auto-`--fix` in one run; pick one mode.

## Edge cases

| Case | Handling |
|---|---|
| Strategy never had a target row | Seed anchor is zero; `failure_ts` = source's first `execution_ts`. |
| `failure_ts` precedes source's first bar | Clamp `failure_ts` up to source's first `execution_ts`. |
| Multiple categories fire for one strategy | Take `min(failure_ts)` across them, single delete-and-recompute pass. |
| Source has no bars in `[failure_ts, now]` | Strategy went silent; log warning, skip recompute. |
| Position-mismatch sample window is the whole table | Cost-cap detection at TOP_N samples (as `pnl_coverage_audit.py` already does); `failure_ts` = earliest sample, recompute repairs the tail correctly even if the sample only spans early minutes. |
| Consumer pause fails | Abort fix run; report what would have been fixed. |
| Manual_probe strategies | Excluded everywhere (`strategy_table_name NOT LIKE 'manual_probe%'`). |
| Naive datetime / UTC bug | Borrow `_parse_ts_utc` from `backfill_rt_window_gap.py` to keep tz-aware datetimes on insert paths. |

## Report

Two outputs, always written together:

**Console** — grouped summary:
```
PnL AUDIT — 2026-05-30 14:30:00 UTC

[prod]   12 strategies / 3 underlyings — 2 violations
  internal_holes: 1 strategy (BTC, my_strat_v3, 4320m missing from 2026-05-25 06:00)
  position_mismatch: 1 strategy (ETH, alpha_v1, 8 mismatches starting 2026-05-29 12:34)
[bt]     220 strategies / 18 underlyings — clean
[rt]     45 strategies / 5 underlyings — clean

→ Wrote audit_report_20260530_143000.md
→ Exit 1 (violations found, no --fix specified)
```

**Markdown file** — one section per type, then per (underlying, strategy) entry with:
- Detected categories + per-category `detail`
- Computed `failure_ts` and proposed fix window
- The exact SQL the fix would execute (or did execute, with row counts)

## Persistent state across runs

To answer "what did we check last time / what did we fix" without re-running the audit, the script appends one JSON line to `./audit_reports/history.jsonl` per run. The file is git-ignored.

Each line is a complete, self-describing record:

```json
{
  "run_id": "20260530_143000_abc1",
  "started_at": "2026-05-30T14:30:00Z",
  "finished_at": "2026-05-30T14:34:12Z",
  "duration_secs": 252.0,
  "mode": "audit",                       // "audit" | "fix" | "fix-window" | "dry-run"
  "scope": {
    "types": ["prod", "bt", "real_trade"],
    "underlying": null,                  // null = all
    "fix_window": null                   // [start, end] when --fix-window
  },
  "totals": {
    "strategies_checked": 277,
    "violations": 2,
    "rows_fixed": 0
  },
  "by_type": {
    "prod":       {"strategies": 12,  "violations": 2, "rows_fixed": 0},
    "bt":         {"strategies": 220, "violations": 0, "rows_fixed": 0},
    "real_trade": {"strategies": 45,  "violations": 0, "rows_fixed": 0}
  },
  "violations": [
    {
      "type": "prod",
      "underlying": "BTC",
      "strategy_table_name": "my_strat_v3",
      "categories": ["internal_holes"],
      "failure_ts": "2026-05-25 06:00:00",
      "detail": "4320m missing from 2026-05-25 06:00",
      "fix_applied": false
    },
    {
      "type": "prod",
      "underlying": "ETH",
      "strategy_table_name": "alpha_v1",
      "categories": ["position_mismatch"],
      "failure_ts": "2026-05-29 12:34:00",
      "detail": "8 mismatches starting 2026-05-29 12:34",
      "fix_applied": false
    }
  ],
  "report_path": "./audit_reports/20260530_143000.md"
}
```

JSONL (one JSON object per line) is chosen over a single rolling JSON array so concurrent runs can't corrupt the file and so `tail`/`grep`/`jq` work line-by-line.

### `--show-history [N]` output

Reads the last N records and renders a one-line-per-run table — useful at the start of a session to recall recent state:

```
RUN                  WHEN                  MODE   STRATS  VIOL  FIXED  TYPES               REPORT
20260530_143000_abc1 2026-05-30 14:30 UTC  audit  277     2     0      prod,bt,real_trade  audit_reports/20260530_143000.md
20260529_090000_def2 2026-05-29 09:00 UTC  fix    275     3     8421   prod,real_trade     audit_reports/20260529_090000.md
20260528_180000_ghi3 2026-05-28 18:00 UTC  audit  274     0     0      prod,bt,real_trade  audit_reports/20260528_180000.md
```

The audit run itself also prints "Last run: 2026-05-29 09:00 UTC — 3 violations, 8421 rows fixed" at the top of its console output when a prior state file exists, so the operator immediately sees deltas.

### Retention

No automatic pruning. The file is line-oriented and small (~1KB per clean run, ~10KB per noisy one); manual `head -n -1000 history.jsonl` if it ever gets unwieldy.

## Code layout

Single file, ~600 lines. No new shared library — detection SQL and fix loops are copy-adapted from existing scripts/asset.

```
scripts/audit_pnl.py
├── Config: SOURCE/TARGET/HOUR/LABEL maps keyed by type
├── ECS helpers: pause_consumer(type), resume_consumer(type), get_boto_client
├── Detection (one function per check):
│   find_missing_or_start_gap(type, client) -> {U: {S: failure_ts}}
│   find_midgap(type, client) -> {U: {S: (window_start, window_end)}}
│   find_stale_end(type, client) -> {U: {S: failure_ts}}
│   audit_positions(type, U, S, client) -> sample mismatches (streaming)
│   audit_hour_sync(type, U, client) -> per-strategy mismatched hour slots
├── Per-strategy failure_ts aggregator
│   resolve_failure_ts(violations_for_strategy) -> ts | window
├── Fix loops:
│   fix_prod_rt_strategies(type, U, [(S, failure_ts)], client) -> int rows
│   fix_bt_strategies(U, [(S, window)], client) -> int rows
│   refresh_hour_table(type, earliest_ts, client)
├── Report rendering:
│   render_console(report) -> str
│   render_markdown(report, path) -> None
└── main():
    1. Build full AuditReport across requested types (parallel by type/U)
    2. If --fix or --fix-window: pause → fix → refresh hour → resume
    3. Always: write markdown + print console
    4. Exit 1 if any violation existed
```

Imports from `libs.computation`: `AnchorRecord`, `AnchorState`, `INSERT_COLUMNS`, `build_prod_lookup`, `build_rt_lookup`, `active_prod_bar_at`, `active_rt_revision_at`, `fetch_new_bars_prod`, `fetch_new_bars_real_trade`, `fetch_new_bars_bt`, `fetch_prices_multi`, `build_pnl_row`, `compute_bt_pnl`, `first_active_minute`, `last_active_minute`, `TIMEFRAME_MAP`.

ClickHouse access: standalone `clickhouse_connect.get_client(...)` (matches existing scripts; the script runs outside Dagster).

## Files deleted by this change

- `scripts/backfill_fet_prices_and_prod.py`
- `scripts/backfill_prod_missing_strategies.py`
- `scripts/backfill_rt_missing_strategies.py`
- `scripts/backfill_rt_midgap_strategies.py`
- `scripts/backfill_rt_window_gap.py`

## Files NOT touched

- `scripts/local_pnl_test.py` (smoke test, unrelated)
- `scripts/patch_dashboards_resolution.py` (Grafana, unrelated)
- `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (daily Dagster check stays — read-only, raises on violations, complements the new script)

## Out of scope (v1)

- Benchmark type — separate discussion will define source/target.
- Auto-fix for `cumulative_pnl` divergence beyond what position-correctness implies (we don't yet have a numeric-tolerance check on the chained value itself; could add in v2 if drift escapes the position check).
- Slack/email notification — operator runs script manually; report is on console + markdown.
- Continuous scheduling — Dagster asset already covers daily detection; this script is on-demand.
