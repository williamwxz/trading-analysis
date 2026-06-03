# Grafana Dashboard 1day Resolution — Design

**Date:** 2026-06-02
**Status:** Approved, implementation pending

## Goal

Extend the existing 2-branch (`1min` + `1hour`) Grafana panel resolution to a 3-branch (`1min` + `1hour` + `1day`) pattern. After this change, panels read the daily PnL rollup (`analytics.strategy_pnl_1day_*_v2`, created in commit `8449be0`) for any portion of a query range that falls more than 30 days in the past — reducing scan volume on long-range views while preserving 1min fidelity for the most recent 6 hours.

## Non-Goals

- No mixed-resolution refinement (e.g., argMax-merge across branches). Branches are time-disjoint and complete; UNION ALL is sufficient.
- No dashboard-level variable for the cutoff. The 30d threshold is hardcoded, matching the existing 6h hardcode.
- No changes to `aws-billing.json` or `streaming-infra.json` — they have zero PnL references.
- No schema or audit changes — the daily tables and audit wiring are already in place from the daily-PnL-rollup PR.

## Architecture

### Target SQL pattern

Every PnL family reference in a panel query becomes:

```sql
(SELECT * FROM analytics.strategy_pnl_1min_<family>[ FINAL]
   WHERE ts >= toStartOfHour(now() - INTERVAL 6 HOUR)
 UNION ALL
 SELECT * FROM analytics.strategy_pnl_1hour_<family>[ FINAL]
   WHERE ts >= toStartOfDay(now() - INTERVAL 30 DAY)
     AND ts < toStartOfHour(now() - INTERVAL 6 HOUR)
 UNION ALL
 SELECT * FROM analytics.strategy_pnl_1day_<family>[ FINAL]
   WHERE ts < toStartOfDay(now() - INTERVAL 30 DAY))
```

`<family>` is `prod_v2`, `bt_v2`, or `real_trade_v2`. The optional ` FINAL` suffix is preserved wherever it existed pre-patch.

### Boundary semantics

The three WHERE clauses are mutually exclusive and gapless. Reading row-by-row:

| Row ts | 1min branch | 1hour branch | 1day branch |
|---|---|---|---|
| `ts ≥ now-6h` (hour-aligned) | ✓ | — | — |
| `now-30d ≤ ts < now-6h` (day-aligned) | — | ✓ | — |
| `ts < now-30d` (day-aligned) | — | — | ✓ |

The day-aligned cut at the 30d mark prevents partial-day double-counting (a single calendar day is served by exactly one of `1hour` or `1day`, never both). The hour-aligned cut at the 6h mark is unchanged from the current 2-branch implementation.

### Query behavior at different range widths

- **last 6h:** only the 1min branch returns rows.
- **last 7d:** 1min for the last 6h, 1hour for the prior ~6.75d. Identical to current behavior.
- **last 60d:** 1min for the last 6h, 1hour for ~6h to 30d ago, 1day for ~30d to 60d ago. Saves ~30× scan volume vs. reading 1hour for the whole range.
- **last 1y:** 1min for the last 6h, 1hour for ~6h to 30d, 1day for the prior ~335d. Bt's 6yr history especially benefits — current 2-branch path reads 1hour (20M rows); new path reads 1day (865k rows).

## Patcher logic

`scripts/patch_dashboards_resolution.py` becomes the source of truth for the 3-branch form. It detects three input states and converges them all:

1. **3-branch UNION ALL** (already canonical) — skip.
2. **2-branch UNION ALL** (current deployed shape: 1min `ts ≥ toStartOfHour(now() - INTERVAL 6 HOUR)` UNION ALL 1hour `ts < toStartOfHour(now() - INTERVAL 6 HOUR)`) — upgrade to 3-branch.
3. **Bare reference** (`analytics.strategy_pnl_1{min,hour}_<family>[ FINAL]` not yet wrapped) — wrap into 3-branch.

Detection regex applied in this order; each regex's lookahead ensures it doesn't fire inside an already-converted subquery. The 3-branch regex returns the original unchanged; the 2-branch regex captures `<family>` and ` FINAL` (optional) and rewrites; the bare regex captures the same groups for newly-introduced references (defense for any future panels added by hand without re-running the patcher).

The patcher walks every panel's `targets[*].rawSql`, including nested panels (rows containing panels). It writes back to disk only if at least one substitution happened, to keep a clean no-op when re-run.

## File scope

| File | Change |
|---|---|
| `scripts/patch_dashboards_resolution.py` | Rewrite regex layer, `_union_sql` helper, detection/idempotency logic. |
| `infra/grafana/dashboards/strategy-pnl-l1-instance.json` | Re-run patcher; commit resulting diff. |
| `infra/grafana/dashboards/strategy-pnl-l2-sid-underlying.json` | Re-run patcher; commit resulting diff. |
| `infra/grafana/dashboards/strategy-pnl-l3-sid.json` | Re-run patcher; commit resulting diff. |
| `infra/grafana/dashboards/strategy-pnl-l4-underlying.json` | Re-run patcher; commit resulting diff. |
| `infra/grafana/dashboards/strategy-pnl-l5-portfolio.json` | Re-run patcher; commit resulting diff. |
| `services/dagster/tests/test_patch_dashboards_resolution.py` (new) | Unit tests covering the three input states + idempotency. |
| `infra/grafana/dashboards/aws-billing.json` | Untouched — no PnL refs. |
| `infra/grafana/dashboards/streaming-infra.json` | Untouched — no PnL refs. |

`scripts/patch_dashboards_resolution.py` becomes the source of truth — any future drift between script and JSON is a bug, fixed by re-running the script.

## Deployment

The committed JSON is automatically pushed to Grafana Cloud by the existing `deploy-grafana-cloud` job in `.github/workflows/ci-cd.yml` on merge to `main`. No manual `terraform apply` or dashboard import is needed.

## Testing

New test file `services/dagster/tests/test_patch_dashboards_resolution.py`. The patcher exposes a single pure entry point `patch_sql(raw_sql: str) -> tuple[str, int]` that returns the rewritten SQL and the number of substitutions performed. All tests exercise that function directly with synthetic SQL strings:

1. **Bare prod ref** — `analytics.strategy_pnl_1min_prod_v2 WHERE ...` rewrites to the 3-branch form.
2. **Bare hour ref** — `analytics.strategy_pnl_1hour_bt_v2 WHERE ...` rewrites to the 3-branch form.
3. **Bare with FINAL** — `analytics.strategy_pnl_1min_real_trade_v2 FINAL WHERE ...` preserves FINAL in all three branches.
4. **2-branch UNION ALL** — the deployed shape rewrites to 3-branch.
5. **2-branch with FINAL** — preserves FINAL across the upgrade.
6. **3-branch idempotency** — a canonical 3-branch input passes through unchanged.
7. **Multiple references in one SQL** — a query with two distinct refs gets both rewritten.
8. **Unrelated SQL** — a query with no PnL refs is unchanged.

No external dependencies (ClickHouse, network); these are pure-function string-rewrite tests.

## Out of Scope

- Variable cutoffs (a Grafana dashboard variable for the 30d threshold).
- Per-row mixed-resolution argMax merging across branches.
- Migration of the patcher to a real SQL parser (regex is sufficient for the current dashboard shape and gives clear test cases).
- Updating CLAUDE.md — there's no doc section about the patcher today, and the script's docstring will reflect the new pattern.

## Implementation order

1. Tests: write the 8 unit tests against the new patcher interface (TDD).
2. Patcher: implement until tests pass.
3. Run the patcher against the 5 dashboards; commit the resulting JSON diff.
4. Manual spot-check: open a patched JSON and verify the 3-branch shape in one panel.
5. CI: confirm `deploy-grafana-cloud` pushes the new JSON on merge.
