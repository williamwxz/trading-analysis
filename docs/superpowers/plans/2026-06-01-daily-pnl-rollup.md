# Daily PnL Rollup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the already-created `strategy_pnl_1day_{prod,bt,real_trade}_v2` tables into the existing PnL coverage audit and update CLAUDE.md to document the new daily layer.

**Architecture:** ClickHouse tables and MVs are already applied + backfilled (commit `8449be0`). Remaining work is Python-only: generalize the bucketed audit helpers in `pnl_coverage_audit.py` so they accept any bucket width (hour or day), add `_DAY_TABLES`, wire it into the audit asset, update unit tests for the renamed helpers, and add new test cases for day buckets. Then update CLAUDE.md to reflect the cascading MV path.

**Tech Stack:** Python 3.11, pytest, Dagster, ClickHouse Cloud. Linting: black (line length 88), ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-06-01-daily-pnl-rollup-design.md`

---

## File Structure

**Modified files:**
- `CLAUDE.md` — three doc edits (data-flow comment, batch path table list, ClickHouse pattern note)
- `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` — rename two helpers (`_check_phase3_hour` → `_check_phase3_bucketed`, `_fetch_q_stat_hour` → `_fetch_q_stat_bucketed`), generalize `_audit_hour_table` → `_audit_bucketed_table` to accept a bucket label/timedelta/expr, add `_DAY_TABLES`, wire `_DAY_TABLES` into the audit asset's loop
- `services/dagster/tests/test_pnl_coverage_audit.py` — update imports and existing call sites for the renames; add a new `TestPhase3Day` test class

**No new files.** No new schema changes. No new Dagster assets or schedules.

---

## Task 1: CLAUDE.md — data flow note

**Files:**
- Modify: `CLAUDE.md` (the "Data Flow → Real-time path" code block)

- [ ] **Step 1: Update the real-time path comment**

Open `CLAUDE.md`. Find the block (under "Data Flow" → "Real-time path"):

```
    → analytics.strategy_pnl_1min_prod_v2
    → analytics.strategy_pnl_1hour_prod_v2  (hourly snapshot upsert on each flush)
```

Replace with:

```
    → analytics.strategy_pnl_1min_prod_v2
    → analytics.strategy_pnl_1hour_prod_v2  (via MV cascade from 1min INSERTs)
    → analytics.strategy_pnl_1day_prod_v2   (via MV cascade from 1hour INSERTs)
```

The change reflects two facts: (a) pnl_consumer does not write to the 1hour table directly — the MV does, and (b) the 1day table now exists and is populated by an analogous cascading MV.

- [ ] **Step 2: Update the Batch path's table list**

Find the "Batch path" block. Locate the line:

```
PnL refresh assets (full recompute, every ~5 min, rolling 7-day window)
    → analytics.strategy_pnl_1min_{prod,bt,real_trade}_v2

Rollup asset (hourly, argMax aggregation)
    → analytics.strategy_pnl_1hour_{prod,bt,real_trade}_v2
```

Append a new line after the Rollup block:

```
Daily rollup (MV cascade — no asset)
    → analytics.strategy_pnl_1day_{prod,bt,real_trade}_v2
```

- [ ] **Step 3: Update the ClickHouse Patterns note**

Find the "ClickHouse Patterns" section. The bullet about `ReplacingMergeTree(updated_at)` mentions querying with `FINAL` or `LIMIT 1 BY`. Append one sentence to that bullet:

> Same applies to the 1hour and 1day rollup tables.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(CLAUDE.md): document daily PnL rollup tables and cascading MV path"
```

Expected: clean commit, no test run needed (doc-only).

---

## Task 2: Refactor `_check_phase3_hour` → `_check_phase3_bucketed`

Generalize the per-bucket position check to accept the bucket width as a `timedelta` parameter so the same function can verify daily slots.

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (lines 300–341)
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py` (imports at line 19; calls at lines 343, 349, 357)

- [ ] **Step 1: Update existing hour-bucket tests to call the new name with explicit bucket**

Edit `services/dagster/tests/test_pnl_coverage_audit.py`:

Change the import block at the top (line 19):

```python
    _check_phase3_hour,
```

to:

```python
    _check_phase3_bucketed,
```

Then update the three call sites in `class TestPhase3Hour` (currently `_check_phase3_hour("t1h", "FET", "S", hour_rows, target_min_changes)` at lines 343, 349, 357) to:

```python
v = _check_phase3_bucketed(
    "t1h", "FET", "S", hour_rows, target_min_changes,
    bucket=timedelta(hours=1),
)
```

(`timedelta` is already imported at the top of the test file — line 3.)

- [ ] **Step 2: Run the existing hour tests to verify they fail (function doesn't exist yet)**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Hour -v
```

Expected: `ImportError: cannot import name '_check_phase3_bucketed'` — the rename hasn't happened in the source yet.

- [ ] **Step 3: Rename and parameterize the function in the source**

Edit `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`. Replace lines 300–341:

```python
def _check_phase3_hour(
    table: str,
    underlying: str,
    stn: str,
    hour_rows: list[tuple[datetime, float]],
    target_min_changes: list[PositionChange],
) -> Violation | None:
    """For each (U, S, hour) row in the 1-hour table, verify position equals the
    latest 1-min change at minute <= hour + 1h.

    hour_rows: list of (hour_ts, position) from the 1-hour target table.
    target_min_changes: position-change sequence from the corresponding 1-min table.
    """
    if not target_min_changes:
        return None
    # Sorted ascending by effective_ts in both inputs.
    for hour_ts, hour_position in hour_rows:
        cutoff = hour_ts + timedelta(hours=1)
        # Find the latest min-change with effective_ts < cutoff.
        latest: PositionChange | None = None
        for change in target_min_changes:
            if change.effective_ts < cutoff:
                latest = change
            else:
                break
        if latest is None:
            # No prior 1-min change — cannot verify this slot, skip silently.
            continue
        if latest.position != hour_position:
            return Violation(
                table=table,
                underlying=underlying,
                stn=stn,
                category="position_mismatch",
                detail=(
                    f"hour slot {hour_ts:%Y-%m-%d %H:%M:%S}: "
                    f"hour_position={hour_position} != latest_min_position={latest.position} "
                    f"(latest min change at {latest.effective_ts:%Y-%m-%d %H:%M:%S})"
                ),
                severity_minutes=1,
            )
    return None
```

with:

```python
def _check_phase3_bucketed(
    table: str,
    underlying: str,
    stn: str,
    bucket_rows: list[tuple[datetime, float]],
    target_min_changes: list[PositionChange],
    bucket: timedelta,
) -> Violation | None:
    """For each (U, S, bucket) row in a rolled-up PnL table, verify position
    equals the latest 1-min change at minute < bucket_ts + bucket.

    Used for both 1hour (bucket=1h) and 1day (bucket=1d) audits.

    bucket_rows: list of (bucket_ts, position) from the rolled-up target table.
    target_min_changes: position-change sequence from the corresponding 1-min table.
    bucket: width of one slot in the target table.
    """
    if not target_min_changes:
        return None
    bucket_label = "hour" if bucket == timedelta(hours=1) else (
        "day" if bucket == timedelta(days=1) else f"{int(bucket.total_seconds() // 60)}m"
    )
    # Sorted ascending by effective_ts in both inputs.
    for bucket_ts, bucket_position in bucket_rows:
        cutoff = bucket_ts + bucket
        # Find the latest min-change with effective_ts < cutoff.
        latest: PositionChange | None = None
        for change in target_min_changes:
            if change.effective_ts < cutoff:
                latest = change
            else:
                break
        if latest is None:
            # No prior 1-min change — cannot verify this slot, skip silently.
            continue
        if latest.position != bucket_position:
            return Violation(
                table=table,
                underlying=underlying,
                stn=stn,
                category="position_mismatch",
                detail=(
                    f"{bucket_label} slot {bucket_ts:%Y-%m-%d %H:%M:%S}: "
                    f"bucket_position={bucket_position} != latest_min_position={latest.position} "
                    f"(latest min change at {latest.effective_ts:%Y-%m-%d %H:%M:%S})"
                ),
                severity_minutes=1,
            )
    return None
```

- [ ] **Step 4: Run hour tests, verify they pass**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Hour -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "refactor(audit): parameterize _check_phase3_hour bucket width"
```

---

## Task 3: Refactor `_fetch_q_stat_hour` → `_fetch_q_stat_bucketed`

Generalize the per-bucket row fetcher so the same function can scan 1hour or 1day tables.

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (lines 790–806, plus the single call site at line 979)

This is a private helper with no direct test coverage, so no test changes are needed for this step — coverage is preserved by the integration through `_audit_hour_table` (and by the new day tests added in Task 5).

- [ ] **Step 1: Rename and add no-op pass-through param**

Edit `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`. Replace the existing `_fetch_q_stat_hour` (lines 790–806):

```python
def _fetch_q_stat_hour(hour_table: str, underlying: str, client) -> dict[str, list[tuple[datetime, float]]]:
    """Q_stat variant for hour tables — returns per-strategy (ts, position) rows."""
    rows = query_rows(
        f"""
SELECT strategy_table_name, ts, position
FROM analytics.{hour_table}
WHERE underlying = '{underlying}'
  AND ts >= toDateTime('{GLOBAL_START_TS}')
ORDER BY strategy_table_name, ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    out: dict[str, list[tuple[datetime, float]]] = {}
    for r in rows:
        out.setdefault(str(r[0]), []).append((r[1], float(r[2])))
    return out
```

with:

```python
def _fetch_q_stat_bucketed(
    bucket_table: str, underlying: str, client
) -> dict[str, list[tuple[datetime, float]]]:
    """Q_stat variant for rolled-up tables (1hour or 1day) — returns per-strategy
    (ts, position) rows. The bucket ts is already pre-aggregated in the source
    table, so no toStartOfHour / toStartOfDay expression is needed here.
    """
    rows = query_rows(
        f"""
SELECT strategy_table_name, ts, position
FROM analytics.{bucket_table}
WHERE underlying = '{underlying}'
  AND ts >= toDateTime('{GLOBAL_START_TS}')
ORDER BY strategy_table_name, ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    out: dict[str, list[tuple[datetime, float]]] = {}
    for r in rows:
        out.setdefault(str(r[0]), []).append((r[1], float(r[2])))
    return out
```

- [ ] **Step 2: Update the single existing call site**

In the same file at line 979, change:

```python
hr_by_stn = _fetch_q_stat_hour(hour_table, u, client)
```

to:

```python
hr_by_stn = _fetch_q_stat_bucketed(hour_table, u, client)
```

(Task 4 will replace this whole block.)

- [ ] **Step 3: Run the full audit test module to verify no regression**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py -v
```

Expected: all tests pass (same as before; rename is internal).

- [ ] **Step 4: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py
git commit -m "refactor(audit): rename _fetch_q_stat_hour to _fetch_q_stat_bucketed"
```

---

## Task 4: Generalize `_audit_hour_table` → `_audit_bucketed_table`

Merge the hour-specific driver into a bucket-parameterized version so the same function can drive 1hour and 1day audits.

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (lines 965–986 — the `_audit_hour_table` function; lines 1020–1028 — the call site in the asset)

- [ ] **Step 1: Replace `_audit_hour_table` with the bucketed driver**

Edit `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`. Replace lines 965–986:

```python
def _audit_hour_table(
    hour_table: str,
    min_table: str,
    client,
) -> AuditReport:
    """Run hour-slot position check (Phase 3 variant) for one 1-hour table.

    Coverage checks (Phase 1) for the 1-hour table are derived from the 1-min
    presence — we don't repeat Phase 1 here. We only verify the argMax
    position at each hour slot matches the latest preceding minute change.
    """
    aggregate = AuditReport()
    underlyings = _list_underlyings(hour_table, client)
    for u in underlyings:
        hr_by_stn = _fetch_q_stat_bucketed(hour_table, u, client)
        for stn, rows in hr_by_stn.items():
            target_min_changes = _fetch_q_trans(min_table, u, stn, client)
            v = _check_phase3_hour(hour_table, u, stn, rows, target_min_changes)
            aggregate.strategies_checked += 1
            if v is not None:
                aggregate.violations.append(v)
    return aggregate
```

with:

```python
def _audit_bucketed_table(
    bucket_table: str,
    min_table: str,
    bucket: timedelta,
    client,
) -> AuditReport:
    """Run bucket-slot position check (Phase 3 variant) for one rolled-up table.

    Coverage checks (Phase 1) for rolled-up tables are derived from the 1-min
    presence — we don't repeat Phase 1 here. We only verify the argMax
    position at each bucket slot matches the latest preceding minute change.

    Used for both 1hour (bucket=1h) and 1day (bucket=1d) audits.
    """
    aggregate = AuditReport()
    underlyings = _list_underlyings(bucket_table, client)
    for u in underlyings:
        rows_by_stn = _fetch_q_stat_bucketed(bucket_table, u, client)
        for stn, rows in rows_by_stn.items():
            target_min_changes = _fetch_q_trans(min_table, u, stn, client)
            v = _check_phase3_bucketed(
                bucket_table, u, stn, rows, target_min_changes, bucket=bucket
            )
            aggregate.strategies_checked += 1
            if v is not None:
                aggregate.violations.append(v)
    return aggregate
```

- [ ] **Step 2: Update the hour-loop call site to pass the bucket**

Edit the asset body. The current loop (lines 1020–1028) reads:

```python
    for hour, min_t, _mode in _HOUR_TABLES:
        context.log.info(f"[audit] starting hour table {hour}")
        sub = _audit_hour_table(hour_table=hour, min_table=min_t, client=get_client())
        context.log.info(
            f"[audit] {hour}: {sub.strategies_checked} strategies, {len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1
```

Replace with:

```python
    for hour, min_t, _mode in _HOUR_TABLES:
        context.log.info(f"[audit] starting hour table {hour}")
        sub = _audit_bucketed_table(
            bucket_table=hour,
            min_table=min_t,
            bucket=timedelta(hours=1),
            client=get_client(),
        )
        context.log.info(
            f"[audit] {hour}: {sub.strategies_checked} strategies, {len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1
```

- [ ] **Step 3: Run the audit tests to confirm no regression**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py
git commit -m "refactor(audit): merge _audit_hour_table into bucketed driver"
```

---

## Task 5: Add `_DAY_TABLES` + wire daily audit into the asset

Add the daily triples and a second loop in the audit asset that audits the 1day tables.

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (constants block after `_HOUR_TABLES` ~ line 79; asset body after the hour loop ~ line 1029)
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py` (add `TestPhase3Day` class)

- [ ] **Step 1: Write the failing day-bucket tests**

In `services/dagster/tests/test_pnl_coverage_audit.py`, add this class immediately after the existing `class TestPhase3Hour` block (which ends around line 358):

```python
# ── Phase 3 (day table): slot position match ─────────────────────────────────


class TestPhase3Day:
    def test_day_slot_matches_latest_minute_change(self):
        """For each day slot, position should equal the latest 1-min position <= day+1d."""
        # 1-min target had changes at 09:05 → 1.0, 23:30 → -1.0
        # Day slot 2026-03-05 00:00 should reflect -1.0
        # (because 23:30 is the latest minute_ts in [2026-03-05, 2026-03-06))
        target_min_changes = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 23:30:00"), -1.0),
        ]
        day_rows = [
            (_dt("2026-03-05 00:00:00"), -1.0),  # correct
        ]
        v = _check_phase3_bucketed(
            "t1d", "FET", "S", day_rows, target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is None

    def test_day_slot_position_drift_fails(self):
        target_min_changes = [PositionChange(_dt("2026-03-05 23:30:00"), -1.0)]
        day_rows = [(_dt("2026-03-05 00:00:00"), 1.0)]  # wrong
        v = _check_phase3_bucketed(
            "t1d", "FET", "S", day_rows, target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is not None
        assert v.category == "position_mismatch"
        assert "day slot" in v.detail

    def test_day_slot_change_in_next_day_does_not_affect_prior_day(self):
        """A change at 2026-03-06 00:30 must NOT affect the 2026-03-05 day slot."""
        target_min_changes = [
            PositionChange(_dt("2026-03-05 12:00:00"), 1.0),
            PositionChange(_dt("2026-03-06 00:30:00"), -1.0),
        ]
        day_rows = [(_dt("2026-03-05 00:00:00"), 1.0)]  # correct: 12:00 change wins
        v = _check_phase3_bucketed(
            "t1d", "FET", "S", day_rows, target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is None

    def test_day_slot_with_no_prior_minute_change_skips(self):
        target_min_changes = [PositionChange(_dt("2026-03-06 10:00:00"), 1.0)]
        day_rows = [(_dt("2026-03-05 00:00:00"), 0.0)]  # no prior min change
        v = _check_phase3_bucketed(
            "t1d", "FET", "S", day_rows, target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is None  # can't verify, skip silently
```

- [ ] **Step 2: Run the new tests to verify they pass**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Day -v
```

Expected: 4 passed. (`_check_phase3_bucketed` already supports day buckets — these tests are sanity coverage, not driving new code.)

- [ ] **Step 3: Add `_DAY_TABLES` constant**

Edit `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`. Find the `_HOUR_TABLES` block (around line 74):

```python
_HOUR_TABLES: list[tuple[str, str, Mode]] = [
    ("strategy_pnl_1hour_prod_v2", "strategy_pnl_1min_prod_v2", "prod"),
    ("strategy_pnl_1hour_bt_v2", "strategy_pnl_1min_bt_v2", "bt"),
    ("strategy_pnl_1hour_real_trade_v2", "strategy_pnl_1min_real_trade_v2", "real_trade"),
]
```

Immediately after it, add:

```python
_DAY_TABLES: list[tuple[str, str, Mode]] = [
    ("strategy_pnl_1day_prod_v2", "strategy_pnl_1min_prod_v2", "prod"),
    ("strategy_pnl_1day_bt_v2", "strategy_pnl_1min_bt_v2", "bt"),
    ("strategy_pnl_1day_real_trade_v2", "strategy_pnl_1min_real_trade_v2", "real_trade"),
]
```

Note: `min_table` for the day tables is the 1-min table (not the 1-hour table). Phase 3 verifies position against the canonical minute-level transition log, which is the same source the hour audit uses. The 1-hour table is not the source of truth for positions.

- [ ] **Step 4: Wire the day loop into the asset**

Edit the asset body. Immediately after the hour loop (which now ends around line 1037 after Task 4's change), add:

```python
    for day, min_t, _mode in _DAY_TABLES:
        context.log.info(f"[audit] starting day table {day}")
        sub = _audit_bucketed_table(
            bucket_table=day,
            min_table=min_t,
            bucket=timedelta(days=1),
            client=get_client(),
        )
        context.log.info(
            f"[audit] {day}: {sub.strategies_checked} strategies, {len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1
```

- [ ] **Step 5: Run the full audit test module to verify everything still passes**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py -v
```

Expected: all tests pass (previous suite + 4 new in `TestPhase3Day`).

- [ ] **Step 6: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): include 1day PnL tables in coverage audit"
```

---

## Task 6: Lint, type-check, and integration sanity

Run the project's standard quality checks before declaring done.

**Files:** none modified in this task.

- [ ] **Step 1: black**

```bash
black services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
```

Expected: either "All done!" or "reformatted N files". If any reformatting happens, commit it:

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "style: black"
```

- [ ] **Step 2: ruff**

```bash
ruff check --fix services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
```

Expected: "All checks passed!" If any auto-fixes apply, commit them:

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "style: ruff"
```

- [ ] **Step 3: mypy**

```bash
mypy services/dagster/trading_dagster/assets/pnl_coverage_audit.py
```

Expected: "Success: no issues found". If mypy reports errors, fix the type annotations inline (most likely candidate: an `Optional[timedelta]` or wrong return type on the bucketed helpers) and re-run.

- [ ] **Step 4: Run the audit test suite one more time end-to-end**

```bash
pytest services/dagster/tests/test_pnl_coverage_audit.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Run the full unit test suite to verify no upstream breakage**

```bash
pytest -m unit
```

Expected: all unit tests pass.

- [ ] **Step 6: ClickHouse spot check (read-only)**

Verify the daily tables are receiving cascading writes. Set `CH_PW` to the value of secret `/production-v3/prod/clickhouse-password` first, then run:

```bash
CH_PW='<password>' python3 - <<'PY'
import os, clickhouse_connect
c = clickhouse_connect.get_client(
    host="qu6o0w9hl8.ap-northeast-1.aws.clickhouse.cloud",
    port=8443, username="default",
    password=os.environ["CH_PW"], secure=True,
)
for v in ["prod", "bt", "real_trade"]:
    h = c.query(
        f"SELECT max(ts) FROM analytics.strategy_pnl_1hour_{v}_v2"
    ).result_rows[0][0]
    d = c.query(
        f"SELECT max(ts) FROM analytics.strategy_pnl_1day_{v}_v2"
    ).result_rows[0][0]
    print(f"{v}: 1hour max ts={h}, 1day max ts={d}")
PY
```

Expected: for each variant, `1day max ts` is `toStartOfDay(1hour max ts)` (i.e. the same UTC day, hour = 00:00:00). This confirms the MV cascade is firing.

---

## Self-review checklist (already applied)

1. **Spec coverage:**
   - Tables / MVs: already applied (commit `8449be0`).
   - Backfill: already applied.
   - Coverage audit extension: Tasks 2–5.
   - CLAUDE.md clarifications: Task 1.
   - Verification: Task 6 step 6.
2. **Placeholder scan:** none.
3. **Type consistency:** `_check_phase3_bucketed`, `_fetch_q_stat_bucketed`, `_audit_bucketed_table`, `_DAY_TABLES` used consistently across all tasks.
4. **Spec ambiguity removed:** Task 5 step 3 explicitly notes the day-table audit sources from the 1-min table (not the 1-hour table), matching the spec's clarification that Phase 3 always verifies against the canonical 1-min transition log.
