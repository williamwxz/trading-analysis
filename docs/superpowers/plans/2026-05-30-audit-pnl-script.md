# `scripts/audit_pnl.py` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single CLI script that audits prod / bt / real_trade PnL tables for missing strategies, coverage gaps, position errors, and 1h↔1min drift — optionally repairs them (prod/rt forward from `failure_ts`, bt window-local) — and persists per-run history to JSONL.

**Architecture:** One self-contained script (`scripts/audit_pnl.py`, ~600 lines). Detection SQL lifted from `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`. Fix loops lifted from the five backfill scripts being deleted. All shared computation imported from `libs/computation`. Pure-Python helpers are unit-tested; SQL/ECS sides are tested by running the script in `--dry-run` against ClickHouse.

**Tech Stack:** Python 3.11, `clickhouse-connect`, `boto3` (ECS pause/resume), `libs.computation`, pytest.

**Spec:** [`docs/superpowers/specs/2026-05-30-audit-pnl-script-design.md`](../specs/2026-05-30-audit-pnl-script-design.md)

---

## File Structure

```
scripts/audit_pnl.py                                — new (~600 LOC). Single file:
    section A: constants, dataclasses, ECS / CH helpers
    section B: detection functions (one per check)
    section C: failure_ts aggregator
    section D: fix functions (prod_rt + bt)
    section E: state file (JSONL) + history rendering
    section F: report rendering (console + markdown)
    section G: CLI + main()

tests/test_audit_pnl.py                             — new. Unit tests for pure helpers:
    failure_ts aggregator, state JSONL read/write, report rendering,
    bt window derivation, history table rendering.

scripts/backfill_fet_prices_and_prod.py             — delete
scripts/backfill_prod_missing_strategies.py         — delete
scripts/backfill_rt_missing_strategies.py           — delete
scripts/backfill_rt_midgap_strategies.py            — delete
scripts/backfill_rt_window_gap.py                   — delete

.gitignore                                          — already adds audit_reports/
                                                      (committed with the spec)
```

Tests follow the existing pattern in `services/dagster/tests/test_pnl_coverage_audit.py`: import private helpers, feed in-memory fixtures, assert on returned dataclasses. No ClickHouse needed for unit tests; integration is covered by running the script in `--dry-run` against the cloud cluster manually.

---

## Task 1: Scaffold the script + constants + dataclasses

**Files:**
- Create: `scripts/audit_pnl.py`

- [ ] **Step 1: Create the file with section-A scaffolding**

Create `scripts/audit_pnl.py` with the following content (exactly as shown). This is the file's spine; later tasks fill in B–G.

```python
"""Consolidated PnL audit & repair for prod / bt / real_trade.

Audits the three pnl_1min / pnl_1hour table families for missing strategies,
coverage gaps, position errors, and 1h↔1min drift; optionally repairs them.

Prod / real_trade: fix forward from each strategy's failure_ts (cumulative_pnl
chains forward, so any earlier error poisons the tail). BT: fix window-local
(cumulative_pnl comes from row_json, not from a chained anchor).

Persists one JSONL record per run to ./audit_reports/history.jsonl so a later
run can summarize prior state via --show-history.

Spec: docs/superpowers/specs/2026-05-30-audit-pnl-script-design.md
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Literal, Optional

import boto3
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from libs.computation import (  # noqa: E402
    INSERT_COLUMNS,
    AnchorRecord,
    AnchorState,
    active_prod_bar_at,
    active_rt_revision_at,
    build_pnl_row,
    build_prod_lookup,
    build_rt_lookup,
    compute_bt_pnl,
    fetch_new_bars_bt,
    fetch_new_bars_prod,
    fetch_new_bars_real_trade,
    fetch_prices_multi,
    first_active_minute,
    last_active_minute,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── section A: constants ────────────────────────────────────────────────────

Mode = Literal["prod", "bt", "real_trade"]
TYPES: tuple[Mode, ...] = ("prod", "bt", "real_trade")

SOURCE_TABLE = {
    "prod": "strategy_output_history_v2",
    "bt": "strategy_output_history_bt_v2",
    "real_trade": "strategy_output_history_v2",
}
TARGET_TABLE = {
    "prod": "strategy_pnl_1min_prod_v2",
    "bt": "strategy_pnl_1min_bt_v2",
    "real_trade": "strategy_pnl_1min_real_trade_v2",
}
HOUR_TABLE = {
    "prod": "strategy_pnl_1hour_prod_v2",
    "bt": "strategy_pnl_1hour_bt_v2",
    "real_trade": "strategy_pnl_1hour_real_trade_v2",
}
SOURCE_LABEL = {"prod": "production", "bt": "backtest", "real_trade": "real_trade"}
ECS_SERVICE = {
    "prod": "trading-analysis-pnl-consumer-prod",
    "real_trade": "trading-analysis-pnl-consumer-real-trade",
}
ECS_CLUSTER = "trading-analysis"
ECS_REGION = "ap-northeast-1"
DEFAULT_AWS_PROFILE = "AdministratorAccess-068704208855"

GLOBAL_START_TS = "2026-02-27 00:00:00"
START_TOLERANCE_HOURS = 2
STALE_END_TOLERANCE_MIN = 10
POSITION_SAMPLE_CAP = 50
BATCH_SIZE = 50_000
DEFAULT_REPORT_DIR = Path("./audit_reports")

# ── section A: dataclasses ──────────────────────────────────────────────────

Category = Literal[
    "missing_strategy",
    "start_gap",
    "mid_gap",
    "position_mismatch",
    "hour_sync",
    "stale_end",
]


@dataclass
class Violation:
    type: Mode
    underlying: str
    strategy_table_name: str
    category: Category
    detail: str
    failure_ts: Optional[datetime] = None  # earliest impacted minute
    window_end: Optional[datetime] = None  # bt-only: explicit upper bound


@dataclass
class StrategyFix:
    """One resolved repair action for one strategy."""
    type: Mode
    underlying: str
    strategy_table_name: str
    failure_ts: datetime
    window_end: Optional[datetime] = None  # bt-only; None means "to now"
    categories: list[Category] = field(default_factory=list)
    fix_applied: bool = False
    rows_written: int = 0


@dataclass
class TypeSummary:
    type: Mode
    strategies_checked: int = 0
    violations: int = 0
    rows_fixed: int = 0


@dataclass
class AuditReport:
    started_at: datetime
    finished_at: Optional[datetime] = None
    mode: str = "audit"  # "audit" | "fix" | "fix-window" | "dry-run"
    scope_types: list[Mode] = field(default_factory=list)
    scope_underlying: Optional[str] = None
    scope_fix_window: Optional[tuple[datetime, datetime]] = None
    violations: list[Violation] = field(default_factory=list)
    fixes: list[StrategyFix] = field(default_factory=list)
    by_type: dict[Mode, TypeSummary] = field(default_factory=dict)
    report_path: Optional[Path] = None
    run_id: str = ""

    def has_violations(self) -> bool:
        return bool(self.violations)


# ── section A: ECS + CH client helpers ──────────────────────────────────────


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
    )


def get_ecs_client():
    profile = os.environ.get("AWS_PROFILE", DEFAULT_AWS_PROFILE)
    return boto3.Session(profile_name=profile).client("ecs", region_name=ECS_REGION)


def pause_consumer(ecs, type_: Mode) -> None:
    if type_ not in ECS_SERVICE:
        return
    svc = ECS_SERVICE[type_]
    log.info("Pausing %s", svc)
    ecs.update_service(cluster=ECS_CLUSTER, service=svc, desiredCount=0)
    ecs.get_waiter("services_stable").wait(cluster=ECS_CLUSTER, services=[svc])


def resume_consumer(ecs, type_: Mode) -> None:
    if type_ not in ECS_SERVICE:
        return
    svc = ECS_SERVICE[type_]
    log.info("Resuming %s", svc)
    ecs.update_service(cluster=ECS_CLUSTER, service=svc, desiredCount=1)


def _parse_ts(s) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def _parse_ts_utc(s) -> datetime:
    """tz-aware UTC; required for clickhouse-connect inserts so the host's local
    offset doesn't silently shift every row by hours."""
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _utcnow_naive() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


# ── section B: detection (Task 2) ───────────────────────────────────────────
# ── section C: failure_ts aggregator (Task 3) ───────────────────────────────
# ── section D: fix functions (Tasks 4–5) ────────────────────────────────────
# ── section E: state file + history (Task 6) ────────────────────────────────
# ── section F: report rendering (Task 7) ────────────────────────────────────
# ── section G: CLI + main (Task 8) ──────────────────────────────────────────


if __name__ == "__main__":
    raise SystemExit("audit_pnl.py main() not implemented yet — see Task 8")
```

- [ ] **Step 2: Verify the file imports cleanly**

Run: `python -c "import scripts.audit_pnl"`
Expected: no output (clean import). If it fails on a missing env var, that's fine — the env loader runs on import — but ImportErrors must be fixed before continuing.

Note: if `python -c` fails because `scripts/` isn't on sys.path, use:
`PYTHONPATH=. python -c "import scripts.audit_pnl"`

- [ ] **Step 3: Commit**

```bash
git add scripts/audit_pnl.py
git commit -m "feat(audit_pnl): scaffold script with constants and dataclasses"
```

---

## Task 2: Detection functions

**Files:**
- Modify: `scripts/audit_pnl.py` (append to section B)
- Create: `tests/test_audit_pnl.py`

- [ ] **Step 1: Create the test file with a fixture for one violation set**

Create `tests/test_audit_pnl.py`:

```python
"""Unit tests for audit_pnl pure-Python helpers (no ClickHouse needed)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from scripts.audit_pnl import (
    AuditReport,
    Category,
    StrategyFix,
    TypeSummary,
    Violation,
)


def _dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _v(category: Category, ts: str = "2026-05-29 12:34:00", **kw) -> Violation:
    base = dict(
        type="prod",
        underlying="BTC",
        strategy_table_name="my_strat_v3",
        category=category,
        detail="test",
        failure_ts=_dt(ts),
    )
    base.update(kw)
    return Violation(**base)
```

- [ ] **Step 2: Add the failure_ts aggregator test (this drives Task 3 too, but the test belongs with the helper it exercises)**

Append to `tests/test_audit_pnl.py`:

```python
class TestResolveFailureTs:
    def test_single_violation_returns_its_failure_ts(self):
        from scripts.audit_pnl import resolve_strategy_fix

        v = _v("start_gap", ts="2026-05-29 12:00:00")
        fix = resolve_strategy_fix([v])
        assert fix.failure_ts == _dt("2026-05-29 12:00:00")
        assert fix.categories == ["start_gap"]
        assert fix.window_end is None

    def test_multiple_categories_take_min_failure_ts(self):
        from scripts.audit_pnl import resolve_strategy_fix

        early = _v("position_mismatch", ts="2026-05-25 06:00:00")
        late = _v("mid_gap", ts="2026-05-29 12:00:00")
        fix = resolve_strategy_fix([early, late])
        assert fix.failure_ts == _dt("2026-05-25 06:00:00")
        assert set(fix.categories) == {"position_mismatch", "mid_gap"}

    def test_bt_violation_with_window_end_propagates(self):
        from scripts.audit_pnl import resolve_strategy_fix

        v = _v("mid_gap", ts="2026-05-25 06:00:00")
        v.type = "bt"
        v.window_end = _dt("2026-05-25 09:00:00")
        fix = resolve_strategy_fix([v])
        assert fix.window_end == _dt("2026-05-25 09:00:00")

    def test_multiple_window_ends_take_max(self):
        from scripts.audit_pnl import resolve_strategy_fix

        v1 = _v("mid_gap", ts="2026-05-25 06:00:00")
        v1.type = "bt"
        v1.window_end = _dt("2026-05-25 09:00:00")
        v2 = _v("mid_gap", ts="2026-05-25 07:00:00")
        v2.type = "bt"
        v2.window_end = _dt("2026-05-25 10:00:00")
        fix = resolve_strategy_fix([v1, v2])
        assert fix.window_end == _dt("2026-05-25 10:00:00")

    def test_empty_list_raises(self):
        from scripts.audit_pnl import resolve_strategy_fix

        with pytest.raises(ValueError):
            resolve_strategy_fix([])
```

- [ ] **Step 3: Run the tests to confirm they fail (resolve_strategy_fix not defined yet)**

Run: `pytest tests/test_audit_pnl.py::TestResolveFailureTs -v`
Expected: 5 ImportError / AttributeError failures.

- [ ] **Step 4: Append detection-function bodies to `scripts/audit_pnl.py` (section B)**

Insert this block immediately after the `# ── section B: detection ...` marker:

```python
def find_missing_or_start_gap(
    type_: Mode, client, underlying_filter: Optional[str] = None
) -> list[Violation]:
    """Strategies absent from target, or whose target starts >2h after source."""
    src = SOURCE_TABLE[type_]
    tgt = TARGET_TABLE[type_]
    where_u = f" AND src.underlying = '{underlying_filter}'" if underlying_filter else ""
    sql = f"""
    WITH
    src AS (
        SELECT
            strategy_table_name,
            any(underlying)                                    AS underlying,
            min(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS first_exec_ts
        FROM analytics.{src}
        WHERE strategy_table_name NOT LIKE 'manual_probe%'
          AND ts >= toDateTime('{GLOBAL_START_TS}')
        GROUP BY strategy_table_name
    ),
    tgt AS (
        SELECT strategy_table_name, min(ts) AS first_tgt_ts
        FROM analytics.{tgt}
        WHERE ts >= toDateTime('{GLOBAL_START_TS}')
        GROUP BY strategy_table_name
    )
    SELECT src.strategy_table_name, src.underlying, src.first_exec_ts, tgt.first_tgt_ts
    FROM src LEFT JOIN tgt USING (strategy_table_name)
    WHERE (
        tgt.strategy_table_name IS NULL
        OR tgt.first_tgt_ts > src.first_exec_ts + INTERVAL {START_TOLERANCE_HOURS} HOUR
    ){where_u}
    ORDER BY src.underlying, src.strategy_table_name
    """
    out: list[Violation] = []
    for stn, und, first_exec, first_tgt in client.query(sql).result_rows:
        if first_tgt is None:
            cat: Category = "missing_strategy"
            detail = f"absent from {tgt}; source starts {first_exec}"
        else:
            cat = "start_gap"
            gap_min = int((first_tgt - first_exec).total_seconds() // 60)
            detail = f"target starts {first_tgt}, source starts {first_exec} ({gap_min}m gap)"
        out.append(
            Violation(
                type=type_, underlying=str(und), strategy_table_name=str(stn),
                category=cat, detail=detail, failure_ts=first_exec,
            )
        )
    return out


def find_midgap(
    type_: Mode, client, underlying_filter: Optional[str] = None
) -> list[Violation]:
    """Strategies whose covered-days count is less than (max-min+1) — at least one
    full day in the target window has zero rows. failure_ts is the first missing
    minute in the earliest hole; window_end is the last missing minute in that
    hole (bt uses both; prod/rt uses only failure_ts)."""
    tgt = TARGET_TABLE[type_]
    where_u = f" AND underlying = '{underlying_filter}'" if underlying_filter else ""

    # Phase 1: which strategies have any mid-history gap.
    flag_sql = f"""
    SELECT strategy_table_name, any(underlying) AS underlying
    FROM analytics.{tgt}
    WHERE ts >= toDateTime('{GLOBAL_START_TS}'){where_u}
    GROUP BY strategy_table_name
    HAVING count(DISTINCT toStartOfDay(ts)) < dateDiff('day', min(ts), max(ts)) + 1
    ORDER BY underlying, strategy_table_name
    """
    flagged = [(str(s), str(u)) for s, u in client.query(flag_sql).result_rows]

    # Phase 2: for each flagged strategy, find the first/last minute of the earliest hole.
    out: list[Violation] = []
    for stn, und in flagged:
        gap_sql = f"""
        SELECT gap_start, gap_end FROM (
            SELECT
                addMinutes(prev_ts, 1)                          AS gap_start,
                addMinutes(ts, -1)                              AS gap_end,
                ts                                              AS this_ts,
                lagInFrame(ts) OVER (ORDER BY ts)               AS prev_ts,
                dateDiff('minute',
                         lagInFrame(ts) OVER (ORDER BY ts), ts) AS gap_minutes
            FROM analytics.{tgt}
            WHERE strategy_table_name = '{stn}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
        )
        WHERE gap_minutes > 1
        ORDER BY gap_start
        LIMIT 1
        """
        rows = client.query(gap_sql).result_rows
        if not rows:
            continue
        gap_start, gap_end = rows[0]
        gap_min = int((gap_end - gap_start).total_seconds() // 60) + 1
        out.append(
            Violation(
                type=type_, underlying=und, strategy_table_name=stn,
                category="mid_gap",
                detail=f"{gap_min}m missing from {gap_start}",
                failure_ts=gap_start,
                window_end=gap_end,
            )
        )
    return out


def find_stale_end(
    type_: Mode, client, now_ts: datetime, underlying_filter: Optional[str] = None
) -> list[Violation]:
    """Strategies whose target's max(ts) is older than now - 10m. Skipped for bt."""
    if type_ == "bt":
        return []
    tgt = TARGET_TABLE[type_]
    where_u = f" AND underlying = '{underlying_filter}'" if underlying_filter else ""
    threshold = now_ts - timedelta(minutes=STALE_END_TOLERANCE_MIN)
    sql = f"""
    SELECT strategy_table_name, any(underlying), max(ts)
    FROM analytics.{tgt}
    WHERE ts >= toDateTime('{GLOBAL_START_TS}'){where_u}
    GROUP BY strategy_table_name
    HAVING max(ts) < toDateTime('{threshold:%Y-%m-%d %H:%M:%S}')
    ORDER BY 1
    """
    out: list[Violation] = []
    for stn, und, max_ts in client.query(sql).result_rows:
        stale_min = int((now_ts - max_ts).total_seconds() // 60)
        out.append(
            Violation(
                type=type_, underlying=str(und), strategy_table_name=str(stn),
                category="stale_end",
                detail=f"max(ts)={max_ts} ({stale_min}m stale)",
                failure_ts=max_ts + timedelta(minutes=1),
            )
        )
    return out


def audit_positions(
    type_: Mode, client, underlying_filter: Optional[str] = None
) -> list[Violation]:
    """Per-strategy position correctness — first mismatch ts becomes failure_ts.

    Streams the target table in ts order, comparing each row's position to the
    active source-derived position. Bounded by POSITION_SAMPLE_CAP samples per
    strategy.
    """
    tgt = TARGET_TABLE[type_]
    src = SOURCE_TABLE[type_]
    where_u = f" AND underlying = '{underlying_filter}'" if underlying_filter else ""

    # Enumerate (underlying, strategy) pairs to walk.
    pairs_sql = f"""
    SELECT DISTINCT underlying, strategy_table_name
    FROM analytics.{tgt}
    WHERE ts >= toDateTime('{GLOBAL_START_TS}'){where_u}
    """
    pairs = [(str(u), str(s)) for u, s in client.query(pairs_sql).result_rows]
    out: list[Violation] = []

    for und, stn in pairs:
        # Source position transitions for this strategy.
        if type_ == "real_trade":
            src_sql = f"""
            SELECT
                toStartOfMinute(revision_ts + INTERVAL 59 SECOND) AS effective_ts,
                JSONExtractFloat(row_json, 'position') AS position
            FROM analytics.{src}
            WHERE underlying = '{und}' AND strategy_table_name = '{stn}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
            ORDER BY ts, revision_ts
            """
        else:
            src_sql = f"""
            SELECT * FROM (
                SELECT
                    toStartOfMinute(ts + INTERVAL config_timeframe MINUTE) AS effective_ts,
                    JSONExtractFloat(row_json, 'position') AS position
                FROM analytics.{src}
                WHERE underlying = '{und}' AND strategy_table_name = '{stn}'
                  AND ts >= toDateTime('{GLOBAL_START_TS}')
                ORDER BY ts, revision_ts
                LIMIT 1 BY ts
            )
            ORDER BY effective_ts
            """
        src_rows = client.query(src_sql).result_rows
        if not src_rows:
            continue
        transitions: list[tuple[datetime, float]] = []
        prev = None
        for eff, pos in src_rows:
            pos = float(pos)
            if prev is None or pos != prev:
                transitions.append((eff, pos))
                prev = pos

        # Stream target rows and find first mismatch.
        tgt_sql = f"""
        SELECT ts, position FROM analytics.{tgt}
        WHERE underlying = '{und}' AND strategy_table_name = '{stn}'
          AND ts >= toDateTime('{GLOBAL_START_TS}')
        ORDER BY ts
        """
        ptr = -1
        first_mismatch: Optional[datetime] = None
        sample_count = 0
        for ts, actual in client.query(tgt_sql).result_rows:
            while ptr + 1 < len(transitions) and transitions[ptr + 1][0] <= ts:
                ptr += 1
            if ptr < 0:
                continue  # orphan — no source change yet
            expected = transitions[ptr][1]
            if expected != float(actual):
                if first_mismatch is None:
                    first_mismatch = ts
                sample_count += 1
                if sample_count >= POSITION_SAMPLE_CAP:
                    break
        if first_mismatch is not None:
            out.append(
                Violation(
                    type=type_, underlying=und, strategy_table_name=stn,
                    category="position_mismatch",
                    detail=f"{sample_count}+ mismatches starting {first_mismatch}",
                    failure_ts=first_mismatch,
                )
            )
    return out


def audit_hour_sync(
    type_: Mode, client, underlying_filter: Optional[str] = None
) -> list[Violation]:
    """1-hour table's position must equal the 1-min table's latest position with
    effective_ts <= hour + 1h. First mismatched hour slot's ts becomes failure_ts.
    """
    hour = HOUR_TABLE[type_]
    minute = TARGET_TABLE[type_]
    where_u = f" AND underlying = '{underlying_filter}'" if underlying_filter else ""

    pairs_sql = f"""
    SELECT DISTINCT underlying, strategy_table_name
    FROM analytics.{hour}
    WHERE ts >= toDateTime('{GLOBAL_START_TS}'){where_u}
    """
    pairs = [(str(u), str(s)) for u, s in client.query(pairs_sql).result_rows]
    out: list[Violation] = []
    for und, stn in pairs:
        # Per-hour expected position derived in SQL via argMax over the minute table.
        sql = f"""
        WITH hr AS (
            SELECT ts AS hour_ts, position AS hour_pos
            FROM analytics.{hour}
            WHERE underlying = '{und}' AND strategy_table_name = '{stn}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
        ),
        expected AS (
            SELECT
                toStartOfHour(addHours(ts, -1)) AS hour_ts,
                argMax(position, ts)            AS expected_pos
            FROM analytics.{minute}
            WHERE underlying = '{und}' AND strategy_table_name = '{stn}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
            GROUP BY toStartOfHour(addHours(ts, -1))
        )
        SELECT hr.hour_ts, hr.hour_pos, expected.expected_pos
        FROM hr LEFT JOIN expected USING (hour_ts)
        WHERE expected.expected_pos IS NOT NULL
          AND hr.hour_pos != expected.expected_pos
        ORDER BY hr.hour_ts
        LIMIT 1
        """
        rows = client.query(sql).result_rows
        if not rows:
            continue
        hour_ts, hour_pos, expected_pos = rows[0]
        out.append(
            Violation(
                type=type_, underlying=und, strategy_table_name=stn,
                category="hour_sync",
                detail=f"hour {hour_ts}: hour={hour_pos} != minute-argMax={expected_pos}",
                failure_ts=hour_ts,
            )
        )
    return out
```

- [ ] **Step 5: Add a sanity test that the detection module compiles**

Append to `tests/test_audit_pnl.py`:

```python
class TestDetectionImports:
    def test_all_detection_functions_importable(self):
        from scripts.audit_pnl import (
            audit_hour_sync,
            audit_positions,
            find_midgap,
            find_missing_or_start_gap,
            find_stale_end,
        )
        assert callable(find_missing_or_start_gap)
        assert callable(find_midgap)
        assert callable(find_stale_end)
        assert callable(audit_positions)
        assert callable(audit_hour_sync)
```

- [ ] **Step 6: Run the import-sanity test**

Run: `pytest tests/test_audit_pnl.py::TestDetectionImports -v`
Expected: 1 PASS. (The TestResolveFailureTs tests still fail — fixed in Task 3.)

- [ ] **Step 7: Commit**

```bash
git add scripts/audit_pnl.py tests/test_audit_pnl.py
git commit -m "feat(audit_pnl): add detection functions for all six checks"
```

---

## Task 3: `failure_ts` aggregator

**Files:**
- Modify: `scripts/audit_pnl.py` (append to section C)

- [ ] **Step 1: Append `resolve_strategy_fix` to section C**

Insert below the `# ── section C: ...` marker:

```python
def resolve_strategy_fix(violations: list[Violation]) -> StrategyFix:
    """Collapse all violations for ONE strategy into a single StrategyFix.

    Caller guarantees all violations share (type, underlying, strategy_table_name).
    """
    if not violations:
        raise ValueError("resolve_strategy_fix requires at least one violation")
    first = violations[0]
    failure_ts = min((v.failure_ts for v in violations if v.failure_ts is not None), default=None)
    if failure_ts is None:
        raise ValueError(
            f"no violation for {first.type}/{first.underlying}/{first.strategy_table_name} "
            f"has a failure_ts"
        )
    # bt window_end propagates if any violation specified one.
    window_end = max(
        (v.window_end for v in violations if v.window_end is not None),
        default=None,
    )
    return StrategyFix(
        type=first.type,
        underlying=first.underlying,
        strategy_table_name=first.strategy_table_name,
        failure_ts=failure_ts,
        window_end=window_end,
        categories=[v.category for v in violations],
    )


def group_violations_by_strategy(
    violations: Iterable[Violation],
) -> dict[tuple[Mode, str, str], list[Violation]]:
    out: dict[tuple[Mode, str, str], list[Violation]] = {}
    for v in violations:
        key = (v.type, v.underlying, v.strategy_table_name)
        out.setdefault(key, []).append(v)
    return out
```

- [ ] **Step 2: Run the aggregator tests**

Run: `pytest tests/test_audit_pnl.py::TestResolveFailureTs -v`
Expected: 5 PASS.

- [ ] **Step 3: Commit**

```bash
git add scripts/audit_pnl.py
git commit -m "feat(audit_pnl): add failure_ts aggregator"
```

---

## Task 4: Fix function — prod / real_trade forward repair

**Files:**
- Modify: `scripts/audit_pnl.py` (append to section D)
- Modify: `tests/test_audit_pnl.py`

- [ ] **Step 1: Write a unit test for the anchor-seed reader**

Append to `tests/test_audit_pnl.py`:

```python
class TestSeedAnchor:
    """fetch_seed_anchor is a thin SQL wrapper; we test that it returns a
    zero anchor when the query returns no rows, and parses fields correctly
    when it does."""

    def test_zero_anchor_when_no_prior_row(self):
        from scripts.audit_pnl import fetch_seed_anchor

        class FakeClient:
            def query(self, _sql):
                class R: result_rows = []
                return R()

        seed = fetch_seed_anchor(
            type_="prod",
            strategy_table_name="alpha",
            failure_ts=_dt("2026-05-29 12:00:00"),
            client=FakeClient(),
        )
        assert seed == {"pnl": 0.0, "price": 0.0, "position": 0.0}

    def test_seed_from_row(self):
        from scripts.audit_pnl import fetch_seed_anchor

        class FakeClient:
            def query(self, _sql):
                class R: result_rows = [(123.45, 50000.0, 0.5)]
                return R()

        seed = fetch_seed_anchor(
            type_="prod",
            strategy_table_name="alpha",
            failure_ts=_dt("2026-05-29 12:00:00"),
            client=FakeClient(),
        )
        assert seed == {"pnl": 123.45, "price": 50000.0, "position": 0.5}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pytest tests/test_audit_pnl.py::TestSeedAnchor -v`
Expected: 2 failures (`fetch_seed_anchor` not defined).

- [ ] **Step 3: Append `fetch_seed_anchor` + `fix_prod_rt_strategies` to section D**

```python
def fetch_seed_anchor(
    type_: Mode, strategy_table_name: str, failure_ts: datetime, client
) -> dict:
    """Return {pnl, price, position} from the latest target row at ts < failure_ts.

    Returns a zero anchor when no such row exists (strategy starts at failure_ts).
    """
    tgt = TARGET_TABLE[type_]
    sql = f"""
    SELECT
        argMax(cumulative_pnl, (ts, updated_at)) AS pnl,
        argMax(price,          (ts, updated_at)) AS price,
        argMax(position,       (ts, updated_at)) AS position
    FROM analytics.{tgt}
    WHERE strategy_table_name = '{strategy_table_name}'
      AND ts < toDateTime('{failure_ts:%Y-%m-%d %H:%M:%S}')
      AND ts >= toDateTime('{failure_ts:%Y-%m-%d %H:%M:%S}') - INTERVAL 7 DAY
    """
    rows = client.query(sql).result_rows
    if not rows or rows[0][0] is None:
        return {"pnl": 0.0, "price": 0.0, "position": 0.0}
    pnl, price, position = rows[0]
    return {"pnl": float(pnl or 0.0), "price": float(price or 0.0), "position": float(position or 0.0)}


def _prepare_rows_for_insert(rows: list[list]) -> list[list]:
    """ts at INSERT_COLUMNS index 7, updated_at at index 14 must be tz-aware datetimes."""
    for r in rows:
        if isinstance(r[7], str):
            r[7] = _parse_ts_utc(r[7])
        if isinstance(r[14], str):
            r[14] = _parse_ts_utc(r[14])
    return rows


def fix_prod_rt_strategies(
    type_: Mode, fixes: list[StrategyFix], client, *, dry_run: bool, now_ts: datetime
) -> None:
    """Repair prod or real_trade strategies forward from each one's failure_ts.

    Mutates each StrategyFix in `fixes` with rows_written and fix_applied.
    """
    assert type_ in ("prod", "real_trade")
    if not fixes:
        return
    src = SOURCE_TABLE[type_]
    tgt = TARGET_TABLE[type_]
    label = SOURCE_LABEL[type_]

    # Group by underlying to share source-bar fetches.
    by_underlying: dict[str, list[StrategyFix]] = {}
    for f in fixes:
        by_underlying.setdefault(f.underlying, []).append(f)

    for underlying, group in sorted(by_underlying.items()):
        earliest = min(f.failure_ts for f in group)
        # If any strategy specifies an explicit window_end (--fix-window path),
        # honor it as the upper bound for source-bar fetch; otherwise walk to now.
        explicit_ends = [f.window_end for f in group if f.window_end is not None]
        upper_end = max(explicit_ends) if explicit_ends else now_ts
        ts_start = earliest.strftime("%Y-%m-%d %H:%M:%S")
        ts_end = (upper_end + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        log.info("[%s/%s] repair window [%s, %s) for %d strategies",
                 type_, underlying, ts_start, ts_end, len(group))

        if type_ == "real_trade":
            bars = fetch_new_bars_real_trade(src, underlying, ts_start, ts_end, client)
        else:
            bars = fetch_new_bars_prod(src, underlying, ts_start, ts_end, client)
        stn_set = {f.strategy_table_name for f in group}
        bars = [b for b in bars if b["strategy_table_name"] in stn_set]
        if not bars:
            log.warning("[%s/%s] no source bars in window — skipping", type_, underlying)
            continue

        prices_map = fetch_prices_multi([underlying], ts_start, ts_end, client, extend_minutes=1440)
        prices = prices_map.pop(underlying, {})

        if type_ == "real_trade":
            lookup = build_rt_lookup(bars)
            is_rt = True
        else:
            lookup = build_prod_lookup(bars)
            is_rt = False
        minute_start_all = first_active_minute(lookup, is_rt=is_rt)
        minute_end_all = last_active_minute(lookup, is_rt=is_rt)
        if minute_start_all is None or minute_end_all is None:
            log.warning("[%s/%s] empty lookup — skipping", type_, underlying)
            continue

        # Seed anchors per strategy from the row just before each failure_ts.
        state = AnchorState()
        for f in group:
            seed = fetch_seed_anchor(type_, f.strategy_table_name, f.failure_ts, client)
            state.set(
                f.strategy_table_name,
                AnchorRecord(pnl=seed["pnl"], price=seed["price"], position=seed["position"]),
            )

        if not dry_run:
            # Per strategy: delete forward from its own failure_ts, capped at its
            # window_end when one was supplied (--fix-window path).
            for f in group:
                upper_clause = (
                    f"AND ts <= toDateTime('{f.window_end:%Y-%m-%d %H:%M:%S}')"
                    if f.window_end is not None else ""
                )
                client.command(
                    f"DELETE FROM analytics.{tgt} "
                    f"WHERE strategy_table_name = '{f.strategy_table_name}' "
                    f"  AND ts >= toDateTime('{f.failure_ts:%Y-%m-%d %H:%M:%S}') "
                    f"  {upper_clause}"
                )
            log.info("[%s/%s] deleted forward for %d strategies", type_, underlying, len(group))

        now_str = _utcnow_naive().strftime("%Y-%m-%d %H:%M:%S")
        batch: list[list] = []
        rows_by_stn: dict[str, int] = {f.strategy_table_name: 0 for f in group}

        minute_cur = minute_start_all
        while minute_cur < minute_end_all:
            ts_str = minute_cur.strftime("%Y-%m-%d %H:%M:%S")
            price = prices.get(ts_str)
            if price is None:
                minute_cur += timedelta(minutes=1)
                continue
            for f in group:
                if minute_cur < f.failure_ts:
                    continue  # this strategy's failure_ts hasn't arrived yet
                if f.window_end is not None and minute_cur > f.window_end:
                    continue  # past this strategy's explicit window upper bound
                stn = f.strategy_table_name
                if type_ == "real_trade":
                    entry = active_rt_revision_at(lookup, stn, minute_cur)
                    bar = entry.rev if entry else None
                else:
                    entry = active_prod_bar_at(lookup, stn, minute_cur)
                    bar = entry.bar if entry else None
                if bar is None:
                    continue
                meta = AnchorRecord(
                    strategy_id=bar["strategy_id"],
                    strategy_name=bar["strategy_name"],
                    underlying=bar["underlying"],
                    config_timeframe=bar["config_timeframe"],
                    weighting=bar["weighting"],
                    strategy_instance_id=bar.get("strategy_instance_id", ""),
                    final_signal=bar["final_signal"],
                    benchmark=bar["bar_benchmark"],
                )
                cpnl = state.compute_pnl(stn, price, bar["position"], meta=meta)
                batch.append(build_pnl_row(stn, bar, price, cpnl, label, ts_str, now_str))
                rows_by_stn[stn] += 1
            if len(batch) >= BATCH_SIZE:
                if not dry_run:
                    _prepare_rows_for_insert(batch)
                    client.insert(f"analytics.{tgt}", batch, column_names=INSERT_COLUMNS)
                batch = []
            minute_cur += timedelta(minutes=1)

        if batch:
            if not dry_run:
                _prepare_rows_for_insert(batch)
                client.insert(f"analytics.{tgt}", batch, column_names=INSERT_COLUMNS)

        for f in group:
            f.rows_written = rows_by_stn[f.strategy_table_name]
            f.fix_applied = not dry_run
            log.info("[%s/%s] %s: %d rows", type_, underlying, f.strategy_table_name, f.rows_written)
```

- [ ] **Step 4: Run all unit tests**

Run: `pytest tests/test_audit_pnl.py -v`
Expected: TestSeedAnchor 2 PASS, TestResolveFailureTs 5 PASS, TestDetectionImports 1 PASS = 8 PASS total.

- [ ] **Step 5: Commit**

```bash
git add scripts/audit_pnl.py tests/test_audit_pnl.py
git commit -m "feat(audit_pnl): add prod/real_trade forward-repair fix function"
```

---

## Task 5: Fix function — bt window-local repair

**Files:**
- Modify: `scripts/audit_pnl.py` (append to section D)

- [ ] **Step 1: Append `fix_bt_strategies` to section D**

```python
def fix_bt_strategies(
    fixes: list[StrategyFix], client, *, dry_run: bool, now_ts: datetime
) -> None:
    """Repair bt strategies window-locally. No anchor seeding (bt extracts
    cumulative_pnl from row_json). No consumer pause (no streaming consumer)."""
    if not fixes:
        return
    src = SOURCE_TABLE["bt"]
    tgt = TARGET_TABLE["bt"]
    label = SOURCE_LABEL["bt"]

    by_underlying: dict[str, list[StrategyFix]] = {}
    for f in fixes:
        by_underlying.setdefault(f.underlying, []).append(f)

    for underlying, group in sorted(by_underlying.items()):
        for f in group:
            window_start = f.failure_ts
            window_end = f.window_end if f.window_end is not None else now_ts
            ts_start = window_start.strftime("%Y-%m-%d %H:%M:%S")
            ts_end = (window_end + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
            log.info("[bt/%s] %s: window [%s, %s)", underlying, f.strategy_table_name, ts_start, ts_end)

            bars = fetch_new_bars_bt(src, underlying, ts_start, ts_end, client)
            bars = [b for b in bars if b["strategy_table_name"] == f.strategy_table_name]
            if not bars:
                log.warning("[bt/%s] no source bars in window — skipping", underlying)
                continue

            prices_map = fetch_prices_multi([underlying], ts_start, ts_end, client, extend_minutes=1440)
            prices = prices_map.pop(underlying, {})

            if not dry_run:
                client.command(
                    f"DELETE FROM analytics.{tgt} "
                    f"WHERE strategy_table_name = '{f.strategy_table_name}' "
                    f"  AND ts >= toDateTime('{ts_start}') "
                    f"  AND ts <  toDateTime('{ts_end}')"
                )

            now_str = _utcnow_naive().strftime("%Y-%m-%d %H:%M:%S")
            rows = compute_bt_pnl(bars, prices, label, now_str)
            f.rows_written = len(rows)
            f.fix_applied = not dry_run
            if not dry_run and rows:
                _prepare_rows_for_insert(rows)
                client.insert(f"analytics.{tgt}", rows, column_names=INSERT_COLUMNS)
            log.info("[bt/%s] %s: %d rows", underlying, f.strategy_table_name, f.rows_written)


def refresh_hour_table(type_: Mode, earliest_ts: datetime, client, *, dry_run: bool) -> None:
    """Rebuild the 1-hour table from the 1-min table for ts >= earliest_ts.

    SQL pattern lifted verbatim from the existing backfill scripts (proven).
    """
    hour = HOUR_TABLE[type_]
    minute = TARGET_TABLE[type_]
    ts_str = earliest_ts.strftime("%Y-%m-%d %H:%M:%S")
    if dry_run:
        log.info("[hour-refresh:%s] DRY-RUN would rebuild from %s", type_, ts_str)
        return
    log.info("[hour-refresh:%s] rebuilding from %s", type_, ts_str)
    client.command(
        f"DELETE FROM analytics.{hour} "
        f"WHERE ts >= toStartOfHour(toDateTime('{ts_str}'))"
    )
    client.command(f"""
INSERT INTO analytics.{hour}
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version,
    toStartOfHour(minute_ts)           AS ts,
    argMax(cumulative_pnl, minute_ts)  AS cumulative_pnl,
    argMax(benchmark, minute_ts)       AS benchmark,
    argMax(position, minute_ts)        AS position,
    argMax(price, minute_ts)           AS price,
    argMax(final_signal, minute_ts)    AS final_signal,
    argMax(weighting, minute_ts)       AS weighting,
    now()                              AS updated_at,
    any(strategy_instance_id)          AS strategy_instance_id
FROM (
    SELECT *, ts AS minute_ts
    FROM analytics.{minute}
    WHERE ts >= toStartOfHour(toDateTime('{ts_str}'))
    ORDER BY strategy_table_name, strategy_id, strategy_name, underlying,
             config_timeframe, source, version, ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying,
               config_timeframe, source, version, ts
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfHour(minute_ts)
""")
    log.info("[hour-refresh:%s] done", type_)
```

- [ ] **Step 2: Run all unit tests (still 8)**

Run: `pytest tests/test_audit_pnl.py -v`
Expected: 8 PASS.

- [ ] **Step 3: Commit**

```bash
git add scripts/audit_pnl.py
git commit -m "feat(audit_pnl): add bt window-local fix and hour-table refresh"
```

---

## Task 6: State file (JSONL) + history rendering

**Files:**
- Modify: `scripts/audit_pnl.py` (append to section E)
- Modify: `tests/test_audit_pnl.py`

- [ ] **Step 1: Write tests for the state-file round-trip and the history table renderer**

Append to `tests/test_audit_pnl.py`:

```python
class TestStateFile:
    def test_append_and_read_round_trip(self, tmp_path):
        from scripts.audit_pnl import AuditReport, append_run_record, read_recent_runs

        report = AuditReport(
            started_at=_dt("2026-05-30 14:30:00"),
            finished_at=_dt("2026-05-30 14:34:12"),
            mode="audit",
            scope_types=["prod", "bt", "real_trade"],
            run_id="20260530_143000_abc1",
        )
        report.by_type["prod"] = TypeSummary(type="prod", strategies_checked=12, violations=2)
        report.violations.append(
            Violation(
                type="prod", underlying="BTC", strategy_table_name="my_strat_v3",
                category="mid_gap", detail="4320m missing",
                failure_ts=_dt("2026-05-25 06:00:00"),
            )
        )
        report.report_path = Path("./audit_reports/foo.md")

        state_file = tmp_path / "history.jsonl"
        append_run_record(report, state_file)
        recent = read_recent_runs(state_file, n=5)
        assert len(recent) == 1
        rec = recent[0]
        assert rec["run_id"] == "20260530_143000_abc1"
        assert rec["mode"] == "audit"
        assert rec["totals"]["violations"] == 1
        assert rec["by_type"]["prod"]["violations"] == 2
        assert rec["violations"][0]["category"] == "mid_gap"

    def test_read_recent_returns_last_n(self, tmp_path):
        from scripts.audit_pnl import AuditReport, append_run_record, read_recent_runs

        state_file = tmp_path / "history.jsonl"
        for i in range(7):
            r = AuditReport(
                started_at=_dt(f"2026-05-{20 + i:02d} 09:00:00"),
                finished_at=_dt(f"2026-05-{20 + i:02d} 09:01:00"),
                run_id=f"run-{i}",
            )
            append_run_record(r, state_file)
        recent = read_recent_runs(state_file, n=3)
        assert [r["run_id"] for r in recent] == ["run-4", "run-5", "run-6"]


class TestHistoryTable:
    def test_render_history_table_one_row(self):
        from scripts.audit_pnl import render_history_table
        record = {
            "run_id": "20260530_143000_abc1",
            "started_at": "2026-05-30T14:30:00Z",
            "mode": "audit",
            "totals": {"strategies_checked": 277, "violations": 2, "rows_fixed": 0},
            "scope": {"types": ["prod", "bt", "real_trade"]},
            "report_path": "./audit_reports/foo.md",
        }
        out = render_history_table([record])
        assert "20260530_143000_abc1" in out
        assert "audit" in out
        assert "277" in out
        assert "prod,bt,real_trade" in out
```

- [ ] **Step 2: Run the new tests to confirm they fail**

Run: `pytest tests/test_audit_pnl.py::TestStateFile tests/test_audit_pnl.py::TestHistoryTable -v`
Expected: 3 import-failure errors.

- [ ] **Step 3: Append section E (state + history) to `scripts/audit_pnl.py`**

```python
def _serialize_report(report: AuditReport) -> dict:
    """Convert AuditReport to a JSON-safe dict for one history record."""
    def _iso(dt: Optional[datetime]) -> Optional[str]:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None

    def _ts(dt: Optional[datetime]) -> Optional[str]:
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

    return {
        "run_id": report.run_id,
        "started_at": _iso(report.started_at),
        "finished_at": _iso(report.finished_at),
        "duration_secs": (
            round((report.finished_at - report.started_at).total_seconds(), 1)
            if report.finished_at and report.started_at else None
        ),
        "mode": report.mode,
        "scope": {
            "types": list(report.scope_types),
            "underlying": report.scope_underlying,
            "fix_window": (
                [_ts(report.scope_fix_window[0]), _ts(report.scope_fix_window[1])]
                if report.scope_fix_window else None
            ),
        },
        "totals": {
            "strategies_checked": sum(s.strategies_checked for s in report.by_type.values()),
            "violations": len(report.violations),
            "rows_fixed": sum(f.rows_written for f in report.fixes if f.fix_applied),
        },
        "by_type": {
            t: {"strategies": s.strategies_checked, "violations": s.violations, "rows_fixed": s.rows_fixed}
            for t, s in report.by_type.items()
        },
        "violations": [
            {
                "type": v.type,
                "underlying": v.underlying,
                "strategy_table_name": v.strategy_table_name,
                "categories": [v.category],
                "failure_ts": _ts(v.failure_ts),
                "detail": v.detail,
                "fix_applied": any(
                    f.fix_applied
                    and f.type == v.type
                    and f.underlying == v.underlying
                    and f.strategy_table_name == v.strategy_table_name
                    for f in report.fixes
                ),
            }
            for v in report.violations
        ],
        "report_path": str(report.report_path) if report.report_path else None,
    }


def append_run_record(report: AuditReport, state_file: Path) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("a") as f:
        f.write(json.dumps(_serialize_report(report), separators=(",", ":")) + "\n")


def read_recent_runs(state_file: Path, n: int = 5) -> list[dict]:
    if not state_file.exists():
        return []
    lines = state_file.read_text().splitlines()
    return [json.loads(line) for line in lines[-n:] if line.strip()]


def render_history_table(records: list[dict]) -> str:
    rows = [["RUN", "WHEN", "MODE", "STRATS", "VIOL", "FIXED", "TYPES", "REPORT"]]
    for r in records:
        types = ",".join(r.get("scope", {}).get("types") or [])
        rows.append([
            r.get("run_id", ""),
            (r.get("started_at") or "").replace("T", " ").replace("Z", " UTC"),
            r.get("mode", ""),
            str(r.get("totals", {}).get("strategies_checked", "")),
            str(r.get("totals", {}).get("violations", "")),
            str(r.get("totals", {}).get("rows_fixed", "")),
            types,
            r.get("report_path") or "",
        ])
    widths = [max(len(str(row[c])) for row in rows) for c in range(len(rows[0]))]
    lines = []
    for row in rows:
        lines.append("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    return "\n".join(lines)
```

- [ ] **Step 4: Run the state-file tests**

Run: `pytest tests/test_audit_pnl.py::TestStateFile tests/test_audit_pnl.py::TestHistoryTable -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/audit_pnl.py tests/test_audit_pnl.py
git commit -m "feat(audit_pnl): persist run history to JSONL with --show-history support"
```

---

## Task 7: Report rendering (console + Markdown)

**Files:**
- Modify: `scripts/audit_pnl.py` (append to section F)
- Modify: `tests/test_audit_pnl.py`

- [ ] **Step 1: Write tests for both renderers**

Append to `tests/test_audit_pnl.py`:

```python
class TestRenderReport:
    def _sample_report(self) -> "AuditReport":
        report = AuditReport(
            started_at=_dt("2026-05-30 14:30:00"),
            finished_at=_dt("2026-05-30 14:34:12"),
            mode="audit",
            scope_types=["prod", "bt", "real_trade"],
            run_id="20260530_143000_abc1",
        )
        report.by_type = {
            "prod": TypeSummary(type="prod", strategies_checked=12, violations=2),
            "bt": TypeSummary(type="bt", strategies_checked=220, violations=0),
            "real_trade": TypeSummary(type="real_trade", strategies_checked=45, violations=0),
        }
        report.violations.append(
            Violation(
                type="prod", underlying="BTC", strategy_table_name="my_strat_v3",
                category="mid_gap", detail="4320m missing from 2026-05-25 06:00",
                failure_ts=_dt("2026-05-25 06:00:00"),
            )
        )
        return report

    def test_console_lists_each_type_with_counts(self):
        from scripts.audit_pnl import render_console
        out = render_console(self._sample_report())
        assert "prod" in out and "12" in out and "2 violations" in out
        assert "bt" in out and "220" in out
        assert "real_trade" in out and "45" in out
        assert "my_strat_v3" in out

    def test_console_clean_run_says_so(self):
        from scripts.audit_pnl import render_console
        r = AuditReport(started_at=_dt("2026-05-30 14:30:00"),
                        finished_at=_dt("2026-05-30 14:30:05"),
                        scope_types=["prod"], run_id="x")
        r.by_type["prod"] = TypeSummary(type="prod", strategies_checked=10, violations=0)
        out = render_console(r)
        assert "clean" in out.lower() or "0 violations" in out

    def test_markdown_writes_file(self, tmp_path):
        from scripts.audit_pnl import write_markdown
        path = tmp_path / "report.md"
        write_markdown(self._sample_report(), path)
        assert path.exists()
        body = path.read_text()
        assert "# PnL Audit Report" in body
        assert "## prod" in body
        assert "my_strat_v3" in body
        assert "mid_gap" in body
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `pytest tests/test_audit_pnl.py::TestRenderReport -v`
Expected: 3 ImportError failures.

- [ ] **Step 3: Append section F to `scripts/audit_pnl.py`**

```python
def render_console(report: AuditReport) -> str:
    lines: list[str] = []
    started = report.started_at.strftime("%Y-%m-%d %H:%M:%S UTC")
    lines.append(f"PnL AUDIT — {started}")
    lines.append("")
    total_viol = len(report.violations)
    for t in TYPES:
        if t not in report.by_type:
            continue
        s = report.by_type[t]
        suffix = "clean" if s.violations == 0 else f"{s.violations} violations"
        lines.append(
            f"[{t}] {s.strategies_checked} strategies / "
            f"{len({v.underlying for v in report.violations if v.type == t})} underlyings — {suffix}"
        )
        if s.violations:
            for v in report.violations:
                if v.type != t:
                    continue
                lines.append(f"  {v.category}: {v.underlying} {v.strategy_table_name} — {v.detail}")
    lines.append("")
    if report.fixes:
        applied = [f for f in report.fixes if f.fix_applied]
        rows_total = sum(f.rows_written for f in applied)
        lines.append(f"→ Applied {len(applied)} fixes, {rows_total} rows written")
    if report.report_path:
        lines.append(f"→ Wrote {report.report_path}")
    exit_msg = "0 (clean)" if total_viol == 0 else f"1 ({total_viol} violations)"
    lines.append(f"→ Exit {exit_msg}")
    return "\n".join(lines)


def write_markdown(report: AuditReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# PnL Audit Report")
    lines.append("")
    lines.append(f"- **Run ID:** `{report.run_id}`")
    lines.append(f"- **Started:** {report.started_at:%Y-%m-%d %H:%M:%S UTC}")
    if report.finished_at:
        lines.append(f"- **Finished:** {report.finished_at:%Y-%m-%d %H:%M:%S UTC}")
    lines.append(f"- **Mode:** {report.mode}")
    lines.append(f"- **Types:** {', '.join(report.scope_types)}")
    if report.scope_underlying:
        lines.append(f"- **Underlying filter:** {report.scope_underlying}")
    if report.scope_fix_window:
        ws, we = report.scope_fix_window
        lines.append(f"- **Fix window:** [{ws:%Y-%m-%d %H:%M:%S}, {we:%Y-%m-%d %H:%M:%S}]")
    lines.append("")
    for t in TYPES:
        if t not in report.by_type:
            continue
        s = report.by_type[t]
        lines.append(f"## {t}")
        lines.append("")
        lines.append(f"- Strategies checked: {s.strategies_checked}")
        lines.append(f"- Violations: {s.violations}")
        lines.append(f"- Rows fixed: {s.rows_fixed}")
        type_viols = [v for v in report.violations if v.type == t]
        if type_viols:
            lines.append("")
            lines.append("| Underlying | Strategy | Category | failure_ts | Detail | Fix |")
            lines.append("|---|---|---|---|---|---|")
            for v in type_viols:
                applied = any(
                    f.fix_applied and f.type == v.type and f.underlying == v.underlying
                    and f.strategy_table_name == v.strategy_table_name
                    for f in report.fixes
                )
                fix_str = "applied" if applied else "—"
                ts_str = v.failure_ts.strftime("%Y-%m-%d %H:%M:%S") if v.failure_ts else ""
                lines.append(
                    f"| {v.underlying} | `{v.strategy_table_name}` | {v.category} | {ts_str} | {v.detail} | {fix_str} |"
                )
        lines.append("")
    path.write_text("\n".join(lines))
```

- [ ] **Step 4: Run all unit tests**

Run: `pytest tests/test_audit_pnl.py -v`
Expected: 14 PASS (8 from earlier + 3 state + 3 render).

- [ ] **Step 5: Commit**

```bash
git add scripts/audit_pnl.py tests/test_audit_pnl.py
git commit -m "feat(audit_pnl): render console + markdown reports"
```

---

## Task 8: CLI + `main()` orchestration

**Files:**
- Modify: `scripts/audit_pnl.py` (replace the placeholder `if __name__` block and append section G)

- [ ] **Step 1: Replace the bottom placeholder with the real CLI + main**

Replace:
```python
if __name__ == "__main__":
    raise SystemExit("audit_pnl.py main() not implemented yet — see Task 8")
```

with:

```python
def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Audit and (optionally) repair PnL tables.")
    p.add_argument("--type", choices=["prod", "bt", "real_trade", "all"], default="all")
    p.add_argument("--underlying", help="Filter to one underlying (default: all)")
    p.add_argument("--fix", action="store_true",
                   help="Apply fixes for detected issues (prod/rt forward, bt window)")
    p.add_argument("--fix-window", nargs=2, metavar=("START", "END"),
                   help="Explicit-window repair; --type must be specified")
    p.add_argument("--report", help="Markdown report path (default: ./audit_reports/<UTC-ts>.md)")
    p.add_argument("--state-file", default=str(DEFAULT_REPORT_DIR / "history.jsonl"),
                   help="JSONL run-history file")
    p.add_argument("--show-history", nargs="?", type=int, const=5, default=None,
                   metavar="N", help="Print the last N runs and exit (default 5)")
    p.add_argument("--no-pause", action="store_true",
                   help="Skip ECS consumer pause/resume during --fix")
    p.add_argument("--dry-run", action="store_true",
                   help="Detect and compute fixes but do not write")
    return p


def _resolve_types(arg: str) -> list[Mode]:
    return list(TYPES) if arg == "all" else [arg]  # type: ignore[list-item]


def _run_audit(
    types: list[Mode], underlying: Optional[str], client, now_ts: datetime
) -> AuditReport:
    """Run all six checks for each requested type. Returns a report with violations
    populated; fixes empty until a separate fix phase runs."""
    report = AuditReport(
        started_at=_utcnow_naive(),
        scope_types=list(types),
        scope_underlying=underlying,
        run_id=_utcnow_naive().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:4],
    )
    for t in types:
        summary = TypeSummary(type=t)
        # Count strategies for this type (denominator for the report).
        count_sql = f"""
        SELECT count(DISTINCT strategy_table_name) FROM analytics.{TARGET_TABLE[t]}
        WHERE ts >= toDateTime('{GLOBAL_START_TS}')
        """ + (f" AND underlying = '{underlying}'" if underlying else "")
        summary.strategies_checked = int(client.query(count_sql).result_rows[0][0])
        all_viols: list[Violation] = []
        all_viols.extend(find_missing_or_start_gap(t, client, underlying))
        all_viols.extend(find_midgap(t, client, underlying))
        all_viols.extend(find_stale_end(t, client, now_ts, underlying))
        all_viols.extend(audit_positions(t, client, underlying))
        all_viols.extend(audit_hour_sync(t, client, underlying))
        summary.violations = len(all_viols)
        report.by_type[t] = summary
        report.violations.extend(all_viols)
        log.info("[audit:%s] %d strategies, %d violations",
                 t, summary.strategies_checked, summary.violations)
    return report


def _run_fixes(report: AuditReport, client, ecs, *, dry_run: bool, no_pause: bool, now_ts: datetime) -> None:
    """Resolve violations into per-strategy fixes; pause consumer per-type;
    delegate to fix_prod_rt_strategies or fix_bt_strategies."""
    grouped = group_violations_by_strategy(report.violations)
    fixes_by_type: dict[Mode, list[StrategyFix]] = {t: [] for t in TYPES}
    for _key, viols in grouped.items():
        fix = resolve_strategy_fix(viols)
        fixes_by_type[fix.type].append(fix)
    report.fixes.extend(f for fs in fixes_by_type.values() for f in fs)

    # prod + real_trade: forward repair under consumer pause.
    for t in ("prod", "real_trade"):
        fixes = fixes_by_type[t]
        if not fixes:
            continue
        try:
            if ecs and not no_pause and not dry_run:
                pause_consumer(ecs, t)
            fix_prod_rt_strategies(t, fixes, client, dry_run=dry_run, now_ts=now_ts)
            earliest = min(f.failure_ts for f in fixes)
            refresh_hour_table(t, earliest, client, dry_run=dry_run)
        finally:
            if ecs and not no_pause and not dry_run:
                resume_consumer(ecs, t)

    # bt: window-local, no consumer pause.
    bt_fixes = fixes_by_type["bt"]
    if bt_fixes:
        fix_bt_strategies(bt_fixes, client, dry_run=dry_run, now_ts=now_ts)
        earliest = min(f.failure_ts for f in bt_fixes)
        refresh_hour_table("bt", earliest, client, dry_run=dry_run)

    # Hour-sync-only failures: refresh hour table even when no minute writes happened.
    for t in TYPES:
        if any(f.type == t for f in report.fixes):
            continue  # already refreshed
        hour_only = [v for v in report.violations if v.type == t and v.category == "hour_sync"]
        if hour_only:
            earliest = min(v.failure_ts for v in hour_only if v.failure_ts)
            refresh_hour_table(t, earliest, client, dry_run=dry_run)

    # Aggregate rows_fixed back into TypeSummary.
    for f in report.fixes:
        if f.fix_applied:
            report.by_type[f.type].rows_fixed += f.rows_written


def main() -> int:
    args = _build_arg_parser().parse_args()

    state_path = Path(args.state_file)

    # Subcommand-style: --show-history short-circuits everything else.
    if args.show_history is not None:
        recent = read_recent_runs(state_path, n=args.show_history)
        if not recent:
            print(f"No history at {state_path}")
            return 0
        print(render_history_table(recent))
        return 0

    if args.fix and args.fix_window:
        log.error("--fix and --fix-window are mutually exclusive")
        return 2
    if args.fix_window and args.type == "all":
        log.error("--fix-window requires --type {prod|bt|real_trade}")
        return 2

    types = _resolve_types(args.type)
    client = get_client()
    now_ts = _utcnow_naive()

    # Show delta against the last run if state file exists.
    prior = read_recent_runs(state_path, n=1)
    if prior:
        p = prior[0]
        log.info("Last run: %s — %d violations, %d rows fixed",
                 p.get("started_at"), p.get("totals", {}).get("violations", 0),
                 p.get("totals", {}).get("rows_fixed", 0))

    report = _run_audit(types, args.underlying, client, now_ts)
    report.mode = "fix" if args.fix else ("fix-window" if args.fix_window else "audit")
    if args.dry_run:
        report.mode = "dry-run"

    # --fix-window path: synthesize per-strategy fixes for the window without
    # running detection-derived ones, then call fix_prod_rt or fix_bt as appropriate.
    if args.fix_window:
        ws, we = _parse_ts(args.fix_window[0]), _parse_ts(args.fix_window[1])
        report.scope_fix_window = (ws, we)
        t = types[0]
        # Find strategies with >10% under-coverage in the window (same heuristic
        # the old backfill_rt_window_gap.py used, generalized to any type).
        tgt = TARGET_TABLE[t]
        gap_sql = f"""
        WITH expected AS (
            SELECT dateDiff('minute',
                toDateTime('{ws:%Y-%m-%d %H:%M:%S}'),
                toDateTime('{we:%Y-%m-%d %H:%M:%S}')) AS minutes
        )
        SELECT t.strategy_table_name, any(t.underlying)
        FROM analytics.{tgt} t CROSS JOIN expected
        WHERE t.ts >= toDateTime('{ws:%Y-%m-%d %H:%M:%S}')
          AND t.ts <  toDateTime('{we:%Y-%m-%d %H:%M:%S}')
        GROUP BY t.strategy_table_name
        HAVING count() < expected.minutes * 0.9
        """
        synthetic: list[StrategyFix] = []
        for stn, und in client.query(gap_sql).result_rows:
            synthetic.append(
                StrategyFix(
                    type=t, underlying=str(und), strategy_table_name=str(stn),
                    failure_ts=ws, window_end=we, categories=["mid_gap"],
                )
            )
        log.info("[--fix-window] %d strategies under 90%% coverage in window", len(synthetic))
        ecs = get_ecs_client() if not args.no_pause and not args.dry_run else None
        try:
            if t in ("prod", "real_trade"):
                if ecs:
                    pause_consumer(ecs, t)
                fix_prod_rt_strategies(t, synthetic, client, dry_run=args.dry_run, now_ts=now_ts)
            else:
                fix_bt_strategies(synthetic, client, dry_run=args.dry_run, now_ts=now_ts)
            refresh_hour_table(t, ws, client, dry_run=args.dry_run)
        finally:
            if ecs:
                resume_consumer(ecs, t)
        report.fixes.extend(synthetic)
    elif args.fix or args.dry_run:
        ecs = get_ecs_client() if not args.no_pause and not args.dry_run else None
        _run_fixes(report, client, ecs, dry_run=args.dry_run, no_pause=args.no_pause, now_ts=now_ts)

    report.finished_at = _utcnow_naive()
    report_path = Path(args.report) if args.report else DEFAULT_REPORT_DIR / f"{report.run_id}.md"
    write_markdown(report, report_path)
    report.report_path = report_path

    append_run_record(report, state_path)

    print(render_console(report))
    return 0 if not report.has_violations() else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Verify `--help` works**

Run: `PYTHONPATH=. python scripts/audit_pnl.py --help`
Expected: usage text listing all flags, exit 0.

- [ ] **Step 3: Verify `--show-history` works on an empty state**

Run: `PYTHONPATH=. python scripts/audit_pnl.py --show-history`
Expected: `No history at ./audit_reports/history.jsonl`, exit 0.

- [ ] **Step 4: Run full unit test suite**

Run: `pytest tests/test_audit_pnl.py -v`
Expected: 14 PASS, 0 fail.

- [ ] **Step 5: Live smoke test against ClickHouse in dry-run audit mode**

Requires `.env` with CH credentials. Run:
`PYTHONPATH=. python scripts/audit_pnl.py --type prod --dry-run`

Expected output (shape, not exact numbers):
- Logs from each check function (~5-30s)
- Console report ending with "Exit 0 (clean)" or "Exit 1 (N violations)"
- A Markdown file in `./audit_reports/<run_id>.md`
- One line appended to `./audit_reports/history.jsonl`

If the script errors due to a missing detection-SQL column or table name, fix it inline (this is the moment when integration discrepancies surface) and commit before continuing.

- [ ] **Step 6: Commit**

```bash
git add scripts/audit_pnl.py
git commit -m "feat(audit_pnl): wire CLI, --fix-window, --show-history, and main()"
```

---

## Task 9: Delete superseded backfill scripts

**Files:**
- Delete: `scripts/backfill_fet_prices_and_prod.py`
- Delete: `scripts/backfill_prod_missing_strategies.py`
- Delete: `scripts/backfill_rt_missing_strategies.py`
- Delete: `scripts/backfill_rt_midgap_strategies.py`
- Delete: `scripts/backfill_rt_window_gap.py`

- [ ] **Step 1: Confirm no other code imports from them**

Run: `grep -rn "from scripts.backfill\|import backfill_" --include="*.py" . | grep -v "^\\./.venv/"`
Expected: no matches. (These are CLI entry points, not modules.)

- [ ] **Step 2: Delete the five files**

```bash
git rm scripts/backfill_fet_prices_and_prod.py \
       scripts/backfill_prod_missing_strategies.py \
       scripts/backfill_rt_missing_strategies.py \
       scripts/backfill_rt_midgap_strategies.py \
       scripts/backfill_rt_window_gap.py
```

- [ ] **Step 3: Confirm tree shrunk to expected files**

Run: `ls scripts/`
Expected: `audit_pnl.py`, `local_pnl_test.py`, `patch_dashboards_resolution.py`.

- [ ] **Step 4: Commit**

```bash
git commit -m "chore(scripts): remove five backfill scripts superseded by audit_pnl.py"
```

---

## Task 10: Quality gates

**Files:** none

- [ ] **Step 1: Format and lint**

Run: `black scripts/audit_pnl.py tests/test_audit_pnl.py && ruff check --fix scripts/audit_pnl.py tests/test_audit_pnl.py`
Expected: no errors after auto-fix. If anything remains, address inline.

- [ ] **Step 2: Type-check (advisory; tolerate existing repo errors)**

Run: `mypy scripts/audit_pnl.py`
Expected: errors only related to dynamic ClickHouse return shapes are acceptable; fix anything in the new code that's a real bug.

- [ ] **Step 3: Re-run the unit suite**

Run: `pytest tests/test_audit_pnl.py -v`
Expected: 14 PASS.

- [ ] **Step 4: Commit any formatting tweaks**

```bash
git add -A
git diff --cached --quiet || git commit -m "chore(audit_pnl): apply black + ruff"
```

---

## Notes for the implementing engineer

- **All ClickHouse credentials come from `.env`.** The script imports `dotenv` at top — don't reach for environment variables before that.
- **AWS profile** defaults to `AdministratorAccess-068704208855`; override via `AWS_PROFILE` env var.
- **The Dagster `pnl_coverage_audit` asset stays untouched** — it's the daily scheduled check. Don't try to share code with it; the script intentionally duplicates the detection SQL because the asset uses `services/dagster/trading_dagster/utils/clickhouse_client` while the script uses raw `clickhouse_connect`.
- **DELETE behavior:** prod and real_trade use `DELETE FROM` (lightweight). The deleted `backfill_rt_window_gap.py` had used `ALTER TABLE ... DELETE` because Dagster lacked the lightweight grant — that's no longer relevant for a script run with an admin user, so prefer `DELETE FROM`. If you hit a permissions error in step 5 of Task 8, switch to `ALTER TABLE ... DELETE ... SETTINGS mutations_sync = 2` and re-test.
- **`benchmark` is out of scope for v1** — do not add a fourth type.
