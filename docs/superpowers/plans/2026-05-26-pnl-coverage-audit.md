# PnL Coverage & Position Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a daily Dagster asset that walks every `(underlying, strategy)` from its true start to "now" and fails the Dagster run on any missing minute or position drift in the six PnL tables.

**Architecture:** Single asset `pnl_coverage_audit` in `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`. Three validation phases (coverage stats, gap drilldown, position-boundary diff) run entirely in Python from compact projections fetched per `(target_table, underlying)` for aggregations and per `(target_table, underlying, strategy)` for window functions. Pure read-only — never writes PnL data.

**Tech Stack:** Python 3.11, Dagster, ClickHouse Cloud via `clickhouse-connect`, pytest. Reuses `libs/computation` helpers (`build_rt_lookup`, `TIMEFRAME_MAP`).

**Spec:** [2026-05-26-pnl-coverage-audit-design.md](../specs/2026-05-26-pnl-coverage-audit-design.md)

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` | NEW | Asset + driver + fetch functions + phase 1/2/3 checks + report formatter |
| `services/dagster/tests/test_pnl_coverage_audit.py` | NEW | Unit tests for pure-Python checks; mocked-CH integration tests for drivers |
| `services/dagster/trading_dagster/definitions/__init__.py` | MODIFY | Register asset, job, and daily schedule |

The asset file groups related concerns (fetch, check, report) for one feature in one place — matches existing pattern in `pnl_strategy_v2.py`.

---

## Task 1: Module skeleton, dataclasses, constants

**Files:**
- Create: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`

- [ ] **Step 1.1: Create the file with imports, constants, and dataclasses**

Write to `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`:

```python
"""PnL Coverage & Position Audit — Dagster Asset.

Daily read-only check that walks every (underlying, strategy) in the six PnL
tables (1-min + 1-hour, for prod / bt / real_trade) and fails the Dagster run
on any missing minute or position drift.

See docs/superpowers/specs/2026-05-26-pnl-coverage-audit-design.md.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from libs.computation import TIMEFRAME_MAP, build_rt_lookup

from ..utils.clickhouse_client import get_client, query_rows

_log = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────────────────

# Global earliest ts we ever consider — matches PROD_REAL_TRADE_START_DATE
# in pnl_strategy_v2.py.
GLOBAL_START_TS = "2026-02-27 00:00:00"

# Phase 1 tolerances (minutes).
START_TOLERANCE_MIN = 5
STALE_END_TOLERANCE_MIN = 10  # actual_max_ts < now() - 10m → stale

# Phase 3 tolerances.
POS_TS_TOLERANCE_MIN = 1

# Top-N offenders included in the failure summary.
TOP_N_OFFENDERS = 10

# ClickHouse per-query memory cap (500 MB).
QUERY_MEMORY_CAP = 524_288_000

# Parallel workers — matches existing pnl_strategy_v2 pattern.
_MAX_WORKERS = 4

# Tables in scope.
Mode = Literal["prod", "bt", "real_trade"]

_TARGET_TABLES: list[tuple[str, str, Mode]] = [
    ("strategy_pnl_1min_prod_v2", "strategy_output_history_v2", "prod"),
    ("strategy_pnl_1min_bt_v2", "strategy_output_history_bt_v2", "bt"),
    ("strategy_pnl_1min_real_trade_v2", "strategy_output_history_v2", "real_trade"),
]

_HOUR_TABLES: list[tuple[str, str, Mode]] = [
    ("strategy_pnl_1hour_prod_v2", "strategy_pnl_1min_prod_v2", "prod"),
    ("strategy_pnl_1hour_bt_v2", "strategy_pnl_1min_bt_v2", "bt"),
    ("strategy_pnl_1hour_real_trade_v2", "strategy_pnl_1min_real_trade_v2", "real_trade"),
]


# ── Data classes ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StratStat:
    """Target-table per-strategy stats from Q_stat."""
    actual_min_ts: datetime
    actual_max_ts: datetime
    actual_rows: int


@dataclass(frozen=True)
class SourceFirstBar:
    """Source per-strategy first-bar info derived from Q_src / Q_src_rt."""
    expected_min_ts: datetime
    tf_minutes: int


@dataclass(frozen=True)
class GapDescriptor:
    """One internal gap detected by Q_gap, after price-gap exemption."""
    stn: str
    gap_end: datetime
    gap_minutes: int


@dataclass(frozen=True)
class PositionChange:
    """One row in either source_changes or target_changes."""
    effective_ts: datetime
    position: float


@dataclass
class Violation:
    """One detected problem for a (table, underlying, strategy)."""
    table: str
    underlying: str
    stn: str
    category: Literal["start_gap", "stale_end", "internal_holes", "position_mismatch"]
    detail: str
    # For ranking the worst offenders in the summary.
    severity_minutes: int = 0


@dataclass
class AuditReport:
    """All violations collected across one audit run."""
    violations: list[Violation] = field(default_factory=list)
    tables_checked: int = 0
    strategies_checked: int = 0
    duration_secs: float = 0.0

    def has_failures(self) -> bool:
        return bool(self.violations)


# ── Implementation goes in subsequent tasks ─────────────────────────────────


def pnl_coverage_audit_asset():
    """Placeholder — replaced in Task 11."""
    raise NotImplementedError
```

- [ ] **Step 1.2: Verify the file imports cleanly**

Run: `python3 -c "from services.dagster.trading_dagster.assets import pnl_coverage_audit; print(pnl_coverage_audit.__doc__)"`

Expected: prints the docstring, no ImportError.

- [ ] **Step 1.3: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py
git commit -m "feat(audit): scaffold pnl_coverage_audit module"
```

---

## Task 2: Phase 1 coverage check (pure function)

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Create: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 2.1: Write failing tests**

Create `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
"""Unit tests for pnl_coverage_audit pure-Python check functions."""

from datetime import datetime

import pytest

from services.dagster.trading_dagster.assets.pnl_coverage_audit import (
    GapDescriptor,
    PositionChange,
    SourceFirstBar,
    StratStat,
    Violation,
    _check_phase1,
    _check_phase2,
    _check_phase3,
    _check_phase3_hour,
    _compute_source_changes_prod_bt,
    _compute_source_changes_rt,
    _format_report,
)


def _dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _price_set(*ts_strings: str) -> set[datetime]:
    return {_dt(s) for s in ts_strings}


# ── Phase 1: coverage ────────────────────────────────────────────────────────

class TestPhase1:
    def test_clean_match_returns_no_violation(self):
        """Actual range matches expected, no missing minutes — no violation."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-05-26 09:55:00"),
            actual_rows=3,
        )
        source = SourceFirstBar(expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5)
        prices = _price_set("2026-03-05 09:05:00", "2026-03-05 09:06:00", "2026-05-26 09:55:00")
        now = _dt("2026-05-26 10:00:00")
        v = _check_phase1("t", "FET", "S", stat, source, prices, now)
        assert v is None

    def test_detects_start_gap(self):
        """actual_min_ts >> expected_min_ts → start_gap."""
        stat = StratStat(
            actual_min_ts=_dt("2026-05-02 20:44:00"),
            actual_max_ts=_dt("2026-05-26 09:58:00"),
            actual_rows=1000,
        )
        source = SourceFirstBar(expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5)
        prices = {_dt("2026-05-26 09:58:00")}
        now = _dt("2026-05-26 10:00:00")
        v = _check_phase1("t", "FET", "S", stat, source, prices, now)
        assert v is not None
        assert v.category == "start_gap"
        assert "2026-03-05" in v.detail
        assert "2026-05-02" in v.detail

    def test_detects_stale_end(self):
        """actual_max_ts < now - 10m → stale_end."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-05-26 09:30:00"),
            actual_rows=1000,
        )
        source = SourceFirstBar(expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5)
        prices = {_dt("2026-03-05 09:05:00")}
        now = _dt("2026-05-26 10:00:00")  # 30 min ahead of actual_max
        v = _check_phase1("t", "FET", "S", stat, source, prices, now)
        assert v is not None
        assert v.category == "stale_end"

    def test_detects_internal_holes(self):
        """actual_rows < |price_set ∩ [expected_min, now-5m]| → internal_holes."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-03-05 09:55:00"),
            actual_rows=2,
        )
        source = SourceFirstBar(expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5)
        prices = _price_set(
            "2026-03-05 09:05:00",
            "2026-03-05 09:10:00",
            "2026-03-05 09:15:00",
            "2026-03-05 09:55:00",
        )
        now = _dt("2026-03-05 10:00:00")
        v = _check_phase1("t", "FET", "S", stat, source, prices, now)
        assert v is not None
        assert v.category == "internal_holes"
        # 4 expected minutes vs 2 actual → 2 missing.
        assert "2" in v.detail

    def test_price_gap_exemption(self):
        """Minutes missing in price_set are not counted as missing rows."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-03-05 09:15:00"),
            actual_rows=2,
        )
        source = SourceFirstBar(expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5)
        # Only 2 price minutes exist in the window.
        prices = _price_set("2026-03-05 09:05:00", "2026-03-05 09:15:00")
        now = _dt("2026-03-05 09:30:00")  # 15m past max; well past actual_max
        v = _check_phase1("t", "FET", "S", stat, source, prices, now)
        # Actual_rows == expected (2), but stale_end fires because actual_max < now-10m.
        assert v is not None
        assert v.category == "stale_end"
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase1 -v`

Expected: All tests fail with `ImportError` for `_check_phase1` etc.

- [ ] **Step 2.3: Implement `_check_phase1` in `pnl_coverage_audit.py`**

Append at the bottom of `services/dagster/trading_dagster/assets/pnl_coverage_audit.py` (just above the `pnl_coverage_audit_asset` placeholder):

```python
def _check_phase1(
    table: str,
    underlying: str,
    stn: str,
    stat: StratStat,
    source: SourceFirstBar,
    price_set: set[datetime],
    now_ts: datetime,
) -> Violation | None:
    """Detect start_gap / stale_end / internal_holes for one (U, S).

    Checks in priority order; returns the first detected violation, or None.
    """
    expected_min = source.expected_min_ts
    expected_max = now_ts - timedelta(minutes=STALE_END_TOLERANCE_MIN // 2)  # now - 5m

    # 1. start_gap
    if stat.actual_min_ts > expected_min + timedelta(minutes=START_TOLERANCE_MIN):
        gap_minutes = int((stat.actual_min_ts - expected_min).total_seconds() // 60)
        return Violation(
            table=table,
            underlying=underlying,
            stn=stn,
            category="start_gap",
            detail=f"expected={expected_min:%Y-%m-%d %H:%M:%S} actual={stat.actual_min_ts:%Y-%m-%d %H:%M:%S} ({gap_minutes}m)",
            severity_minutes=gap_minutes,
        )

    # 2. stale_end
    stale_threshold = now_ts - timedelta(minutes=STALE_END_TOLERANCE_MIN)
    if stat.actual_max_ts < stale_threshold:
        stale_minutes = int((now_ts - stat.actual_max_ts).total_seconds() // 60)
        return Violation(
            table=table,
            underlying=underlying,
            stn=stn,
            category="stale_end",
            detail=f"expected≈{expected_max:%Y-%m-%d %H:%M:%S} actual={stat.actual_max_ts:%Y-%m-%d %H:%M:%S} ({stale_minutes}m stale)",
            severity_minutes=stale_minutes,
        )

    # 3. internal_holes
    expected_minutes = sum(
        1 for p in price_set if expected_min <= p <= expected_max
    )
    if stat.actual_rows < expected_minutes:
        missing = expected_minutes - stat.actual_rows
        return Violation(
            table=table,
            underlying=underlying,
            stn=stn,
            category="internal_holes",
            detail=f"expected_minutes={expected_minutes} actual_rows={stat.actual_rows} missing={missing}",
            severity_minutes=missing,
        )

    return None
```

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase1 -v`

Expected: All 5 tests PASS.

- [ ] **Step 2.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): phase 1 coverage check with tests"
```

---

## Task 3: Phase 2 gap drilldown

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 3.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Phase 2: gap drilldown ───────────────────────────────────────────────────


class TestPhase2:
    def test_collects_top_gaps_by_duration(self):
        """Returns the top N gaps sorted by gap_minutes desc, descriptor-typed."""
        # Q_gap returns list of (gap_end, gap_secs). Phase 2 trims by price-gap
        # exemption and sorts.
        q_gap_rows = [
            (_dt("2026-03-05 10:00:00"), 600),   # 10 min gap, no price gaps
            (_dt("2026-03-06 11:00:00"), 7200),  # 2-hour gap, no price gaps
            (_dt("2026-03-07 12:01:00"), 120),   # 2 min gap, no price gaps
        ]
        price_set = _price_set()  # empty — no exemption
        descriptors = _check_phase2("FET", "S", q_gap_rows, price_set, top_n=2)
        assert len(descriptors) == 2
        # Sorted by gap_minutes desc.
        assert descriptors[0].gap_minutes == 120
        assert descriptors[0].gap_end == _dt("2026-03-06 11:00:00")
        assert descriptors[1].gap_minutes == 10

    def test_price_gap_exemption_reduces_gap_minutes(self):
        """Missing price minutes inside a gap are subtracted from gap_minutes."""
        # Raw gap is 10 minutes; 3 of those minutes have no price → reported gap = 7.
        q_gap_rows = [(_dt("2026-03-05 10:00:00"), 600)]
        prices_in_gap = _price_set(
            "2026-03-05 09:50:00",  # before gap
            "2026-03-05 09:53:00",  # inside gap [09:50, 10:00) means missing minutes are 09:51, 09:52, ...
            # Missing minutes: 09:51, 09:52, 09:54, 09:55, 09:56, 09:57, 09:58, 09:59 → 8 missing minutes
            # Wait — we said prices_present = {09:50, 09:53}, the gap covers (09:50, 10:00),
            # so missing prices are 09:51, 09:52, 09:54 .. 09:59 = 8 missing minutes.
            # gap_minutes after exemption = 10 − 8 = 2.
        )
        descriptors = _check_phase2("FET", "S", q_gap_rows, prices_in_gap, top_n=5)
        assert len(descriptors) == 1
        assert descriptors[0].gap_minutes == 2

    def test_gap_fully_explained_by_prices_returns_empty(self):
        """If every missing minute has no price, the gap is fully exempt → drop it."""
        q_gap_rows = [(_dt("2026-03-05 10:00:00"), 600)]
        # Anchor present at 09:50 and 10:00; no other price minutes in between.
        prices = _price_set("2026-03-05 09:50:00", "2026-03-05 10:00:00")
        descriptors = _check_phase2("FET", "S", q_gap_rows, prices, top_n=5)
        assert descriptors == []
```

- [ ] **Step 3.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase2 -v`

Expected: Fail with `ImportError` for `_check_phase2`.

- [ ] **Step 3.3: Implement `_check_phase2`**

Append to `pnl_coverage_audit.py`:

```python
def _check_phase2(
    underlying: str,
    stn: str,
    q_gap_rows: list[tuple[datetime, int]],
    price_set: set[datetime],
    top_n: int = TOP_N_OFFENDERS,
) -> list[GapDescriptor]:
    """For one flagged strategy, derive gap descriptors trimmed by price-gap exemption.

    Each q_gap row is (gap_end_ts, gap_secs) where gap_secs > 60. The actual
    gap interval is [gap_end - gap_secs, gap_end) on minute boundaries. We
    count missing minutes inside the interval and subtract minutes that have
    no price (since the recompute legitimately skips those). If the resulting
    gap_minutes drops to 0, the gap is fully exempt and dropped.
    """
    descriptors: list[GapDescriptor] = []
    for gap_end, gap_secs in q_gap_rows:
        gap_start_exclusive = gap_end - timedelta(seconds=gap_secs)
        # Missing minutes are the minute boundaries strictly between
        # gap_start_exclusive and gap_end. e.g. gap from 09:50 to 10:00 means
        # missing minutes 09:51..09:59.
        raw_missing = (gap_secs // 60) - 1
        if raw_missing <= 0:
            continue
        # Count price-present minutes inside the gap; missing-price minutes
        # don't count as PnL-table holes.
        present_inside = sum(
            1
            for p in price_set
            if gap_start_exclusive < p < gap_end
        )
        # raw_missing = total expected minutes; present_inside = minutes with price
        # that we expected a row for. So PnL holes = present_inside.
        effective_holes = present_inside
        if effective_holes <= 0:
            continue
        descriptors.append(
            GapDescriptor(stn=stn, gap_end=gap_end, gap_minutes=effective_holes)
        )

    descriptors.sort(key=lambda d: d.gap_minutes, reverse=True)
    return descriptors[:top_n]
```

- [ ] **Step 3.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase2 -v`

Expected: All 3 tests PASS.

- [ ] **Step 3.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): phase 2 gap drilldown with tests"
```

---

## Task 4: Source-change extractor for prod/bt

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 4.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Source changes: prod / bt ────────────────────────────────────────────────


class TestSourceChangesProdBt:
    def test_emits_at_first_bar_unconditionally(self):
        """Every strategy emits at least one change at the first bar's closing_ts."""
        # bars: list of (ts_str, position) for one strategy, tf=5min
        bars = [("2026-03-05 09:00:00", 1.0)]
        changes = _compute_source_changes_prod_bt(bars, tf_minutes=5)
        assert len(changes) == 1
        assert changes[0] == PositionChange(_dt("2026-03-05 09:05:00"), 1.0)

    def test_dedups_consecutive_equal_positions(self):
        """If position doesn't change, no new change is emitted."""
        bars = [
            ("2026-03-05 09:00:00", 1.0),
            ("2026-03-05 09:05:00", 1.0),
            ("2026-03-05 09:10:00", 1.0),
        ]
        changes = _compute_source_changes_prod_bt(bars, tf_minutes=5)
        assert len(changes) == 1
        assert changes[0] == PositionChange(_dt("2026-03-05 09:05:00"), 1.0)

    def test_emits_at_each_position_transition(self):
        bars = [
            ("2026-03-05 09:00:00", 1.0),
            ("2026-03-05 09:05:00", -1.0),
            ("2026-03-05 09:10:00", -1.0),
            ("2026-03-05 09:15:00", 0.0),
        ]
        changes = _compute_source_changes_prod_bt(bars, tf_minutes=5)
        assert len(changes) == 3
        assert changes[0] == PositionChange(_dt("2026-03-05 09:05:00"), 1.0)
        assert changes[1] == PositionChange(_dt("2026-03-05 09:10:00"), -1.0)
        assert changes[2] == PositionChange(_dt("2026-03-05 09:20:00"), 0.0)

    def test_empty_bars_returns_empty(self):
        assert _compute_source_changes_prod_bt([], tf_minutes=5) == []
```

- [ ] **Step 4.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestSourceChangesProdBt -v`

Expected: Fail with `ImportError` for `_compute_source_changes_prod_bt`.

- [ ] **Step 4.3: Implement `_compute_source_changes_prod_bt`**

Append to `pnl_coverage_audit.py`:

```python
def _compute_source_changes_prod_bt(
    bars: list[tuple[str, float]],
    tf_minutes: int,
) -> list[PositionChange]:
    """Extract position-change points from first-revision source bars (prod/bt).

    Input: list of (bar_ts_str, position) sorted by bar_ts asc, for ONE strategy.
    Output: PositionChange(effective_ts=closing_ts, position) at each transition,
    including the first bar.
    """
    changes: list[PositionChange] = []
    prev_position: float | None = None
    for ts_str, position in bars:
        if prev_position is None or position != prev_position:
            closing_ts = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=tf_minutes)
            changes.append(PositionChange(effective_ts=closing_ts, position=position))
            prev_position = position
    return changes
```

- [ ] **Step 4.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestSourceChangesProdBt -v`

Expected: All 4 tests PASS.

- [ ] **Step 4.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): source-change extractor for prod/bt"
```

---

## Task 5: Source-change extractor for real_trade

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 5.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Source changes: real_trade (uses build_rt_lookup) ────────────────────────


class TestSourceChangesRt:
    def test_uses_accepted_revisions_only(self):
        """Revisions failing the (bar_ts, revision_ts) > prev rule must be discarded.

        Build bars that contain:
          - bar A at 09:00 with rev at 09:00:10 → accepted
          - bar A at 09:00 with rev at 09:00:05 → REJECTED (rev older than prev)
          - bar B at 09:05 with rev at 09:05:10 → accepted
        """
        # bars_with_revs: list of dicts matching what fetch_new_bars_real_trade returns
        bars = [
            {
                "strategy_table_name": "S",
                "ts": "2026-03-05 09:00:00",
                "revision_ts": "2026-03-05 09:00:10",
                "execution_ts": "2026-03-05 09:01:00",
                "closing_ts": "2026-03-05 09:05:00",
                "config_timeframe": "5m",
                "position": 1.0,
            },
            {
                "strategy_table_name": "S",
                "ts": "2026-03-05 09:00:00",
                "revision_ts": "2026-03-05 09:00:05",  # older — REJECT
                "execution_ts": "2026-03-05 09:01:00",
                "closing_ts": "2026-03-05 09:05:00",
                "config_timeframe": "5m",
                "position": -1.0,
            },
            {
                "strategy_table_name": "S",
                "ts": "2026-03-05 09:05:00",
                "revision_ts": "2026-03-05 09:05:10",
                "execution_ts": "2026-03-05 09:06:00",
                "closing_ts": "2026-03-05 09:10:00",
                "config_timeframe": "5m",
                "position": -1.0,
            },
        ]
        changes = _compute_source_changes_rt(bars, stn="S")
        # Accepted: position 1.0 at 09:01, position -1.0 at 09:06.
        # Both transitions are emitted.
        assert len(changes) == 2
        assert changes[0] == PositionChange(_dt("2026-03-05 09:01:00"), 1.0)
        assert changes[1] == PositionChange(_dt("2026-03-05 09:06:00"), -1.0)

    def test_dedups_consecutive_equal_positions(self):
        bars = [
            {
                "strategy_table_name": "S",
                "ts": "2026-03-05 09:00:00",
                "revision_ts": "2026-03-05 09:00:10",
                "execution_ts": "2026-03-05 09:01:00",
                "closing_ts": "2026-03-05 09:05:00",
                "config_timeframe": "5m",
                "position": 1.0,
            },
            {
                "strategy_table_name": "S",
                "ts": "2026-03-05 09:05:00",
                "revision_ts": "2026-03-05 09:05:10",
                "execution_ts": "2026-03-05 09:06:00",
                "closing_ts": "2026-03-05 09:10:00",
                "config_timeframe": "5m",
                "position": 1.0,  # same as prev
            },
        ]
        changes = _compute_source_changes_rt(bars, stn="S")
        assert len(changes) == 1

    def test_unknown_stn_returns_empty(self):
        assert _compute_source_changes_rt([], stn="S") == []
```

- [ ] **Step 5.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestSourceChangesRt -v`

Expected: Fail with `ImportError`.

- [ ] **Step 5.3: Implement `_compute_source_changes_rt`**

Append to `pnl_coverage_audit.py`:

```python
def _compute_source_changes_rt(
    bars_with_revs: list[dict],
    stn: str,
) -> list[PositionChange]:
    """Extract position-change points for real_trade.

    Input: all revisions for one strategy as returned by
    fetch_new_bars_real_trade (each row has ts, revision_ts, execution_ts,
    closing_ts, position, config_timeframe). bars_with_revs may contain
    multiple strategies — only rows whose strategy_table_name == stn are used.
    Filtering is via build_rt_lookup which applies the
    (bar_ts, revision_ts) > prev_accepted rule.

    Output: PositionChange(effective_ts=execution_ts, position) at each
    transition between accepted revisions.
    """
    relevant = [b for b in bars_with_revs if b.get("strategy_table_name") == stn]
    if not relevant:
        return []
    lookup = build_rt_lookup(relevant)
    entries = lookup.get(stn, [])
    changes: list[PositionChange] = []
    prev_position: float | None = None
    for entry in entries:
        pos = float(entry.rev["position"])
        if prev_position is None or pos != prev_position:
            changes.append(PositionChange(effective_ts=entry.execution_ts, position=pos))
            prev_position = pos
    return changes
```

- [ ] **Step 5.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestSourceChangesRt -v`

Expected: All 3 tests PASS.

- [ ] **Step 5.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): source-change extractor for real_trade"
```

---

## Task 6: Phase 3 position boundary diff (1-min tables)

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 6.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Phase 3: 1-min position boundary diff ────────────────────────────────────


class TestPhase3Min:
    def test_identical_sequences_pass(self):
        source = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 09:10:00"), -1.0),
        ]
        target = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 09:10:00"), -1.0),
        ]
        v = _check_phase3("t", "FET", "S", source, target)
        assert v is None

    def test_length_mismatch_fails(self):
        source = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 09:10:00"), -1.0),
        ]
        target = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
        ]
        v = _check_phase3("t", "FET", "S", source, target)
        assert v is not None
        assert v.category == "position_mismatch"
        assert "length" in v.detail.lower()

    def test_value_mismatch_fails(self):
        source = [PositionChange(_dt("2026-03-05 09:05:00"), 1.0)]
        target = [PositionChange(_dt("2026-03-05 09:05:00"), -1.0)]
        v = _check_phase3("t", "FET", "S", source, target)
        assert v is not None
        assert v.category == "position_mismatch"
        assert "1.0" in v.detail and "-1.0" in v.detail

    def test_ts_within_tolerance_passes(self):
        """If timestamps differ by <= POS_TS_TOLERANCE_MIN (1 min), match."""
        source = [PositionChange(_dt("2026-03-05 09:05:00"), 1.0)]
        target = [PositionChange(_dt("2026-03-05 09:05:30"), 1.0)]
        v = _check_phase3("t", "FET", "S", source, target)
        assert v is None

    def test_ts_outside_tolerance_fails(self):
        source = [PositionChange(_dt("2026-03-05 09:05:00"), 1.0)]
        target = [PositionChange(_dt("2026-03-05 09:10:00"), 1.0)]
        v = _check_phase3("t", "FET", "S", source, target)
        assert v is not None
        assert v.category == "position_mismatch"
```

- [ ] **Step 6.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Min -v`

Expected: Fail with `ImportError`.

- [ ] **Step 6.3: Implement `_check_phase3`**

Append to `pnl_coverage_audit.py`:

```python
def _check_phase3(
    table: str,
    underlying: str,
    stn: str,
    source_changes: list[PositionChange],
    target_changes: list[PositionChange],
) -> Violation | None:
    """Compare source and target position-change sequences for 1-min tables.

    Fails on first mismatch. Length differs → fail. ts differs by more than
    POS_TS_TOLERANCE_MIN minutes → fail. position differs (exact) → fail.
    """
    if len(source_changes) != len(target_changes):
        return Violation(
            table=table,
            underlying=underlying,
            stn=stn,
            category="position_mismatch",
            detail=f"length mismatch: source={len(source_changes)} target={len(target_changes)}",
            severity_minutes=abs(len(source_changes) - len(target_changes)),
        )
    tol = timedelta(minutes=POS_TS_TOLERANCE_MIN)
    for i, (src, tgt) in enumerate(zip(source_changes, target_changes)):
        if abs(src.effective_ts - tgt.effective_ts) > tol:
            return Violation(
                table=table,
                underlying=underlying,
                stn=stn,
                category="position_mismatch",
                detail=f"ts mismatch at #{i}: source={src.effective_ts:%Y-%m-%d %H:%M:%S} target={tgt.effective_ts:%Y-%m-%d %H:%M:%S}",
                severity_minutes=1,
            )
        if src.position != tgt.position:
            return Violation(
                table=table,
                underlying=underlying,
                stn=stn,
                category="position_mismatch",
                detail=f"position mismatch at #{i}: source={src.position} target={tgt.position} (ts={src.effective_ts:%Y-%m-%d %H:%M:%S})",
                severity_minutes=1,
            )
    return None
```

- [ ] **Step 6.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Min -v`

Expected: All 5 tests PASS.

- [ ] **Step 6.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): phase 3 position boundary diff for 1-min tables"
```

---

## Task 7: Phase 3 hour-slot position check (1-hour tables)

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 7.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Phase 3 (hour table): slot position match ────────────────────────────────


class TestPhase3Hour:
    def test_hour_slots_match_minute_argmax(self):
        """For each hour slot, position should equal the latest 1-min position <= hour+1h."""
        # 1-min target had changes at 09:05 → 1.0, 09:30 → -1.0
        # Hour slot 09:00 should reflect argMax(position, minute_ts) = -1.0
        # (because 09:30 is the latest minute_ts in [09:00, 10:00))
        target_min_changes = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 09:30:00"), -1.0),
        ]
        hour_rows = [
            (_dt("2026-03-05 09:00:00"), -1.0),  # correct
        ]
        v = _check_phase3_hour("t1h", "FET", "S", hour_rows, target_min_changes)
        assert v is None

    def test_hour_slot_position_drift_fails(self):
        target_min_changes = [PositionChange(_dt("2026-03-05 09:30:00"), -1.0)]
        hour_rows = [(_dt("2026-03-05 09:00:00"), 1.0)]  # wrong
        v = _check_phase3_hour("t1h", "FET", "S", hour_rows, target_min_changes)
        assert v is not None
        assert v.category == "position_mismatch"

    def test_hour_slot_with_no_prior_minute_change_skips(self):
        """If a 1-hour row exists before any 1-min change, we cannot compare — skip."""
        target_min_changes = [PositionChange(_dt("2026-03-05 10:00:00"), 1.0)]
        hour_rows = [(_dt("2026-03-05 09:00:00"), 0.0)]  # no prior min change
        v = _check_phase3_hour("t1h", "FET", "S", hour_rows, target_min_changes)
        assert v is None  # can't verify, skip silently
```

- [ ] **Step 7.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Hour -v`

Expected: Fail with `ImportError`.

- [ ] **Step 7.3: Implement `_check_phase3_hour`**

Append to `pnl_coverage_audit.py`:

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

- [ ] **Step 7.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestPhase3Hour -v`

Expected: All 3 tests PASS.

- [ ] **Step 7.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): phase 3 hour-slot position check"
```

---

## Task 8: Report formatter

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 8.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Report formatter ─────────────────────────────────────────────────────────


class TestFormatReport:
    def test_empty_report_returns_clean_message(self):
        from services.dagster.trading_dagster.assets.pnl_coverage_audit import AuditReport
        report = AuditReport()
        msg = _format_report(report)
        assert "CLEAN" in msg

    def test_groups_by_table_and_sorts_top_n(self):
        from services.dagster.trading_dagster.assets.pnl_coverage_audit import AuditReport
        report = AuditReport()
        # Three violations, two in the same table.
        report.violations = [
            Violation("t1", "FET", "S1", "start_gap", "...", severity_minutes=84000),
            Violation("t1", "FET", "S2", "start_gap", "...", severity_minutes=1000),
            Violation("t2", "ETH", "S3", "stale_end", "...", severity_minutes=30),
        ]
        msg = _format_report(report)
        # Both tables represented.
        assert "[t1]" in msg
        assert "[t2]" in msg
        # Top offender (84000 min) listed first in t1.
        t1_block = msg[msg.index("[t1]") : msg.index("[t2]")]
        assert t1_block.index("S1") < t1_block.index("S2")
```

- [ ] **Step 8.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestFormatReport -v`

Expected: Fail with `ImportError`.

- [ ] **Step 8.3: Implement `_format_report`**

Append to `pnl_coverage_audit.py`:

```python
def _format_report(report: AuditReport) -> str:
    """Render a human-readable summary of violations grouped by table.

    Within each table, top N offenders by severity_minutes appear first.
    """
    if not report.violations:
        return f"PnL COVERAGE & POSITION AUDIT CLEAN — {report.tables_checked} tables, {report.strategies_checked} strategies, {report.duration_secs:.1f}s"

    by_table: dict[str, list[Violation]] = {}
    for v in report.violations:
        by_table.setdefault(v.table, []).append(v)

    lines: list[str] = ["PnL COVERAGE & POSITION AUDIT FAILED", ""]
    for table in sorted(by_table.keys()):
        violations = by_table[table]
        # Per-category counts.
        by_cat: dict[str, int] = {}
        for v in violations:
            by_cat[v.category] = by_cat.get(v.category, 0) + 1

        lines.append(f"[{table}]")
        for cat in ("start_gap", "stale_end", "internal_holes", "position_mismatch"):
            if cat in by_cat:
                lines.append(f"  {cat}: {by_cat[cat]} strategies")

        # Top-N offenders.
        worst = sorted(violations, key=lambda v: v.severity_minutes, reverse=True)[:TOP_N_OFFENDERS]
        lines.append(f"  Top {len(worst)} worst:")
        for v in worst:
            lines.append(f"    {v.underlying} {v.stn[:80]}  {v.category}  {v.detail}")
        lines.append("")

    return "\n".join(lines)
```

- [ ] **Step 8.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestFormatReport -v`

Expected: All 2 tests PASS.

- [ ] **Step 8.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): report formatter"
```

---

## Task 9: Fetch functions (Q_src, Q_src_rt, Q_px, Q_stat, Q_trans, Q_gap)

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`

These are thin SQL wrappers. Validation is via a smoke test against real ClickHouse rather than mocked unit tests (which would only re-encode the SQL strings).

- [ ] **Step 9.1: Implement all six fetch functions**

Append to `pnl_coverage_audit.py`:

```python
def _underlying_to_instrument(u: str) -> str:
    u = u.upper()
    return u if u.endswith("USDT") else f"{u}USDT"


def _fetch_q_stat(target_table: str, underlying: str, client) -> dict[str, StratStat]:
    """Q_stat: per-strategy min(ts)/max(ts)/count() in target_table for one U."""
    rows = query_rows(
        f"""
SELECT strategy_table_name, min(ts), max(ts), count()
FROM analytics.{target_table}
WHERE underlying = '{underlying}'
GROUP BY strategy_table_name
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    out: dict[str, StratStat] = {}
    for r in rows:
        stn = str(r[0])
        out[stn] = StratStat(
            actual_min_ts=r[1],
            actual_max_ts=r[2],
            actual_rows=int(r[3]),
        )
    return out


def _fetch_q_px(underlying: str, client) -> set[datetime]:
    """Q_px: 1-min price timestamps for one U since GLOBAL_START_TS."""
    instrument = _underlying_to_instrument(underlying)
    rows = query_rows(
        f"""
SELECT ts FROM analytics.futures_price_1min
WHERE instrument = '{instrument}' AND ts >= toDateTime('{GLOBAL_START_TS}')
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return {r[0] for r in rows}


def _fetch_q_src_prod_bt(source_table: str, underlying: str, client) -> dict[str, list[tuple[str, float]]]:
    """Q_src (prod/bt): first-revision (ts, position) per strategy for one U.

    Also returns the (constant) tf as part of a sibling query — captured by
    _fetch_q_src_tf below.
    """
    rows = query_rows(
        f"""
SELECT
  strategy_table_name,
  toString(ts),
  argMin(JSONExtract(row_json, 'position', 'Float64'), revision_ts)
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
GROUP BY strategy_table_name, ts
ORDER BY strategy_table_name, ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    out: dict[str, list[tuple[str, float]]] = {}
    for r in rows:
        stn = str(r[0])
        out.setdefault(stn, []).append((str(r[1]), float(r[2])))
    return out


def _fetch_q_src_tf(source_table: str, underlying: str, client) -> dict[str, int]:
    """Per-strategy config_timeframe (in minutes) for one U."""
    rows = query_rows(
        f"""
SELECT strategy_table_name,
       any(JSONExtractString(row_json, 'config_timeframe'))
FROM analytics.{source_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
GROUP BY strategy_table_name
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    out: dict[str, int] = {}
    for r in rows:
        tf_str = str(r[1])
        out[str(r[0])] = TIMEFRAME_MAP.get(tf_str, 5)
    return out


def _fetch_q_src_rt(underlying: str, client) -> list[dict]:
    """Q_src_rt: all revisions for one U, in (stn, ts, revision_ts) order.

    Returns rows already enriched with the fields that build_rt_lookup expects:
    ts, revision_ts, execution_ts, closing_ts, position, config_timeframe,
    strategy_table_name.
    """
    rows = query_rows(
        f"""
SELECT
  strategy_table_name,
  toString(ts),
  toString(revision_ts),
  toString(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
  JSONExtract(row_json, 'position', 'Float64') AS position,
  JSONExtractString(row_json, 'config_timeframe') AS tf
FROM analytics.strategy_output_history_v2
WHERE underlying = '{underlying}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
ORDER BY strategy_table_name, ts, revision_ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    out: list[dict] = []
    for r in rows:
        stn, ts_s, rev_s, exec_s, position, tf = r
        tf_minutes = TIMEFRAME_MAP.get(tf, 5)
        # closing_ts = ts + tf_minutes
        ts_dt = datetime.strptime(str(ts_s)[:19], "%Y-%m-%d %H:%M:%S")
        closing_dt = ts_dt + timedelta(minutes=tf_minutes)
        out.append(
            {
                "strategy_table_name": str(stn),
                "ts": str(ts_s),
                "revision_ts": str(rev_s),
                "execution_ts": str(exec_s),
                "closing_ts": closing_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "position": float(position),
                "config_timeframe": str(tf),
            }
        )
    return out


def _fetch_q_trans(target_table: str, underlying: str, stn: str, client) -> list[PositionChange]:
    """Q_trans: per-strategy position transitions in target_table.

    Uses lagInFrame over a single-partition window — bounded memory.
    """
    rows = query_rows(
        f"""
SELECT ts, position
FROM (
  SELECT ts, position,
         lagInFrame(position) OVER (ORDER BY ts) AS prev_pos,
         row_number()         OVER (ORDER BY ts) AS rn
  FROM analytics.{target_table}
  WHERE underlying = '{underlying}'
    AND strategy_table_name = '{stn}'
)
WHERE position != prev_pos OR rn = 1
ORDER BY ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return [PositionChange(effective_ts=r[0], position=float(r[1])) for r in rows]


def _fetch_q_gap(target_table: str, underlying: str, stn: str, client) -> list[tuple[datetime, int]]:
    """Q_gap: per-strategy ts gaps > 60s in target_table. Returns (gap_end, gap_secs)."""
    rows = query_rows(
        f"""
SELECT gap_end, gap_secs FROM (
  SELECT ts AS gap_end,
         toUnixTimestamp(ts) AS ts_secs,
         lagInFrame(toUnixTimestamp(ts)) OVER (ORDER BY ts) AS prev_ts_secs,
         (toUnixTimestamp(ts) - lagInFrame(toUnixTimestamp(ts)) OVER (ORDER BY ts)) AS gap_secs
  FROM analytics.{target_table}
  WHERE underlying = '{underlying}'
    AND strategy_table_name = '{stn}'
)
WHERE prev_ts_secs > 0 AND gap_secs > 60
ORDER BY gap_secs DESC
LIMIT 50
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return [(r[0], int(r[1])) for r in rows]


def _fetch_q_stat_hour(hour_table: str, underlying: str, client) -> dict[str, list[tuple[datetime, float]]]:
    """Q_stat variant for hour tables — returns per-strategy (ts, position) rows."""
    rows = query_rows(
        f"""
SELECT strategy_table_name, ts, position
FROM analytics.{hour_table}
WHERE underlying = '{underlying}'
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

- [ ] **Step 9.2: Smoke-test against real ClickHouse via `.env`**

Run: 

```bash
set -a && source .env && set +a && python3 -c "
from services.dagster.trading_dagster.assets.pnl_coverage_audit import (
    _fetch_q_stat, _fetch_q_px, _fetch_q_src_prod_bt, _fetch_q_src_tf,
    _fetch_q_src_rt, _fetch_q_trans, _fetch_q_gap, _fetch_q_stat_hour,
)
from libs.clickhouse_client import get_client
c = get_client()
stat = _fetch_q_stat('strategy_pnl_1min_prod_v2', 'FET', c)
print(f'Q_stat FET: {len(stat)} strategies')
px = _fetch_q_px('FET', c)
print(f'Q_px FET: {len(px)} timestamps')
src = _fetch_q_src_prod_bt('strategy_output_history_v2', 'FET', c)
print(f'Q_src FET: {len(src)} strategies, e.g. first has {len(next(iter(src.values())))} bars')
tf = _fetch_q_src_tf('strategy_output_history_v2', 'FET', c)
print(f'Q_src_tf FET: {len(tf)} strategies, sample tf={next(iter(tf.values()))}')
rt = _fetch_q_src_rt('FET', c)
print(f'Q_src_rt FET: {len(rt)} revisions')
stn = next(iter(stat))
trans = _fetch_q_trans('strategy_pnl_1min_prod_v2', 'FET', stn, c)
print(f'Q_trans FET/{stn[:40]}: {len(trans)} transitions')
gap = _fetch_q_gap('strategy_pnl_1min_prod_v2', 'FET', stn, c)
print(f'Q_gap FET/{stn[:40]}: {len(gap)} gaps')
hr = _fetch_q_stat_hour('strategy_pnl_1hour_prod_v2', 'FET', c)
print(f'Q_stat_hour FET: {len(hr)} strategies, sample={next(iter(hr.values()))[:2]}')
"
```

Expected: All eight prints succeed with non-zero counts, no exceptions.

- [ ] **Step 9.3: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py
git commit -m "feat(audit): fetch functions for source/target/price queries"
```

---

## Task 10: Per-underlying driver `_audit_underlying`

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

This wires all phases together for one `(target_table, underlying, mode)` triple. Tests use monkeypatched fetch functions.

- [ ] **Step 10.1: Write failing tests**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Driver: per-underlying audit ─────────────────────────────────────────────


class TestAuditUnderlying:
    def test_clean_underlying_returns_no_violations(self, monkeypatch):
        """All phases pass → empty violations list."""
        from services.dagster.trading_dagster.assets import pnl_coverage_audit as mod

        monkeypatch.setattr(mod, "_fetch_q_stat", lambda t, u, c: {
            "S1": StratStat(_dt("2026-03-05 09:05:00"), _dt("2026-05-26 09:55:00"), 2),
        })
        monkeypatch.setattr(mod, "_fetch_q_px", lambda u, c: _price_set(
            "2026-03-05 09:05:00", "2026-05-26 09:55:00"))
        monkeypatch.setattr(mod, "_fetch_q_src_prod_bt", lambda st, u, c: {
            "S1": [("2026-03-05 09:00:00", 1.0)],
        })
        monkeypatch.setattr(mod, "_fetch_q_src_tf", lambda st, u, c: {"S1": 5})
        monkeypatch.setattr(mod, "_fetch_q_trans", lambda t, u, s, c: [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
        ])
        monkeypatch.setattr(mod, "_fetch_q_gap", lambda t, u, s, c: [])

        report = mod._audit_underlying(
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            mode="prod",
            underlying="FET",
            client=None,
            now_ts=_dt("2026-05-26 10:00:00"),
        )
        assert report.violations == []

    def test_start_gap_is_detected(self, monkeypatch):
        from services.dagster.trading_dagster.assets import pnl_coverage_audit as mod

        # Target starts 2 months late.
        monkeypatch.setattr(mod, "_fetch_q_stat", lambda t, u, c: {
            "S1": StratStat(_dt("2026-05-02 20:44:00"), _dt("2026-05-26 09:55:00"), 100),
        })
        monkeypatch.setattr(mod, "_fetch_q_px", lambda u, c: _price_set("2026-03-05 09:05:00"))
        monkeypatch.setattr(mod, "_fetch_q_src_prod_bt", lambda st, u, c: {
            "S1": [("2026-03-05 09:00:00", 1.0)],
        })
        monkeypatch.setattr(mod, "_fetch_q_src_tf", lambda st, u, c: {"S1": 5})
        monkeypatch.setattr(mod, "_fetch_q_trans", lambda t, u, s, c: [
            PositionChange(_dt("2026-05-02 20:45:00"), 1.0),
        ])
        monkeypatch.setattr(mod, "_fetch_q_gap", lambda t, u, s, c: [])

        report = mod._audit_underlying(
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            mode="prod",
            underlying="FET",
            client=None,
            now_ts=_dt("2026-05-26 10:00:00"),
        )
        # Phase 1 should report start_gap and skip phase 3 for that strategy.
        assert any(v.category == "start_gap" for v in report.violations)
```

- [ ] **Step 10.2: Run tests to verify they fail**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestAuditUnderlying -v`

Expected: Fail with `AttributeError` for `_audit_underlying`.

- [ ] **Step 10.3: Implement `_audit_underlying`**

Append to `pnl_coverage_audit.py`:

```python
def _audit_underlying(
    target_table: str,
    source_table: str,
    mode: Mode,
    underlying: str,
    client,
    now_ts: datetime,
) -> AuditReport:
    """Run phases 1-3 for all strategies of one (target_table, underlying)."""
    report = AuditReport()

    # ── Fetch shared per-U projections ───────────────────────────────────────
    stat_by_stn = _fetch_q_stat(target_table, underlying, client)
    if not stat_by_stn:
        return report  # target has nothing for this U — nothing to verify
    price_set = _fetch_q_px(underlying, client)

    if mode == "real_trade":
        rt_revs = _fetch_q_src_rt(underlying, client)
        # Build first-bar info per stn from accepted revisions.
        src_first_by_stn: dict[str, SourceFirstBar] = {}
        # Group revs by stn first.
        revs_by_stn: dict[str, list[dict]] = {}
        for rev in rt_revs:
            revs_by_stn.setdefault(rev["strategy_table_name"], []).append(rev)
        for stn, revs in revs_by_stn.items():
            lookup = build_rt_lookup(revs)
            entries = lookup.get(stn, [])
            if not entries:
                continue
            tf = TIMEFRAME_MAP.get(entries[0].rev["config_timeframe"], 5)
            src_first_by_stn[stn] = SourceFirstBar(
                expected_min_ts=entries[0].execution_ts, tf_minutes=tf
            )
    else:
        bars_by_stn = _fetch_q_src_prod_bt(source_table, underlying, client)
        tf_by_stn = _fetch_q_src_tf(source_table, underlying, client)
        src_first_by_stn = {}
        for stn, bars in bars_by_stn.items():
            if not bars:
                continue
            tf = tf_by_stn.get(stn, 5)
            first_bar_ts = datetime.strptime(str(bars[0][0])[:19], "%Y-%m-%d %H:%M:%S")
            src_first_by_stn[stn] = SourceFirstBar(
                expected_min_ts=first_bar_ts + timedelta(minutes=tf),
                tf_minutes=tf,
            )

    # ── Per-strategy loop ────────────────────────────────────────────────────
    for stn, stat in stat_by_stn.items():
        report.strategies_checked += 1
        source = src_first_by_stn.get(stn)
        if source is None:
            # Target has rows for a strategy not in source — orphaned.
            report.violations.append(
                Violation(
                    table=target_table,
                    underlying=underlying,
                    stn=stn,
                    category="position_mismatch",  # closest existing category
                    detail="strategy present in target but not in source",
                    severity_minutes=stat.actual_rows,
                )
            )
            continue

        # Phase 1
        v1 = _check_phase1(target_table, underlying, stn, stat, source, price_set, now_ts)
        if v1 is not None:
            report.violations.append(v1)
            # Phase 2 drilldown for any flagged strategy.
            q_gap = _fetch_q_gap(target_table, underlying, stn, client)
            gaps = _check_phase2(underlying, stn, q_gap, price_set)
            for g in gaps:
                report.violations.append(
                    Violation(
                        table=target_table,
                        underlying=underlying,
                        stn=stn,
                        category="internal_holes",
                        detail=f"gap ending {g.gap_end:%Y-%m-%d %H:%M:%S} ({g.gap_minutes}m)",
                        severity_minutes=g.gap_minutes,
                    )
                )
            # Skip Phase 3 for flagged strategies — usually the position
            # sequence is misleading when coverage is broken.
            continue

        # Phase 3 (only when Phase 1 is clean)
        target_changes = _fetch_q_trans(target_table, underlying, stn, client)
        if mode == "real_trade":
            source_changes = _compute_source_changes_rt(revs_by_stn.get(stn, []), stn)
        else:
            source_changes = _compute_source_changes_prod_bt(
                bars_by_stn.get(stn, []), source.tf_minutes
            )
        v3 = _check_phase3(target_table, underlying, stn, source_changes, target_changes)
        if v3 is not None:
            report.violations.append(v3)

    return report
```

- [ ] **Step 10.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestAuditUnderlying -v`

Expected: Both tests PASS.

- [ ] **Step 10.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): per-underlying driver wiring phases 1-3"
```

---

## Task 11: Per-target-table driver and the Dagster asset

**Files:**
- Modify: `services/dagster/trading_dagster/assets/pnl_coverage_audit.py`
- Modify: `services/dagster/tests/test_pnl_coverage_audit.py`

- [ ] **Step 11.1: Write failing test for the table-level driver**

Append to `services/dagster/tests/test_pnl_coverage_audit.py`:

```python
# ── Driver: per-table audit ──────────────────────────────────────────────────


class TestAuditTable:
    def test_loops_underlyings_collects_all_violations(self, monkeypatch):
        from services.dagster.trading_dagster.assets import pnl_coverage_audit as mod

        # Two underlyings each with one violation.
        def fake_audit_underlying(target_table, source_table, mode, underlying, client, now_ts):
            r = AuditReport()
            r.strategies_checked = 1
            r.violations = [Violation(target_table, underlying, "S1", "start_gap", "...", 100)]
            return r

        monkeypatch.setattr(mod, "_audit_underlying", fake_audit_underlying)
        monkeypatch.setattr(mod, "_list_underlyings", lambda t, c: ["FET", "ETH"])

        report = mod._audit_table(
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            mode="prod",
            client=None,
            now_ts=_dt("2026-05-26 10:00:00"),
        )
        assert len(report.violations) == 2
        assert report.strategies_checked == 2
```

- [ ] **Step 11.2: Run test to verify it fails**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py::TestAuditTable -v`

Expected: Fail with `AttributeError` for `_audit_table`.

- [ ] **Step 11.3: Implement `_list_underlyings`, `_audit_table`, hour-table driver, and replace the asset placeholder**

In `pnl_coverage_audit.py`, **delete** the existing placeholder `pnl_coverage_audit_asset` function and append:

```python
def _list_underlyings(target_table: str, client) -> list[str]:
    """Distinct underlyings present in the target table."""
    rows = query_rows(
        f"SELECT DISTINCT underlying FROM analytics.{target_table} "
        f"WHERE underlying IS NOT NULL AND underlying != ''",
        client=client,
    )
    return sorted(str(r[0]) for r in rows)


def _audit_table(
    target_table: str,
    source_table: str,
    mode: Mode,
    client,
    now_ts: datetime,
) -> AuditReport:
    """Run audit over all underlyings of one target table.

    Uses ThreadPoolExecutor for per-underlying parallelism. Each thread gets
    its own ClickHouse client (created on demand) since `clickhouse-connect`
    clients are not thread-safe to share.
    """
    underlyings = _list_underlyings(target_table, client)
    aggregate = AuditReport()

    def _run(u: str) -> AuditReport:
        # Per-thread client; cheap to create.
        thread_client = get_client()
        return _audit_underlying(
            target_table=target_table,
            source_table=source_table,
            mode=mode,
            underlying=u,
            client=thread_client,
            now_ts=now_ts,
        )

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(_run, u): u for u in underlyings}
        for future in as_completed(futures):
            sub = future.result()
            aggregate.violations.extend(sub.violations)
            aggregate.strategies_checked += sub.strategies_checked

    return aggregate


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
        hr_by_stn = _fetch_q_stat_hour(hour_table, u, client)
        for stn, rows in hr_by_stn.items():
            target_min_changes = _fetch_q_trans(min_table, u, stn, client)
            v = _check_phase3_hour(hour_table, u, stn, rows, target_min_changes)
            aggregate.strategies_checked += 1
            if v is not None:
                aggregate.violations.append(v)
    return aggregate


@asset(
    name="pnl_coverage_audit",
    group_name="strategy_pnl",
    compute_kind="clickhouse",
    description="Daily read-only audit of PnL tables: per-(U, S) coverage + position-boundary checks.",
    op_tags={"dagster/timeout": 3600},
)
def pnl_coverage_audit_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Audit all six PnL tables; raise RuntimeError on any violation."""
    import time as _time

    started = _time.time()
    now_ts = datetime.now(tz=UTC).replace(tzinfo=None)
    full_report = AuditReport()

    for target, source, mode in _TARGET_TABLES:
        context.log.info(f"[audit] starting {target} ({mode})")
        sub = _audit_table(
            target_table=target,
            source_table=source,
            mode=mode,
            client=get_client(),
            now_ts=now_ts,
        )
        context.log.info(
            f"[audit] {target}: {sub.strategies_checked} strategies, {len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1

    for hour, min_t, _mode in _HOUR_TABLES:
        context.log.info(f"[audit] starting hour table {hour}")
        sub = _audit_hour_table(hour_table=hour, min_table=min_t, client=get_client())
        context.log.info(
            f"[audit] {hour}: {sub.strategies_checked} strategies, {len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1

    full_report.duration_secs = _time.time() - started
    report_str = _format_report(full_report)
    context.log.info(report_str)

    if full_report.has_failures():
        raise RuntimeError(report_str)

    return MaterializeResult(
        metadata={
            "tables_checked": full_report.tables_checked,
            "strategies_checked": full_report.strategies_checked,
            "duration_secs": round(full_report.duration_secs, 1),
        }
    )


# ── Job & schedule definitions ──────────────────────────────────────────────

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

- [ ] **Step 11.4: Run tests to verify they pass**

Run: `pytest services/dagster/tests/test_pnl_coverage_audit.py -v`

Expected: All tests across `TestPhase1`, `TestPhase2`, `TestSourceChangesProdBt`, `TestSourceChangesRt`, `TestPhase3Min`, `TestPhase3Hour`, `TestFormatReport`, `TestAuditUnderlying`, `TestAuditTable` PASS.

- [ ] **Step 11.5: Commit**

```bash
git add services/dagster/trading_dagster/assets/pnl_coverage_audit.py services/dagster/tests/test_pnl_coverage_audit.py
git commit -m "feat(audit): per-table driver, asset, job, schedule"
```

---

## Task 12: Wire into Dagster definitions

**Files:**
- Modify: `services/dagster/trading_dagster/definitions/__init__.py`

- [ ] **Step 12.1: Add the asset and schedule to definitions**

Edit `services/dagster/trading_dagster/definitions/__init__.py`. Add an import and register:

Replace:

```python
from ..assets.pnl_strategy_v2 import (
    pnl_bt_v2_full_asset,
    pnl_hourly_rollup_asset,
    pnl_hourly_rollup_job,
    pnl_hourly_rollup_schedule,
    pnl_prod_v2_full_asset,
    pnl_real_trade_v2_full_asset,
)
from ..assets.postgres_cleanup import postgres_cleanup_asset
```

With:

```python
from ..assets.pnl_coverage_audit import (
    pnl_coverage_audit_asset,
    pnl_coverage_audit_job,
    pnl_coverage_audit_schedule,
)
from ..assets.pnl_strategy_v2 import (
    pnl_bt_v2_full_asset,
    pnl_hourly_rollup_asset,
    pnl_hourly_rollup_job,
    pnl_hourly_rollup_schedule,
    pnl_prod_v2_full_asset,
    pnl_real_trade_v2_full_asset,
)
from ..assets.postgres_cleanup import postgres_cleanup_asset
```

Replace:

```python
all_assets = [
    # Market data (backfill only — real-time via pnl_consumer)
    binance_futures_backfill_asset,
    # Full recomputes (unpartitioned — use when rewriting history from scratch)
    pnl_prod_v2_full_asset,
    pnl_real_trade_v2_full_asset,
    pnl_bt_v2_full_asset,
    # Hourly rollup (streaming supplement — runs every hour via schedule)
    pnl_hourly_rollup_asset,
    # Infra checks
    postgres_cleanup_asset,
]
```

With:

```python
all_assets = [
    # Market data (backfill only — real-time via pnl_consumer)
    binance_futures_backfill_asset,
    # Full recomputes (unpartitioned — use when rewriting history from scratch)
    pnl_prod_v2_full_asset,
    pnl_real_trade_v2_full_asset,
    pnl_bt_v2_full_asset,
    # Hourly rollup (streaming supplement — runs every hour via schedule)
    pnl_hourly_rollup_asset,
    # Daily coverage & position audit (read-only, fails on gaps)
    pnl_coverage_audit_asset,
    # Infra checks
    postgres_cleanup_asset,
]
```

Replace:

```python
defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
    schedules=[pnl_hourly_rollup_schedule],
    jobs=[pnl_hourly_rollup_job],
)
```

With:

```python
defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
    schedules=[pnl_hourly_rollup_schedule, pnl_coverage_audit_schedule],
    jobs=[pnl_hourly_rollup_job, pnl_coverage_audit_job],
)
```

- [ ] **Step 12.2: Verify Dagster loads the definitions cleanly**

Run: `python3 -c "from services.dagster.trading_dagster.definitions import defs; print([a.key.to_user_string() for a in defs.get_asset_graph().assets_defs])"`

Expected: prints the list including `pnl_coverage_audit`, with no exceptions.

- [ ] **Step 12.3: Run the full test suite**

Run: `pytest services/dagster/tests/ -v`

Expected: all tests pass (existing tests should be unaffected).

- [ ] **Step 12.4: Commit**

```bash
git add services/dagster/trading_dagster/definitions/__init__.py
git commit -m "feat(audit): register pnl_coverage_audit asset + daily schedule"
```

---

## Task 13: End-to-end smoke test against real ClickHouse

**Files:**
- No code changes.

This validates the whole audit runs against production data and surfaces the FET start_gap correctly.

- [ ] **Step 13.1: Run the asset locally via Dagster CLI**

Run:

```bash
set -a && source .env && set +a && \
python3 -c "
from datetime import datetime, UTC
from services.dagster.trading_dagster.assets.pnl_coverage_audit import (
    _audit_table,
)
from libs.clickhouse_client import get_client

now_ts = datetime.now(tz=UTC).replace(tzinfo=None)
report = _audit_table(
    target_table='strategy_pnl_1min_prod_v2',
    source_table='strategy_output_history_v2',
    mode='prod',
    client=get_client(),
    now_ts=now_ts,
)
print(f'strategies_checked={report.strategies_checked}')
print(f'violations={len(report.violations)}')
for v in report.violations[:5]:
    print(f'  {v.underlying} {v.stn[:60]}  {v.category}  {v.detail}')
"
```

Expected: `violations > 0` and FET strategies appear with `start_gap` showing expected `2026-03-05` vs actual `2026-05-02`. Total runtime under 5 minutes.

- [ ] **Step 13.2: Spot-check that a clean underlying produces no violations**

After fixing FET (out of scope for this plan — would be a separate full-recompute run), the same script should report `violations=0` for that underlying. For now, verify the non-FET underlyings don't all flag — i.e., the check isn't producing false positives across the board:

```bash
set -a && source .env && set +a && python3 -c "
from datetime import datetime, UTC
from services.dagster.trading_dagster.assets.pnl_coverage_audit import _audit_underlying
from libs.clickhouse_client import get_client

now_ts = datetime.now(tz=UTC).replace(tzinfo=None)
for u in ['BTC', 'ETH', 'SOL']:
    r = _audit_underlying(
        target_table='strategy_pnl_1min_prod_v2',
        source_table='strategy_output_history_v2',
        mode='prod',
        underlying=u,
        client=get_client(),
        now_ts=now_ts,
    )
    print(f'{u}: strategies={r.strategies_checked} violations={len(r.violations)}')
"
```

Expected: each of BTC/ETH/SOL prints reasonable counts. Some violations are acceptable (the audit may find real issues!) but not e.g. "all 137 ETH strategies have start_gap" — that would indicate a logic bug. If the rate is implausibly high, treat as a finding and re-investigate before declaring the plan complete.

- [ ] **Step 13.3: Final commit (if any debugging adjustments were made)**

If no further changes, this step is a no-op. Otherwise:

```bash
git add -p
git commit -m "fix(audit): <description of finding from smoke test>"
```

---

## Plan complete

The audit asset is now wired into Dagster with a daily schedule. The FET historical gap will be flagged on the next 06:00 UTC run. Operator response: trigger a manual `pnl_prod_v2_full` materialization with a wider window (or pursue the separate out-of-scope fix to extend `_get_underlying_resume_dt`).
