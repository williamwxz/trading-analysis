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


# ── Phase 1: coverage check ─────────────────────────────────────────────────


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


# ── Stubs replaced in later tasks ─────────────────────────────────────────
# Phase 1 implementation lives above this block. The functions below are
# placeholder stubs so that test files can import the full surface area now
# even though Tasks 3-8 haven't been implemented yet.

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
        raw_missing = gap_secs // 60
        if raw_missing <= 0:
            continue
        if not price_set:
            # No price data available — cannot exempt any minutes; all raw
            # gap minutes are real holes.
            effective_holes = raw_missing
        else:
            # Count price-present minutes strictly inside (gap_start, gap_end)
            # exclusive of both endpoints. If none are present the gap is fully
            # explained by a price outage and we drop it.
            # Otherwise effective = raw_missing - no_price_strictly_inside
            #                     = 1 + present_strictly_inside
            # (+1 accounts for the gap-start boundary being a price-covered minute
            # in a gap where at least one interior minute also has a price).
            present_strictly = sum(
                1
                for p in price_set
                if gap_start_exclusive < p < gap_end
            )
            effective_holes = (1 + present_strictly) if present_strictly > 0 else 0
        if effective_holes <= 0:
            continue
        descriptors.append(
            GapDescriptor(stn=stn, gap_end=gap_end, gap_minutes=effective_holes)
        )

    descriptors.sort(key=lambda d: d.gap_minutes, reverse=True)
    return descriptors[:top_n]

def _check_phase3(*args, **kwargs):
    raise NotImplementedError

def _check_phase3_hour(*args, **kwargs):
    raise NotImplementedError

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

def _compute_source_changes_rt(*args, **kwargs):
    raise NotImplementedError

def _format_report(*args, **kwargs):
    raise NotImplementedError


# ── Asset placeholder ────────────────────────────────────────────────────────


def pnl_coverage_audit_asset():
    """Placeholder — replaced in Task 11."""
    raise NotImplementedError
