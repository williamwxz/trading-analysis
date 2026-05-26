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


# ── ClickHouse fetch functions ───────────────────────────────────────────────


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


# ── Per-underlying driver ────────────────────────────────────────────────────


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


# ── Asset placeholder ────────────────────────────────────────────────────────


def pnl_coverage_audit_asset():
    """Placeholder — replaced in Task 11."""
    raise NotImplementedError
