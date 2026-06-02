"""PnL Coverage & Position Audit — Dagster Asset.

Daily read-only check that walks every (underlying, strategy) in the nine PnL
tables (1-min + 1-hour + 1-day, for prod / bt / real_trade) and fails the Dagster run
on any missing minute or position drift.

See docs/superpowers/specs/2026-05-26-pnl-coverage-audit-design.md.
"""

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

from ..utils.clickhouse_client import (
    get_client,
    query_rows,
    query_rows_stream,
    query_scalar,
)

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

# ClickHouse per-query memory cap (500 MB). Every audit query is either an
# aggregate (count/min/max), a single-strategy scan bounded by LIMIT 1 BY, or a
# streamed in-order read — all of which stay well under this. The cap is a guard
# rail, not the primary control: per-strategy scope + streaming keep both
# ClickHouse and Python memory low (runtime is allowed to grow instead).
QUERY_MEMORY_CAP = 524_288_000

# Time-chunk width (days) for the real_trade revision scan. The bt source spans
# 2020->2026; chunking the row_json read keeps each query under the cap.
_SRC_CHUNK_DAYS = 7

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
    (
        "strategy_pnl_1hour_real_trade_v2",
        "strategy_pnl_1min_real_trade_v2",
        "real_trade",
    ),
]

_DAY_TABLES: list[tuple[str, str, Mode]] = [
    ("strategy_pnl_1day_prod_v2", "strategy_pnl_1min_prod_v2", "prod"),
    ("strategy_pnl_1day_bt_v2", "strategy_pnl_1min_bt_v2", "bt"),
    (
        "strategy_pnl_1day_real_trade_v2",
        "strategy_pnl_1min_real_trade_v2",
        "real_trade",
    ),
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


@dataclass(frozen=True)
class PositionMismatch:
    """One target row whose position disagrees with the source's active bar."""

    ts: datetime
    expected: float
    actual: float


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
    expected_minutes: int,
    now_ts: datetime,
) -> Violation | None:
    """Detect start_gap / stale_end / internal_holes for one (U, S).

    Checks in priority order; returns the first detected violation, or None.

    ``expected_minutes`` is the count of 1-min price timestamps in
    ``[expected_min, expected_max]`` — computed server-side with a bounded
    ``count()`` query (``_count_prices``) so we never materialize the
    underlying's full price-timestamp set in Python.
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
            detail=(
                f"expected={expected_min:%Y-%m-%d %H:%M:%S} "
                f"actual={stat.actual_min_ts:%Y-%m-%d %H:%M:%S} ({gap_minutes}m)"
            ),
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
            detail=(
                f"expected≈{expected_max:%Y-%m-%d %H:%M:%S} "
                f"actual={stat.actual_max_ts:%Y-%m-%d %H:%M:%S} "
                f"({stale_minutes}m stale)"
            ),
            severity_minutes=stale_minutes,
        )

    # 3. internal_holes
    if stat.actual_rows < expected_minutes:
        missing = expected_minutes - stat.actual_rows
        return Violation(
            table=table,
            underlying=underlying,
            stn=stn,
            category="internal_holes",
            detail=(
                f"expected_minutes={expected_minutes} actual_rows={stat.actual_rows} "
                f"missing={missing}"
            ),
            severity_minutes=missing,
        )

    return None


def _check_phase2(
    underlying: str,
    stn: str,
    q_gap_rows: list[tuple[datetime, int]],
    present_counts: list[int],
    has_prices: bool,
    top_n: int = TOP_N_OFFENDERS,
) -> list[GapDescriptor]:
    """For one flagged strategy, derive gap descriptors trimmed by price-gap exemption.

    Each q_gap row is (gap_end_ts, gap_secs) where gap_secs > 60. The actual
    gap interval is [gap_end - gap_secs, gap_end) on minute boundaries. We count
    missing minutes inside the interval and subtract minutes that have no price
    (since the recompute legitimately skips those).

    ``present_counts[i]`` is the number of price timestamps strictly inside the
    i-th gap's open interval ``(gap_start, gap_end)`` — computed server-side per
    gap with a bounded ``count()`` (``_count_prices_strict``) rather than by
    scanning a materialized price set. ``has_prices`` is False only when the
    underlying has no price rows at all, in which case nothing can be exempted.
    """
    descriptors: list[GapDescriptor] = []
    for (gap_end, gap_secs), present_strictly in zip(q_gap_rows, present_counts):
        raw_missing = gap_secs // 60
        if raw_missing <= 0:
            continue
        if not has_prices:
            # No price data available — cannot exempt any minutes; all raw
            # gap minutes are real holes.
            effective_holes = raw_missing
        else:
            # Price-present minutes strictly inside (gap_start, gap_end),
            # exclusive of both endpoints. If none are present the gap is fully
            # explained by a price outage and we drop it. Otherwise
            #   effective = raw_missing - no_price_strictly_inside
            #             = 1 + present_strictly_inside
            # (+1 accounts for the gap-start boundary being a price-covered
            # minute in a gap where at least one interior minute also has a price).
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
            detail=(
                f"length mismatch: source={len(source_changes)} "
                f"target={len(target_changes)}"
            ),
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
                detail=(
                    f"ts mismatch at #{i}: source={src.effective_ts:%Y-%m-%d %H:%M:%S} "
                    f"target={tgt.effective_ts:%Y-%m-%d %H:%M:%S}"
                ),
                severity_minutes=1,
            )
        if src.position != tgt.position:
            return Violation(
                table=table,
                underlying=underlying,
                stn=stn,
                category="position_mismatch",
                detail=(
                    f"position mismatch at #{i}: source={src.position} "
                    f"target={tgt.position} (ts={src.effective_ts:%Y-%m-%d %H:%M:%S})"
                ),
                severity_minutes=1,
            )
    return None


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
    bucket_label = (
        "hour"
        if bucket == timedelta(hours=1)
        else (
            "day"
            if bucket == timedelta(days=1)
            else f"{int(bucket.total_seconds() // 60)}m"
        )
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
                    f"bucket_position={bucket_position} != "
                    f"latest_min_position={latest.position} "
                    f"(latest min change at {latest.effective_ts:%Y-%m-%d %H:%M:%S})"
                ),
                severity_minutes=1,
            )
    return None


def _check_position_per_minute(
    source_changes: list[PositionChange],
    target_rows: list[tuple[datetime, float]],
) -> tuple[int, int, list[PositionMismatch]]:
    """Per-minute position correctness check.

    Walks target_rows in ts order, maintaining a pointer into source_changes
    so source_changes[ptr] is the latest change with effective_ts <= ts.
    Returns (mismatch_count, orphan_count, sample_mismatches).

    - mismatch_count: target rows whose position != active source position
    - orphan_count: target rows BEFORE any source change (cannot verify)
    - sample_mismatches: first TOP_N_OFFENDERS mismatches for the report
    """
    if not source_changes or not target_rows:
        return 0, 0, []

    mismatch_count = 0
    orphan_count = 0
    samples: list[PositionMismatch] = []
    ptr = -1  # index of the active source change; -1 means before any change

    for ts, actual_pos in target_rows:
        # Advance ptr while the next source change is <= ts.
        while (
            ptr + 1 < len(source_changes) and source_changes[ptr + 1].effective_ts <= ts
        ):
            ptr += 1
        if ptr < 0:
            orphan_count += 1
            continue
        expected_pos = source_changes[ptr].position
        if expected_pos != actual_pos:
            mismatch_count += 1
            if len(samples) < TOP_N_OFFENDERS:
                samples.append(
                    PositionMismatch(ts=ts, expected=expected_pos, actual=actual_pos)
                )

    return mismatch_count, orphan_count, samples


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
            closing_ts = datetime.strptime(
                ts_str[:19], "%Y-%m-%d %H:%M:%S"
            ) + timedelta(minutes=tf_minutes)
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
            changes.append(
                PositionChange(effective_ts=entry.execution_ts, position=pos)
            )
            prev_position = pos
    return changes


def _format_report(report: AuditReport) -> str:
    """Render a human-readable summary of violations grouped by table.

    Within each table, top N offenders by severity_minutes appear first.
    """
    if not report.violations:
        return (
            f"PnL COVERAGE & POSITION AUDIT CLEAN — {report.tables_checked} tables, "
            f"{report.strategies_checked} strategies, {report.duration_secs:.1f}s"
        )

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
        worst = sorted(violations, key=lambda v: v.severity_minutes, reverse=True)[
            :TOP_N_OFFENDERS
        ]
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
  AND ts >= toDateTime('{GLOBAL_START_TS}')
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


def _has_prices(underlying: str, client) -> bool:
    """Whether the underlying has any 1-min price rows since GLOBAL_START_TS.

    Replaces the old ``not price_set`` test. One cheap index-only existence
    probe per underlying — no timestamps are pulled into Python.
    """
    instrument = _underlying_to_instrument(underlying)
    n = query_scalar(
        f"""
SELECT count() FROM (
  SELECT 1 FROM analytics.futures_price_1min
  WHERE instrument = '{instrument}' AND ts >= toDateTime('{GLOBAL_START_TS}')
  LIMIT 1
)
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return bool(n)


def _count_prices(underlying: str, start: datetime, end: datetime, client) -> int:
    """Count 1-min price timestamps in the inclusive window [start, end] for one U.

    Server-side ``count()`` over a ts range on the (instrument, ts) sort key —
    bounded ClickHouse memory, a single scalar back to Python. Feeds Phase 1's
    internal_holes expected-minute count without materializing the price set.
    """
    instrument = _underlying_to_instrument(underlying)
    n = query_scalar(
        f"""
SELECT count() FROM analytics.futures_price_1min
WHERE instrument = '{instrument}'
  AND ts >= toDateTime('{start:%Y-%m-%d %H:%M:%S}')
  AND ts <= toDateTime('{end:%Y-%m-%d %H:%M:%S}')
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return int(n or 0)


def _count_prices_strict(
    underlying: str, start: datetime, end: datetime, client
) -> int:
    """Count 1-min price timestamps strictly inside the open interval (start, end).

    Used by Phase 2's per-gap exemption; both endpoints excluded.
    """
    instrument = _underlying_to_instrument(underlying)
    n = query_scalar(
        f"""
SELECT count() FROM analytics.futures_price_1min
WHERE instrument = '{instrument}'
  AND ts > toDateTime('{start:%Y-%m-%d %H:%M:%S}')
  AND ts < toDateTime('{end:%Y-%m-%d %H:%M:%S}')
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return int(n or 0)


def _src_time_windows(end_dt: datetime | None = None) -> list[tuple[str, str]]:
    """[start, end) string windows of _SRC_CHUNK_DAYS spanning the audited history."""
    start = datetime.strptime(GLOBAL_START_TS[:19], "%Y-%m-%d %H:%M:%S")
    end = (end_dt or datetime.now(tz=UTC).replace(tzinfo=None)) + timedelta(days=1)
    windows: list[tuple[str, str]] = []
    cur = start
    step = timedelta(days=_SRC_CHUNK_DAYS)
    while cur < end:
        nxt = min(cur + step, end)
        windows.append(
            (cur.strftime("%Y-%m-%d %H:%M:%S"), nxt.strftime("%Y-%m-%d %H:%M:%S"))
        )
        cur = nxt
    return windows


def _load_source_prod_bt(
    source_table: str, underlying: str, stn: str, client, tf_values: list[int]
) -> tuple[SourceFirstBar | None, list[PositionChange]]:
    """Stream ONE prod/bt strategy's first-revision bars; reduce to changes inline.

    The bar stream is consumed block-by-block via ``query_rows_stream`` and
    collapsed on the fly into (a) the first two bars (for timeframe inference)
    and (b) position-transition bars only. Nothing else is retained, so peak
    Python memory is O(transitions) regardless of how many bars the strategy
    has — a 1-min strategy with hundreds of thousands of bars still costs only
    its handful of flips. ClickHouse side stays bounded too: ``LIMIT 1 BY ts``
    over the (strategy_table_name, config_timeframe, ts, revision_ts) sort key
    picks the first revision per bar without buffering row_json.

    Returns ``(None, [])`` when the strategy has no source rows (orphan upstream).
    manual_probe% strategies are excluded.
    """
    sql = f"""
SELECT ts_str, position, tf
FROM (
  SELECT
    toString(ts)                            AS ts_str,
    ts                                      AS ts_raw,
    JSONExtractFloat(row_json, 'position')  AS position,
    config_timeframe                        AS tf
  FROM analytics.{source_table}
  WHERE underlying = '{underlying}'
    AND strategy_table_name = '{stn}'
    AND strategy_table_name NOT LIKE 'manual_probe%'
    AND ts >= toDateTime('{GLOBAL_START_TS}')
  ORDER BY ts, revision_ts
  LIMIT 1 BY ts
)
ORDER BY ts_raw
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
"""
    first_two: list[tuple[str, float]] = []
    transition_bars: list[tuple[str, float]] = []
    tf_raw: str | None = None
    prev_position: float | None = None
    for r in query_rows_stream(sql, client=client):
        ts_str = str(r[0])
        position = float(r[1])
        if tf_raw is None:
            tf_raw = str(r[2])
        if len(first_two) < 2:
            first_two.append((ts_str, position))
        if prev_position is None or position != prev_position:
            transition_bars.append((ts_str, position))
            prev_position = position
    source = _derive_source_first_bar(first_two, tf_raw, tf_values)
    if source is None:
        return None, []
    # transition_bars is already de-duplicated; _compute_source_changes_prod_bt
    # re-applies the dedup (idempotent here) and attaches closing_ts = ts + tf.
    return source, _compute_source_changes_prod_bt(transition_bars, source.tf_minutes)


def _derive_source_first_bar(
    bars: list[tuple[str, float]], tf_raw: str | None, tf_values: list[int]
) -> SourceFirstBar | None:
    """Build SourceFirstBar (prod/bt) from a strategy's first-revision bars.

    config_timeframe maps to minutes; when absent (fallback 5) the timeframe is
    inferred by snapping the first two bars' spacing to the nearest known value.
    Returns None when the strategy has no source bars (orphan).
    """
    if not bars:
        return None
    tf = TIMEFRAME_MAP.get(str(tf_raw), 5)
    if tf == 5 and len(bars) >= 2:
        ts0 = datetime.strptime(str(bars[0][0])[:19], "%Y-%m-%d %H:%M:%S")
        ts1 = datetime.strptime(str(bars[1][0])[:19], "%Y-%m-%d %H:%M:%S")
        spacing_min = int((ts1 - ts0).total_seconds() // 60)
        if spacing_min > 0:
            tf = min(tf_values, key=lambda t: abs(t - spacing_min))
    first_bar_ts = datetime.strptime(str(bars[0][0])[:19], "%Y-%m-%d %H:%M:%S")
    return SourceFirstBar(
        expected_min_ts=first_bar_ts + timedelta(minutes=tf), tf_minutes=tf
    )


def _load_source_rt(
    underlying: str, stn: str, client
) -> tuple[SourceFirstBar | None, list[PositionChange]]:
    """Stream ONE real_trade strategy's revisions; return (first-bar, changes).

    real_trade needs every revision of the strategy (build_rt_lookup applies the
    ``(bar_ts, revision_ts) > prev_accepted`` acceptance rule across them), so we
    hold one strategy's revisions at a time — bounded, and far smaller than the
    whole underlying's revision history the previous per-U fetch buffered. The
    scan is time-chunked (``_src_time_windows``) to keep each row_json read under
    the cap; build_rt_lookup re-sorts per strategy so chunk order is irrelevant.

    Returns ``(None, [])`` when the strategy has no accepted revisions (orphan).
    """
    revs: list[dict] = []
    for ws, we in _src_time_windows():
        for r in query_rows_stream(
            f"""
SELECT
  toString(ts),
  toString(revision_ts),
  toString(toStartOfMinute(revision_ts + INTERVAL 59 SECOND)) AS execution_ts,
  JSONExtract(row_json, 'position', 'Float64') AS position,
  config_timeframe AS tf
FROM analytics.strategy_output_history_v2
WHERE underlying = '{underlying}'
  AND strategy_table_name = '{stn}'
  AND strategy_table_name NOT LIKE 'manual_probe%'
  AND ts >= toDateTime('{ws}') AND ts < toDateTime('{we}')
ORDER BY ts, revision_ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
            client=client,
        ):
            ts_s, rev_s, exec_s, position, tf = r
            tf_minutes = TIMEFRAME_MAP.get(str(tf), 5)
            ts_dt = datetime.strptime(str(ts_s)[:19], "%Y-%m-%d %H:%M:%S")
            closing_dt = ts_dt + timedelta(minutes=tf_minutes)
            revs.append(
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
    if not revs:
        return None, []
    lookup = build_rt_lookup(revs)
    entries = lookup.get(stn, [])
    if not entries:
        return None, []
    tf = TIMEFRAME_MAP.get(entries[0].rev["config_timeframe"], 5)
    source = SourceFirstBar(expected_min_ts=entries[0].execution_ts, tf_minutes=tf)
    return source, _compute_source_changes_rt(revs, stn)


def _fetch_q_trans(
    target_table: str, underlying: str, stn: str, client
) -> list[PositionChange]:
    """Q_trans: per-strategy position transitions in target_table.

    Uses lagInFrame over a single-partition window to keep memory bounded.
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
    AND ts >= toDateTime('{GLOBAL_START_TS}')
)
WHERE position != prev_pos OR rn = 1
ORDER BY ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return [PositionChange(effective_ts=r[0], position=float(r[1])) for r in rows]


def _fetch_q_gap(
    target_table: str, underlying: str, stn: str, client
) -> list[tuple[datetime, int]]:
    """Q_gap: per-strategy ts gaps > 60s in target_table.

    Returns (gap_end, gap_secs).
    """
    rows = query_rows(
        f"""
SELECT gap_end, gap_secs FROM (
  SELECT ts AS gap_end,
         toUnixTimestamp(ts) AS ts_secs,
         lagInFrame(toUnixTimestamp(ts)) OVER (ORDER BY ts) AS prev_ts_secs,
         (
           toUnixTimestamp(ts)
           - lagInFrame(toUnixTimestamp(ts)) OVER (ORDER BY ts)
         ) AS gap_secs
  FROM analytics.{target_table}
  WHERE underlying = '{underlying}'
    AND strategy_table_name = '{stn}'
    AND ts >= toDateTime('{GLOBAL_START_TS}')
)
WHERE prev_ts_secs > 0 AND gap_secs > 60
ORDER BY gap_secs DESC
LIMIT 50
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    )
    return [(r[0], int(r[1])) for r in rows]


def _iter_q_target_full(target_table: str, underlying: str, stn: str, client):
    """Stream a strategy's full (ts, position) series in ascending-ts order.

    Yields rows one at a time via ``query_rows_stream`` so the whole series is
    never resident — critical for bt, whose backtest target can span years (far
    more than the ~100k-row assumption the old materializing fetch was sized
    for). ``ORDER BY ts`` rides the (strategy_table_name, ts) sort key, so
    ClickHouse reads in-order and streams without a full server-side sort.
    Consumed by ``_check_position_per_minute``, which only keeps a pointer plus
    a few sample mismatches — O(1) Python memory.
    """
    for r in query_rows_stream(
        f"""
SELECT ts, position
FROM analytics.{target_table}
WHERE underlying = '{underlying}'
  AND strategy_table_name = '{stn}'
  AND ts >= toDateTime('{GLOBAL_START_TS}')
ORDER BY ts
SETTINGS max_memory_usage = {QUERY_MEMORY_CAP}
""",
        client=client,
    ):
        yield (r[0], float(r[1]))


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

    # ── Enumerate strategies (small per-U aggregate) ─────────────────────────
    # _fetch_q_stat is a GROUP BY whose result is one tiny row per strategy, so
    # it stays cheap even on bt's large target. Everything else below is fetched
    # per strategy with bounded/streaming queries.
    stat_by_stn = _fetch_q_stat(target_table, underlying, client)
    if not stat_by_stn:
        return report  # target has nothing for this U — nothing to verify
    has_prices = _has_prices(underlying, client)
    _TF_VALUES = sorted(TIMEFRAME_MAP.values())  # for prod/bt timeframe inference

    # ── Per-strategy loop ────────────────────────────────────────────────────
    for stn, stat in stat_by_stn.items():
        report.strategies_checked += 1
        if mode == "real_trade":
            source, source_changes = _load_source_rt(underlying, stn, client)
        else:
            source, source_changes = _load_source_prod_bt(
                source_table, underlying, stn, client, _TF_VALUES
            )
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

        # Phase 1 — internal_holes needs the expected-minute count, fetched as a
        # bounded server-side count() over the price window for this strategy.
        expected_max = now_ts - timedelta(minutes=STALE_END_TOLERANCE_MIN // 2)
        expected_minutes = _count_prices(
            underlying, source.expected_min_ts, expected_max, client
        )
        v1 = _check_phase1(
            target_table, underlying, stn, stat, source, expected_minutes, now_ts
        )
        if v1 is not None:
            report.violations.append(v1)
            # Phase 2 drilldown for any flagged strategy (rare path). Count
            # price-present minutes per gap server-side instead of scanning a
            # materialized price set.
            q_gap = _fetch_q_gap(target_table, underlying, stn, client)
            present_counts = [
                _count_prices_strict(
                    underlying, gap_end - timedelta(seconds=gap_secs), gap_end, client
                )
                for gap_end, gap_secs in q_gap
            ]
            gaps = _check_phase2(underlying, stn, q_gap, present_counts, has_prices)
            for g in gaps:
                report.violations.append(
                    Violation(
                        table=target_table,
                        underlying=underlying,
                        stn=stn,
                        category="internal_holes",
                        detail=(
                            f"gap ending {g.gap_end:%Y-%m-%d %H:%M:%S} "
                            f"({g.gap_minutes}m)"
                        ),
                        severity_minutes=g.gap_minutes,
                    )
                )
            # Continue to Phase 3 — position correctness is checked regardless
            # of coverage state.

        # Phase 3: per-minute position correctness (always runs). The target
        # series is streamed row-by-row; _check_position_per_minute keeps only a
        # pointer and a few samples, so memory is independent of series length.
        mismatch_count, orphan_count, samples = _check_position_per_minute(
            source_changes, _iter_q_target_full(target_table, underlying, stn, client)
        )
        if mismatch_count > 0:
            sample_str = ", ".join(
                f"{m.ts:%Y-%m-%d %H:%M:%S} exp={m.expected} got={m.actual}"
                for m in samples
            )
            report.violations.append(
                Violation(
                    table=target_table,
                    underlying=underlying,
                    stn=stn,
                    category="position_mismatch",
                    detail=(
                        f"{mismatch_count} rows wrong (of {stat.actual_rows}); "
                        f"samples: {sample_str}"
                    ),
                    severity_minutes=mismatch_count,
                )
            )

    return report


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


@asset(
    name="pnl_coverage_audit",
    group_name="strategy_pnl",
    compute_kind="clickhouse",
    description=(
        "Daily read-only audit of PnL tables: per-(U, S) coverage + "
        "position-boundary checks."
    ),
    op_tags={"dagster/timeout": 3600},
)
def pnl_coverage_audit_asset(context: AssetExecutionContext) -> MaterializeResult:
    """Audit all nine PnL tables; raise RuntimeError on any violation."""
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
            f"[audit] {target}: {sub.strategies_checked} strategies, "
            f"{len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1

    for hour, min_t, _mode in _HOUR_TABLES:
        context.log.info(f"[audit] starting hour table {hour}")
        sub = _audit_bucketed_table(
            bucket_table=hour,
            min_table=min_t,
            bucket=timedelta(hours=1),
            client=get_client(),
        )
        context.log.info(
            f"[audit] {hour}: {sub.strategies_checked} strategies, "
            f"{len(sub.violations)} violations"
        )
        full_report.violations.extend(sub.violations)
        full_report.strategies_checked += sub.strategies_checked
        full_report.tables_checked += 1

    for day, min_t, _mode in _DAY_TABLES:
        context.log.info(f"[audit] starting day table {day}")
        sub = _audit_bucketed_table(
            bucket_table=day,
            min_table=min_t,
            bucket=timedelta(days=1),
            client=get_client(),
        )
        context.log.info(
            f"[audit] {day}: {sub.strategies_checked} strategies, "
            f"{len(sub.violations)} violations"
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
