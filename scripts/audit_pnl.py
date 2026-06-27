"""Consolidated PnL audit & repair for prod / bt / real_trade.

Audits the three pnl_1min / pnl_1hour table families for missing strategies,
coverage gaps, position errors, and 1h↔1min drift; optionally repairs them.

Prod / real_trade: fix forward from each strategy's failure_ts (cumulative_pnl
chains forward, so any earlier error poisons the tail). BT: fix window-local
(cumulative_pnl comes from row_json, not from a chained anchor).

Persists one JSONL record per run to ./audit_reports/history.jsonl so a later
run can summarize prior state via --show-history.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import uuid
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

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
    fetch_bt_anchors,
    fetch_bt_benchmarks,
    fetch_new_bars_prod,
    fetch_new_bars_real_trade,
    fetch_prices_multi,
    first_active_minute,
    last_active_minute,
)
from libs.computation.fetch_bars import _TF_EXPR  # noqa: E402

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
DAY_TABLE = {
    "prod": "strategy_pnl_1day_prod_v2",
    "bt": "strategy_pnl_1day_bt_v2",
    "real_trade": "strategy_pnl_1day_real_trade_v2",
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

# Source-bar fetch lookback for fix paths. Must cover at least 2 × max(timeframe)
# so the previous active bar for any tf is included. The largest tf is `1d` (1440min);
# worst case a 1d bar at `ts = failure_ts - 24h - 1min` is needed to cover minute
# `failure_ts` (its execution_ts = ts + 1440 = failure_ts - 1min, still active).
# A 1440-min lookback misses that bar. 2880 (48h) covers it with margin.
# Invariant guarded by tests/test_audit_pnl.py::TestBarFetchLookback.
_BAR_FETCH_LOOKBACK_MINUTES = 2880

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
    failure_ts: datetime | None = None  # earliest impacted minute
    window_end: datetime | None = None  # bt-only: explicit upper bound


@dataclass
class StrategyFix:
    """One resolved repair action for one strategy."""

    type: Mode
    underlying: str
    strategy_table_name: str
    failure_ts: datetime
    window_end: datetime | None = None  # bt-only; None means "to now"
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
    finished_at: datetime | None = None
    mode: str = "audit"  # "audit" | "fix" | "fix-window" | "dry-run"
    scope_types: list[Mode] = field(default_factory=list)
    scope_underlying: str | None = None
    scope_fix_window: tuple[datetime, datetime] | None = None
    violations: list[Violation] = field(default_factory=list)
    fixes: list[StrategyFix] = field(default_factory=list)
    by_type: dict[Mode, TypeSummary] = field(default_factory=dict)
    report_path: Path | None = None
    run_id: str = ""

    def has_violations(self) -> bool:
        return bool(self.violations)


# ── section A: ECS + CH client helpers ──────────────────────────────────────


# Per-query memory controls applied to EVERY client (incl. fix_bt_strategies
# worker threads, which resolve this module-level get_client). ClickHouse Cloud
# leaves max_memory_usage = 0 (unlimited per query); a window recompute over the
# bt tables (billions of rows) can then try to grab the whole server's memory and
# trip `(total) memory limit exceeded`, OOM-crashing Cloud. Capping per-query
# memory and lowering the external-aggregation/sort thresholds forces GROUP BY /
# ORDER BY to spill to disk instead of holding everything in RAM.
_DEFAULT_QUERY_SETTINGS = {
    "max_memory_usage": 4_000_000_000,  # 4 GiB hard cap per query
    "max_bytes_before_external_group_by": 2_000_000_000,  # spill GROUP BY at 2 GiB
    "max_bytes_before_external_sort": 2_000_000_000,  # spill ORDER BY at 2 GiB
}


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
        connect_timeout=15,
        send_receive_timeout=1800,
        settings=_DEFAULT_QUERY_SETTINGS,
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
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


def _utcnow_naive() -> datetime:
    """Naive UTC — caller must wrap with _parse_ts_utc before inserting into
    ClickHouse DateTime columns."""
    return datetime.now(tz=UTC).replace(tzinfo=None)


_UNDERLYING_PATTERN = re.compile(r"^[A-Z0-9]+$")


def _validate_underlying(underlying: str | None) -> str | None:
    """Whitelist-validate underlying filter; raise ValueError on garbage."""
    if underlying is None:
        return None
    u = underlying.upper()
    if not _UNDERLYING_PATTERN.fullmatch(u):
        raise ValueError(f"invalid underlying filter: {underlying!r}")
    return u


def _q(value: str) -> str:
    """Escape a SQL string literal by doubling single quotes."""
    return value.replace("'", "''")


# ── section B: detection (Task 2) ───────────────────────────────────────────


def find_missing_or_start_gap(
    type_: Mode, client, underlying_filter: str | None = None
) -> list[Violation]:
    """Strategies absent from target, or whose target starts >2h after source."""
    underlying_filter = _validate_underlying(underlying_filter)
    src = SOURCE_TABLE[type_]
    tgt = TARGET_TABLE[type_]
    where_u = (
        f" AND src.underlying = '{underlying_filter}'" if underlying_filter else ""
    )
    if type_ == "real_trade":
        first_exec_expr = "min(toStartOfMinute(revision_ts + INTERVAL 59 SECOND))"
    else:
        first_exec_expr = f"min(toStartOfMinute(ts + INTERVAL ({_TF_EXPR}) MINUTE))"
    sql = f"""
    WITH
    src AS (
        SELECT
            strategy_table_name,
            any(underlying)                                    AS underlying,
            {first_exec_expr} AS first_exec_ts
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
    SETTINGS join_use_nulls = 1
    """  # join_use_nulls=1 is critical: without it ClickHouse LEFT JOIN returns
    #     DateTime '1970-01-01' / empty-string defaults for unmatched right-side
    #     columns, not NULL — so `tgt.strategy_table_name IS NULL` never fires
    #     and strategies completely missing from target slip through undetected.
    out: list[Violation] = []
    for stn, und, first_exec, first_tgt in client.query(sql).result_rows:
        if first_tgt is None:
            cat: Category = "missing_strategy"
            detail = f"absent from {tgt}; source starts {first_exec}"
        else:
            cat = "start_gap"
            gap_min = int((first_tgt - first_exec).total_seconds() // 60)
            detail = (
                f"target starts {first_tgt}, source starts {first_exec} "
                f"({gap_min}m gap)"
            )
        out.append(
            Violation(
                type=type_,
                underlying=str(und),
                strategy_table_name=str(stn),
                category=cat,
                detail=detail,
                failure_ts=first_exec,
            )
        )
    return out


def find_midgap(
    type_: Mode, client, underlying_filter: str | None = None
) -> list[Violation]:
    """Strategies whose covered-days count is less than (max-min+1) — at least one
    full day in the target window has zero rows. failure_ts is the first missing
    minute in the earliest hole; window_end is the last missing minute in that
    hole (bt uses both; prod/rt uses only failure_ts)."""
    underlying_filter = _validate_underlying(underlying_filter)
    tgt = TARGET_TABLE[type_]
    where_u = f" AND underlying = '{underlying_filter}'" if underlying_filter else ""

    # Phase 1: which strategies have any mid-history gap.
    # Alias `any(underlying)` as `und` (not `underlying`) to avoid
    # ClickHouse resolving the WHERE-clause `underlying = '...'` against
    # the aliased aggregate, which raises ILLEGAL_AGGREGATION.
    flag_sql = f"""
    SELECT strategy_table_name, any(underlying) AS und
    FROM analytics.{tgt}
    WHERE ts >= toDateTime('{GLOBAL_START_TS}'){where_u}
    GROUP BY strategy_table_name
    HAVING count(DISTINCT toStartOfDay(ts)) < dateDiff('day', min(ts), max(ts)) + 1
    ORDER BY und, strategy_table_name
    """
    flagged = [(str(s), str(u)) for s, u in client.query(flag_sql).result_rows]

    # Phase 2: for each flagged strategy, find the first/last minute of
    # the earliest hole.
    out: list[Violation] = []
    for stn, und in flagged:
        # prev_ts > toDateTime(GLOBAL_START_TS) guards against the first row's
        # lagInFrame() returning the DateTime default 1970-01-01 00:00:00 —
        # without this filter, every flagged strategy would report a spurious
        # "gap" from 1970 to its first real row.
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
            WHERE strategy_table_name = '{_q(stn)}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
        )
        WHERE gap_minutes > 1
          AND prev_ts >= toDateTime('{GLOBAL_START_TS}')
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
                type=type_,
                underlying=und,
                strategy_table_name=stn,
                category="mid_gap",
                detail=f"{gap_min}m missing from {gap_start}",
                failure_ts=gap_start,
                window_end=gap_end,
            )
        )
    return out


def find_stale_end(
    type_: Mode, client, now_ts: datetime, underlying_filter: str | None = None
) -> list[Violation]:
    """Strategies whose target's max(ts) is older than now - 10m. Skipped for bt."""
    underlying_filter = _validate_underlying(underlying_filter)
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
                type=type_,
                underlying=str(und),
                strategy_table_name=str(stn),
                category="stale_end",
                detail=f"max(ts)={max_ts} ({stale_min}m stale)",
                failure_ts=max_ts + timedelta(minutes=1),
            )
        )
    return out


def audit_positions(
    type_: Mode, client, underlying_filter: str | None = None
) -> list[Violation]:
    """Per-strategy position correctness — first mismatch ts becomes failure_ts.

    Streams the target table in ts order, comparing each row's position to the
    active source-derived position. Bounded by POSITION_SAMPLE_CAP samples per
    strategy.
    """
    underlying_filter = _validate_underlying(underlying_filter)
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
            WHERE underlying = '{_q(und)}' AND strategy_table_name = '{_q(stn)}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
            ORDER BY ts, revision_ts
            """
        else:
            src_sql = f"""
            SELECT * FROM (
                SELECT
                    toStartOfMinute(ts + INTERVAL ({_TF_EXPR}) MINUTE) AS effective_ts,
                    JSONExtractFloat(row_json, 'position') AS position
                FROM analytics.{src}
                WHERE underlying = '{_q(und)}' AND strategy_table_name = '{_q(stn)}'
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
        WHERE underlying = '{_q(und)}' AND strategy_table_name = '{_q(stn)}'
          AND ts >= toDateTime('{GLOBAL_START_TS}')
        ORDER BY ts
        """
        ptr = -1
        first_mismatch: datetime | None = None
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
                    type=type_,
                    underlying=und,
                    strategy_table_name=stn,
                    category="position_mismatch",
                    detail=f"{sample_count}+ mismatches starting {first_mismatch}",
                    failure_ts=first_mismatch,
                )
            )
    return out


def audit_hour_sync(
    type_: Mode, client, underlying_filter: str | None = None
) -> list[Violation]:
    """1-hour table's position must equal the 1-min table's latest position with
    effective_ts <= hour + 1h. First mismatched hour slot's ts becomes failure_ts.
    """
    underlying_filter = _validate_underlying(underlying_filter)
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
            WHERE underlying = '{_q(und)}' AND strategy_table_name = '{_q(stn)}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
        ),
        expected AS (
            SELECT
                toStartOfHour(ts)    AS hour_ts,
                argMax(position, ts) AS expected_pos
            FROM analytics.{minute}
            WHERE underlying = '{_q(und)}' AND strategy_table_name = '{_q(stn)}'
              AND ts >= toDateTime('{GLOBAL_START_TS}')
            GROUP BY toStartOfHour(ts)
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
                type=type_,
                underlying=und,
                strategy_table_name=stn,
                category="hour_sync",
                detail=(
                    f"hour {hour_ts}: hour={hour_pos} != "
                    f"minute-argMax={expected_pos}"
                ),
                failure_ts=hour_ts,
            )
        )
    return out


# ── section C: failure_ts aggregator (Task 3) ───────────────────────────────


def resolve_strategy_fix(violations: list[Violation]) -> StrategyFix:
    """Collapse all violations for ONE strategy into a single StrategyFix.

    Caller guarantees all violations share (type, underlying, strategy_table_name).
    """
    if not violations:
        raise ValueError("resolve_strategy_fix requires at least one violation")
    first = violations[0]
    expected_key = (first.type, first.underlying, first.strategy_table_name)
    for v in violations:
        if (v.type, v.underlying, v.strategy_table_name) != expected_key:
            raise ValueError(
                f"resolve_strategy_fix received mixed-strategy violations: "
                f"expected {expected_key}, got ({v.type}, {v.underlying}, "
                f"{v.strategy_table_name})"
            )
    failure_ts = min(
        (v.failure_ts for v in violations if v.failure_ts is not None), default=None
    )
    if failure_ts is None:
        raise ValueError(
            f"no violation for {first.type}/{first.underlying}/"
            f"{first.strategy_table_name} has a failure_ts"
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


# ── section D: fix functions (Tasks 4–5) ────────────────────────────────────


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
    WHERE strategy_table_name = '{_q(strategy_table_name)}'
      AND ts < toDateTime('{failure_ts:%Y-%m-%d %H:%M:%S}')
      AND ts >= toDateTime('{failure_ts:%Y-%m-%d %H:%M:%S}') - INTERVAL 7 DAY
    """
    rows = client.query(sql).result_rows
    if not rows or rows[0][0] is None:
        return {"pnl": 0.0, "price": 0.0, "position": 0.0}
    pnl, price, position = rows[0]
    return {
        "pnl": float(pnl or 0.0),
        "price": float(price or 0.0),
        "position": float(position or 0.0),
    }


def _prepare_rows_for_insert(rows: list[list]) -> list[list]:
    """ts at INSERT_COLUMNS index 7, updated_at at index 14 must be
    tz-aware datetimes."""
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
        log.info(
            "[%s/%s] repair window [%s, %s) for %d strategies",
            type_,
            underlying,
            ts_start,
            ts_end,
            len(group),
        )

        # Extend bar-fetch start back so any bar whose coverage extends INTO the
        # window — but whose `ts` falls just before window_start — is included.
        # Without enough lookback, the active_*_at lookup has no bar covering the
        # boundary minute (e.g., a 1d bar at ts=failure_ts-25h would be missed
        # by a 24h lookback, leaving 1d-tf strategies with no rows for ~14h).
        # See _BAR_FETCH_LOOKBACK_MINUTES at top of file.
        bar_fetch_start = (
            earliest - timedelta(minutes=_BAR_FETCH_LOOKBACK_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        if type_ == "real_trade":
            bars = fetch_new_bars_real_trade(
                src, underlying, bar_fetch_start, ts_end, client
            )
        else:
            bars = fetch_new_bars_prod(src, underlying, bar_fetch_start, ts_end, client)
        stn_set = {f.strategy_table_name for f in group}
        bars = [b for b in bars if b["strategy_table_name"] in stn_set]
        if not bars:
            log.warning(
                "[%s/%s] no source bars in window — skipping", type_, underlying
            )
            continue

        prices_map = fetch_prices_multi(
            [underlying], ts_start, ts_end, client, extend_minutes=1440
        )
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
                AnchorRecord(
                    pnl=seed["pnl"], price=seed["price"], position=seed["position"]
                ),
            )

        if not dry_run:
            # Per strategy: delete forward from its own failure_ts, capped at its
            # window_end when one was supplied (--fix-window path).
            for f in group:
                upper_clause = (
                    f"AND ts <= toDateTime('{f.window_end:%Y-%m-%d %H:%M:%S}')"
                    if f.window_end is not None
                    else ""
                )
                client.command(
                    f"DELETE FROM analytics.{tgt} "
                    f"WHERE strategy_table_name = '{_q(f.strategy_table_name)}' "
                    f"  AND ts >= toDateTime('{f.failure_ts:%Y-%m-%d %H:%M:%S}') "
                    f"  {upper_clause}"
                )
            log.info(
                "[%s/%s] deleted forward for %d strategies",
                type_,
                underlying,
                len(group),
            )

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
                batch.append(
                    build_pnl_row(stn, bar, price, cpnl, label, ts_str, now_str)
                )
                rows_by_stn[stn] += 1
            if len(batch) >= BATCH_SIZE:
                if not dry_run:
                    _prepare_rows_for_insert(batch)
                    client.insert(
                        f"analytics.{tgt}", batch, column_names=INSERT_COLUMNS
                    )
                batch = []
            minute_cur += timedelta(minutes=1)

        if batch:
            if not dry_run:
                _prepare_rows_for_insert(batch)
                client.insert(f"analytics.{tgt}", batch, column_names=INSERT_COLUMNS)

        for f in group:
            f.rows_written = rows_by_stn[f.strategy_table_name]
            f.fix_applied = not dry_run
            log.info(
                "[%s/%s] %s: %d rows",
                type_,
                underlying,
                f.strategy_table_name,
                f.rows_written,
            )


def fix_bt_strategies(
    fixes: list[StrategyFix], client, *, dry_run: bool, now_ts: datetime
) -> None:
    """Repair bt strategies window-locally. No anchor seeding (bt extracts
    cumulative_pnl from row_json). No consumer pause (no streaming consumer)."""
    if not fixes:
        return

    by_underlying: dict[str, list[StrategyFix]] = {}
    for f in fixes:
        by_underlying.setdefault(f.underlying, []).append(f)

    # Each underlying is processed independently — one bar fetch + one price
    # fetch shared across all strategies of that underlying, then a single batch
    # INSERT, then a per-underlying hour-table refresh as a checkpoint. If the
    # script is killed mid-run, completed underlyings have both minute and hour
    # tables consistent; only the in-flight underlying is partial (and its
    # remaining violations will surface on the next audit run).
    with ThreadPoolExecutor(max_workers=_BT_FIX_WORKERS) as pool:
        futures = {
            pool.submit(
                _fix_bt_underlying, underlying, group, dry_run=dry_run, now_ts=now_ts
            ): underlying
            for underlying, group in sorted(by_underlying.items())
        }
        for future in as_completed(futures):
            underlying = futures[future]
            try:
                future.result()
            except Exception:
                log.exception("[bt/%s] worker failed", underlying)
                raise


_BT_FIX_WORKERS = 4


def _fix_bt_underlying(
    underlying: str,
    fixes: list[StrategyFix],
    *,
    dry_run: bool,
    now_ts: datetime,
) -> None:
    """Repair all bt strategies of one underlying in a single batched pass.

    Runs in its own thread with its own ClickHouse client (clickhouse-connect
    clients aren't thread-safe). One bar fetch + one price fetch over the union
    window, per-strategy DELETE + compute_bt_pnl, a single batch INSERT, then
    a scoped hour-table refresh for THIS underlying as a checkpoint.
    """
    tgt = TARGET_TABLE["bt"]
    client = get_client()

    # Defensive clamp: any failure_ts earlier than GLOBAL_START_TS is treated as
    # a detection artifact (see the lagInFrame-vs-epoch bug fixed in
    # find_midgap). Clamping prevents an outlier from blowing the union window
    # back to 1970 and causing a multi-million-row over-recompute.
    _global_start = _parse_ts(GLOBAL_START_TS)
    for f in fixes:
        if f.failure_ts < _global_start:
            log.warning(
                "[bt/%s] %s: failure_ts %s < GLOBAL_START_TS, clamping",
                underlying,
                f.strategy_table_name,
                f.failure_ts,
            )
            f.failure_ts = _global_start

    # Union window across all strategies in this underlying.
    earliest = min(f.failure_ts for f in fixes)
    latest_end = max(
        (f.window_end if f.window_end is not None else now_ts) for f in fixes
    )
    ts_start = earliest.strftime("%Y-%m-%d %H:%M:%S")
    ts_end = (latest_end + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")

    # Extend the bar-fetch start back so any bar whose coverage extends INTO
    # the window — but whose `ts` falls just before window_start — is included.
    # Without enough lookback, `compute_bt_pnl` produces no output for boundary
    # minutes between window_start and the first fetched bar's execution_ts
    # (gaps + discontinuities in the recomputed range, especially for 1d-tf).
    # See _BAR_FETCH_LOOKBACK_MINUTES at top of file.
    bar_fetch_start = (
        earliest - timedelta(minutes=_BAR_FETCH_LOOKBACK_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")
    log.info(
        "[bt/%s] union window [%s, %s) (bar fetch from %s) for %d strategies",
        underlying,
        ts_start,
        ts_end,
        bar_fetch_start,
        len(fixes),
    )

    anchors_by_stn = fetch_bt_anchors(underlying, ts_start, ts_end, client)
    benchmarks = fetch_bt_benchmarks(underlying, ts_start, ts_end, client)

    # Extend price lower bound back 1 day to cover the straddling anchor's ts.
    price_lo = (
        datetime.strptime(ts_start[:19], "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
    ).strftime("%Y-%m-%d %H:%M:%S")
    prices_map = fetch_prices_multi(
        [underlying], price_lo, ts_end, client, extend_minutes=0
    )
    prices = prices_map.pop(underlying, {})

    all_rows: list[list] = []
    for f in fixes:
        window_start = f.failure_ts
        window_end = f.window_end if f.window_end is not None else now_ts
        ws_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
        we_str = (window_end + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        anchors = anchors_by_stn.get(f.strategy_table_name, [])
        if not anchors:
            log.warning(
                "[bt/%s] %s: no cum-pnl anchors in window — skipping",
                underlying,
                f.strategy_table_name,
            )
            continue

        # Warm-start seed: last target row strictly before the window (survives the
        # DELETE below, which only removes ts >= window_start). Absent / price==0 ⇒
        # compute_bt_pnl cold-starts from the first bar's cum_pnl_first.
        seed = fetch_seed_anchor("bt", f.strategy_table_name, window_start, client)
        seed_anchors: dict[str, tuple[float, float]] = {}
        if seed["price"] != 0.0:
            seed_anchors[f.strategy_table_name] = (seed["pnl"], seed["price"])

        if not dry_run:
            client.command(
                f"DELETE FROM analytics.{tgt} "
                f"WHERE strategy_table_name = '{_q(f.strategy_table_name)}' "
                f"  AND ts >= toDateTime('{ws_str}') "
                f"  AND ts <  toDateTime('{we_str}')"
            )

        rows = compute_bt_pnl(anchors, seed_anchors, prices, benchmarks, ws_str, we_str)
        f.rows_written = len(rows)
        f.fix_applied = not dry_run
        all_rows.extend(rows)
        log.info(
            "[bt/%s] %s: %d rows",
            underlying,
            f.strategy_table_name,
            f.rows_written,
        )

    if not dry_run and all_rows:
        _prepare_rows_for_insert(all_rows)
        for i in range(0, len(all_rows), BATCH_SIZE):
            client.insert(
                f"analytics.{tgt}",
                all_rows[i : i + BATCH_SIZE],
                column_names=INSERT_COLUMNS,
            )

    # Per-underlying hour-table refresh — the checkpoint that makes partial
    # runs leave consistent state.
    refresh_hour_table("bt", earliest, client, dry_run=dry_run, underlying=underlying)


def refresh_hour_table(
    type_: Mode,
    earliest_ts: datetime,
    client,
    *,
    dry_run: bool,
    underlying: str | None = None,
) -> None:
    """Rebuild the 1-hour table from the 1-min table for ts >= earliest_ts.

    SQL pattern lifted verbatim from the existing backfill scripts (proven).
    Optional ``underlying`` filter scopes both DELETE and INSERT so per-underlying
    checkpoint refreshes can run in parallel without racing on each other's rows.
    """
    hour = HOUR_TABLE[type_]
    minute = TARGET_TABLE[type_]
    ts_str = earliest_ts.strftime("%Y-%m-%d %H:%M:%S")
    u_clause = f" AND underlying = '{_q(underlying)}'" if underlying else ""
    scope_str = f"{type_}/{underlying}" if underlying else type_
    if dry_run:
        log.info("[hour-refresh:%s] DRY-RUN would rebuild from %s", scope_str, ts_str)
        return
    log.info("[hour-refresh:%s] rebuilding from %s", scope_str, ts_str)
    client.command(
        f"DELETE FROM analytics.{hour} "
        f"WHERE ts >= toStartOfHour(toDateTime('{ts_str}')){u_clause}"
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
    WHERE ts >= toStartOfHour(toDateTime('{ts_str}')){u_clause}
    ORDER BY strategy_table_name, strategy_id, strategy_name, underlying,
             config_timeframe, source, version, ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying,
               config_timeframe, source, version, ts
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfHour(minute_ts)
""")
    log.info("[hour-refresh:%s] done", scope_str)


def refresh_day_table(
    type_: Mode,
    earliest_ts: datetime,
    client,
    *,
    dry_run: bool,
    underlying: str | None = None,
) -> None:
    """Rebuild the 1-day table from the 1-hour table for ts >= earliest_ts.

    The 1-day tables are normally fed by an MV on 1-hour INSERTs, but a window
    recompute needs an explicit, correct full-day argMax rebuild: the MV produces
    per-INSERT-batch partial daily aggregates, so relying on the cascade after a
    refresh leaves the 1-day table with stale/partial rows. DELETE + a single
    full-window INSERT (same toStartOfDay + argMax shape as the schema backfill)
    gives the correct daily rollup. Mirrors refresh_hour_table.
    """
    day = DAY_TABLE[type_]
    hour = HOUR_TABLE[type_]
    ts_str = earliest_ts.strftime("%Y-%m-%d %H:%M:%S")
    u_clause = f" AND underlying = '{_q(underlying)}'" if underlying else ""
    scope_str = f"{type_}/{underlying}" if underlying else type_
    if dry_run:
        log.info("[day-refresh:%s] DRY-RUN would rebuild from %s", scope_str, ts_str)
        return
    log.info("[day-refresh:%s] rebuilding from %s", scope_str, ts_str)
    client.command(
        f"DELETE FROM analytics.{day} "
        f"WHERE ts >= toStartOfDay(toDateTime('{ts_str}')){u_clause}"
    )
    client.command(f"""
INSERT INTO analytics.{day}
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version,
    toStartOfDay(hour_ts)              AS ts,
    argMax(cumulative_pnl, hour_ts)    AS cumulative_pnl,
    argMax(benchmark, hour_ts)         AS benchmark,
    argMax(position, hour_ts)          AS position,
    argMax(price, hour_ts)             AS price,
    argMax(final_signal, hour_ts)      AS final_signal,
    argMax(weighting, hour_ts)         AS weighting,
    now()                              AS updated_at,
    any(strategy_instance_id)          AS strategy_instance_id
FROM (
    SELECT *, ts AS hour_ts
    FROM analytics.{hour}
    WHERE ts >= toStartOfDay(toDateTime('{ts_str}')){u_clause}
    ORDER BY strategy_table_name, strategy_id, strategy_name, underlying,
             config_timeframe, source, version, ts, updated_at DESC
    LIMIT 1 BY strategy_table_name, strategy_id, strategy_name, underlying,
               config_timeframe, source, version, ts
)
GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
         config_timeframe, source, version, toStartOfDay(hour_ts)
""")
    log.info("[day-refresh:%s] done", scope_str)


# ── section E: state file + history (Task 6) ────────────────────────────────


def _serialize_report(report: AuditReport) -> dict:
    """Convert AuditReport to a JSON-safe dict for one history record."""

    def _iso(dt: datetime | None) -> str | None:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None

    def _ts(dt: datetime | None) -> str | None:
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

    return {
        "run_id": report.run_id,
        "started_at": _iso(report.started_at),
        "finished_at": _iso(report.finished_at),
        "duration_secs": (
            round((report.finished_at - report.started_at).total_seconds(), 1)
            if report.finished_at and report.started_at
            else None
        ),
        "mode": report.mode,
        "scope": {
            "types": list(report.scope_types),
            "underlying": report.scope_underlying,
            "fix_window": (
                [_ts(report.scope_fix_window[0]), _ts(report.scope_fix_window[1])]
                if report.scope_fix_window
                else None
            ),
        },
        "totals": {
            "strategies_checked": sum(
                s.strategies_checked for s in report.by_type.values()
            ),
            "violations": len(report.violations),
            "rows_fixed": sum(f.rows_written for f in report.fixes if f.fix_applied),
        },
        "by_type": {
            t: {
                "strategies": s.strategies_checked,
                "violations": s.violations,
                "rows_fixed": s.rows_fixed,
            }
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
        rows.append(
            [
                r.get("run_id", ""),
                (r.get("started_at") or "").replace("T", " ").replace("Z", " UTC"),
                r.get("mode", ""),
                str(r.get("totals", {}).get("strategies_checked", "")),
                str(r.get("totals", {}).get("violations", "")),
                str(r.get("totals", {}).get("rows_fixed", "")),
                types,
                r.get("report_path") or "",
            ]
        )
    widths = [max(len(str(row[c])) for row in rows) for c in range(len(rows[0]))]
    lines = []
    for row in rows:
        lines.append("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    return "\n".join(lines)


# ── section F: report rendering (Task 7) ────────────────────────────────────


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
        num_underlyings = len({v.underlying for v in report.violations if v.type == t})
        lines.append(
            f"[{t}] {s.strategies_checked} strategies / "
            f"{num_underlyings} underlyings — {suffix}"
        )
        if s.violations:
            for v in report.violations:
                if v.type != t:
                    continue
                lines.append(
                    f"  {v.category}: {v.underlying} {v.strategy_table_name} "
                    f"— {v.detail}"
                )
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
        lines.append(
            f"- **Fix window:** [{ws:%Y-%m-%d %H:%M:%S}, {we:%Y-%m-%d %H:%M:%S}]"
        )
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
            lines.append(
                "| Underlying | Strategy | Category | failure_ts | Detail | Fix |"
            )
            lines.append("|---|---|---|---|---|---|")
            for v in type_viols:
                applied = any(
                    f.fix_applied
                    and f.type == v.type
                    and f.underlying == v.underlying
                    and f.strategy_table_name == v.strategy_table_name
                    for f in report.fixes
                )
                fix_str = "applied" if applied else "—"
                ts_str = (
                    v.failure_ts.strftime("%Y-%m-%d %H:%M:%S") if v.failure_ts else ""
                )
                lines.append(
                    f"| {v.underlying} | `{v.strategy_table_name}` | "
                    f"{v.category} | {ts_str} | {v.detail} | {fix_str} |"
                )
        lines.append("")
    path.write_text("\n".join(lines))


# ── section G: CLI + main (Task 8) ──────────────────────────────────────────


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Audit and (optionally) repair PnL tables.")
    p.add_argument("--type", choices=["prod", "bt", "real_trade", "all"], default="all")
    p.add_argument("--underlying", help="Filter to one underlying (default: all)")
    p.add_argument(
        "--fix",
        action="store_true",
        help="Apply fixes for detected issues (prod/rt forward, bt window)",
    )
    p.add_argument(
        "--fix-window",
        nargs=2,
        metavar=("START", "END"),
        help="Explicit-window repair; --type must be specified",
    )
    p.add_argument(
        "--report", help="Markdown report path (default: ./audit_reports/<UTC-ts>.md)"
    )
    p.add_argument(
        "--state-file",
        default=str(DEFAULT_REPORT_DIR / "history.jsonl"),
        help="JSONL run-history file",
    )
    p.add_argument(
        "--show-history",
        nargs="?",
        type=int,
        const=5,
        default=None,
        metavar="N",
        help="Print the last N runs and exit (default 5)",
    )
    p.add_argument(
        "--no-pause",
        action="store_true",
        help="Skip ECS consumer pause/resume during --fix",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect and compute fixes but do not write",
    )
    return p


def _resolve_types(arg: str) -> list[Mode]:
    return list(TYPES) if arg == "all" else [arg]  # type: ignore[list-item]


def _run_audit(
    types: list[Mode], underlying: str | None, client, now_ts: datetime
) -> AuditReport:
    """Run all five checks for each requested type. Returns a report with violations
    populated; fixes empty until a separate fix phase runs."""
    underlying = _validate_underlying(underlying)
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
        log.info(
            "[audit:%s] %d strategies, %d violations",
            t,
            summary.strategies_checked,
            summary.violations,
        )
    return report


def _run_fixes(
    report: AuditReport, client, ecs, *, dry_run: bool, no_pause: bool, now_ts: datetime
) -> None:
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

    # bt: window-local, no consumer pause. fix_bt_strategies handles its own
    # per-underlying hour-table refresh as a checkpoint, so no end-of-run
    # refresh is needed here.
    bt_fixes = fixes_by_type["bt"]
    if bt_fixes:
        fix_bt_strategies(bt_fixes, client, dry_run=dry_run, now_ts=now_ts)

    # Hour-sync-only failures: refresh hour table even when no minute writes happened.
    for t in TYPES:
        if any(f.type == t for f in report.fixes):
            continue  # already refreshed
        hour_only = [
            v for v in report.violations if v.type == t and v.category == "hour_sync"
        ]
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
        log.info(
            "Last run: %s — %d violations, %d rows fixed",
            p.get("started_at"),
            p.get("totals", {}).get("violations", 0),
            p.get("totals", {}).get("rows_fixed", 0),
        )

    # --fix-window builds its own per-strategy fix list from the source-history
    # table and never consults detection results, so skip the (expensive,
    # GLOBAL_START_TS-wide) audit scan in that path — it only added latency.
    if args.fix_window:
        report = AuditReport(
            started_at=_utcnow_naive(),
            scope_types=list(types),
            scope_underlying=_validate_underlying(args.underlying),
            run_id=_utcnow_naive().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:4],
        )
    else:
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
        # Select EVERY strategy with source bars in the window — a full
        # delete+recompute of the window, not a coverage-gap fill. The old
        # heuristic only picked strategies under 90% coverage, which silently
        # skipped strategies that were fully present but holding WRONG values
        # (e.g. late-revision pnl drift) — exactly the case a window recompute
        # exists to repair. Sourcing the list from the source-history table also
        # catches strategies entirely missing from the target. The bar-fetch
        # lookback mirrors the fix functions so 1d-tf strategies whose bar ts
        # precedes the window are included.
        src = SOURCE_TABLE[t]
        fetch_start = (ws - timedelta(minutes=_BAR_FETCH_LOOKBACK_MINUTES)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        strat_sql = f"""
        SELECT strategy_table_name, any(underlying)
        FROM analytics.{src}
        WHERE ts >= toDateTime('{fetch_start}')
          AND ts <  toDateTime('{we:%Y-%m-%d %H:%M:%S}')
          AND strategy_table_name NOT LIKE 'manual_probe%'
        GROUP BY strategy_table_name
        """
        synthetic: list[StrategyFix] = []
        for stn, und in client.query(strat_sql).result_rows:
            synthetic.append(
                StrategyFix(
                    type=t,
                    underlying=str(und),
                    strategy_table_name=str(stn),
                    failure_ts=ws,
                    window_end=we,
                    categories=["mid_gap"],
                )
            )
        log.info(
            "[--fix-window] %d strategies with source bars in [%s, %s) — recompute",
            len(synthetic),
            ws,
            we,
        )
        ecs = get_ecs_client() if not args.no_pause and not args.dry_run else None
        try:
            if t in ("prod", "real_trade"):
                if ecs:
                    pause_consumer(ecs, t)
                fix_prod_rt_strategies(
                    t, synthetic, client, dry_run=args.dry_run, now_ts=now_ts
                )
                refresh_hour_table(t, ws, client, dry_run=args.dry_run)
            else:
                # fix_bt_strategies refreshes its own hour table per underlying.
                fix_bt_strategies(
                    synthetic, client, dry_run=args.dry_run, now_ts=now_ts
                )
            # The 1-day rollup is MV-fed from 1-hour INSERTs and is not corrected
            # by the refresh above; rebuild it explicitly for the window.
            refresh_day_table(t, ws, client, dry_run=args.dry_run)
        finally:
            if ecs:
                resume_consumer(ecs, t)
        report.fixes.extend(synthetic)
    elif args.fix or args.dry_run:
        ecs = get_ecs_client() if not args.no_pause and not args.dry_run else None
        _run_fixes(
            report,
            client,
            ecs,
            dry_run=args.dry_run,
            no_pause=args.no_pause,
            now_ts=now_ts,
        )

    report.finished_at = _utcnow_naive()
    report_path = (
        Path(args.report) if args.report else DEFAULT_REPORT_DIR / f"{report.run_id}.md"
    )
    write_markdown(report, report_path)
    report.report_path = report_path

    append_run_record(report, state_path)

    print(render_console(report))
    return 0 if not report.has_violations() else 1


if __name__ == "__main__":
    sys.exit(main())
