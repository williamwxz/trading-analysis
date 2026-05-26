"""Per-minute active-bar resolution for Dagster batch PnL recompute.

Provides the same semantics as pnl_consumer's candle_lookup queries, but
resolved in Python from a pre-fetched bar/revision dataset — avoiding
one ClickHouse query per minute over a multi-day window.

For prod/bt:
    Active bar at minute M = latest bar B where closing_ts(B) <= M < next_closing_ts(B).
    Mirrors candle_lookup.fetch_strategies_for_candle: ts + tf_minutes <= candle_ts.

For real_trade:
    Active revision at minute M = accepted revision R where execution_ts(R) <= M
    < next_execution_ts(R). Acceptance filter: (bar_ts, revision_ts) must be strictly
    greater than the previous accepted revision's (bar_ts, revision_ts) — mirrors
    AnchorState.should_apply_revision in the pnl_consumer.
"""

from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from libs.computation.pnl_formula import TIMEFRAME_MAP


def _parse_ts(s: str) -> datetime:
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


@dataclass
class ProdBarEntry:
    closing_ts: datetime
    next_closing_ts: datetime
    bar: dict


@dataclass
class RtRevisionEntry:
    execution_ts: datetime
    next_execution_ts: datetime
    rev: dict


# {strategy_table_name: [ProdBarEntry sorted by closing_ts ASC]}
ProdLookup = dict[str, list[ProdBarEntry]]
# {strategy_table_name: [RtRevisionEntry sorted by execution_ts ASC]}
RtLookup = dict[str, list[RtRevisionEntry]]


def build_prod_lookup(bars: list[dict]) -> ProdLookup:
    """Build per-strategy sorted closing_ts lookup for prod/bt bars."""
    by_stn: dict[str, list[dict]] = {}
    for bar in bars:
        by_stn.setdefault(bar["strategy_table_name"], []).append(bar)

    lookup: ProdLookup = {}
    for stn, stn_bars in by_stn.items():
        stn_bars.sort(key=lambda b: b["ts"])
        entries: list[ProdBarEntry] = []
        for i, bar in enumerate(stn_bars):
            tf = TIMEFRAME_MAP.get(bar["config_timeframe"], 5)
            closing_ts = _parse_ts(bar["ts"]) + timedelta(minutes=tf)
            if i + 1 < len(stn_bars):
                next_tf = TIMEFRAME_MAP.get(stn_bars[i + 1]["config_timeframe"], 5)
                next_closing_ts = _parse_ts(stn_bars[i + 1]["ts"]) + timedelta(minutes=next_tf)
            else:
                next_closing_ts = closing_ts + timedelta(minutes=tf)
            entries.append(ProdBarEntry(closing_ts=closing_ts, next_closing_ts=next_closing_ts, bar=bar))
        lookup[stn] = entries
    return lookup


def build_rt_lookup(bars: list[dict]) -> RtLookup:
    """Build per-strategy sorted execution_ts lookup for real_trade revisions.

    Acceptance filter mirrors AnchorState.should_apply_revision: a revision is
    accepted iff (bar_ts, revision_ts) > (prev_accepted.ts, prev_accepted.revision_ts).
    Revisions are sorted by (ts, revision_ts) ascending before filtering.

    After acceptance, revisions are re-sorted by execution_ts and deduplicated:
    when multiple accepted revisions share the same execution_ts (e.g. a late
    bulk re-revision batch covering many bars at the same revision_ts), only the
    last one in (ts, revision_ts) order is kept. This ensures the lookup timeline
    is strictly monotonic so no entry has a zero-width active window.
    """
    by_stn: dict[str, list[dict]] = {}
    for bar in bars:
        by_stn.setdefault(bar["strategy_table_name"], []).append(bar)

    lookup: RtLookup = {}
    for stn, stn_bars in by_stn.items():
        stn_bars.sort(key=lambda b: (b["ts"], b["revision_ts"]))
        accepted: list[dict] = []
        last_bar_ts: str = ""
        last_revision_ts: str = ""
        for rev in stn_bars:
            if (rev["ts"], rev["revision_ts"]) > (last_bar_ts, last_revision_ts):
                accepted.append(rev)
                last_bar_ts = rev["ts"]
                last_revision_ts = rev["revision_ts"]
        if not accepted:
            continue

        # Re-sort by execution_ts; deduplicate by keeping the last accepted
        # revision per execution_ts (latest (ts, revision_ts) wins).
        accepted.sort(key=lambda b: (b["execution_ts"], b["ts"], b["revision_ts"]))
        deduped: list[dict] = []
        for rev in accepted:
            if deduped and deduped[-1]["execution_ts"] == rev["execution_ts"]:
                deduped[-1] = rev  # replace with later (ts, revision_ts)
            else:
                deduped.append(rev)

        entries: list[RtRevisionEntry] = []
        for i, rev in enumerate(deduped):
            exec_ts = _parse_ts(rev["execution_ts"])
            if i + 1 < len(deduped):
                next_exec_ts = _parse_ts(deduped[i + 1]["execution_ts"])
            else:
                tf = TIMEFRAME_MAP.get(rev["config_timeframe"], 5)
                next_exec_ts = _parse_ts(rev["closing_ts"]) + timedelta(minutes=tf)
            entries.append(RtRevisionEntry(execution_ts=exec_ts, next_execution_ts=next_exec_ts, rev=rev))
        lookup[stn] = entries
    return lookup


def active_prod_bar_at(lookup: ProdLookup, stn: str, minute: datetime) -> Optional[ProdBarEntry]:
    """Return the active bar for stn at minute, or None if no bar covers this minute."""
    entries = lookup.get(stn)
    if not entries:
        return None
    keys = [e.closing_ts for e in entries]
    idx = bisect_right(keys, minute) - 1
    if idx < 0:
        return None
    entry = entries[idx]
    return entry if minute < entry.next_closing_ts else None


def active_rt_revision_at(lookup: RtLookup, stn: str, minute: datetime) -> Optional[RtRevisionEntry]:
    """Return the active accepted revision for stn at minute, or None."""
    entries = lookup.get(stn)
    if not entries:
        return None
    keys = [e.execution_ts for e in entries]
    idx = bisect_right(keys, minute) - 1
    if idx < 0:
        return None
    entry = entries[idx]
    return entry if minute < entry.next_execution_ts else None


def first_active_minute(lookup: ProdLookup | RtLookup, is_rt: bool) -> Optional[datetime]:
    """Return the earliest minute any strategy becomes active."""
    candidates: list[datetime] = []
    for entries in lookup.values():
        if entries:
            candidates.append(entries[0].execution_ts if is_rt else entries[0].closing_ts)
    return min(candidates) if candidates else None


def last_active_minute(lookup: ProdLookup | RtLookup, is_rt: bool) -> Optional[datetime]:
    """Return the latest minute any strategy is still active (exclusive upper bound)."""
    candidates: list[datetime] = []
    for entries in lookup.values():
        if entries:
            candidates.append(entries[-1].next_execution_ts if is_rt else entries[-1].next_closing_ts)
    return max(candidates) if candidates else None


def check_strategy_drop(
    prev_active: set[str],
    curr_active: set[str],
    minute: datetime,
    underlying: str,
    lookup: ProdLookup | RtLookup,
    is_rt: bool,
) -> None:
    """Raise if a strategy that was active at M-1 unexpectedly has no bar at M.

    A drop is legitimate when the strategy has exhausted all its bars in the
    fetched window (all entries have next_closing_ts/next_execution_ts <= minute).
    A drop is a bug when the strategy's lookup still has a future window that
    should cover minute — indicating a missing source bar.
    """
    dropped = prev_active - curr_active
    if not dropped:
        return
    bug_drops: list[str] = []
    for stn in dropped:
        entries = lookup.get(stn, [])
        attr = "next_execution_ts" if is_rt else "next_closing_ts"
        if any(getattr(e, attr) > minute for e in entries):
            bug_drops.append(stn)
    if bug_drops:
        raise RuntimeError(
            f"[{underlying}] Strategy count dropped unexpectedly at {minute}: "
            f"{len(bug_drops)} strategies have a bar window covering this minute but "
            f"returned no active bar. Missing: {sorted(bug_drops)[:5]}"
            f"{'...' if len(bug_drops) > 5 else ''}. "
            "Source table likely has a data hole."
        )
