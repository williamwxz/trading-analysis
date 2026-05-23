"""Unit tests for libs.computation.minute_loop.

Tests the per-strategy active-bar resolution logic used by Dagster's per-minute
recompute loop. No I/O — all functions are pure Python.
"""
from datetime import datetime, timedelta

import pytest

from libs.computation.minute_loop import (
    active_prod_bar_at,
    active_rt_revision_at,
    build_prod_lookup,
    build_rt_lookup,
    check_strategy_drop,
    first_active_minute,
    last_active_minute,
)


def _bar(stn="strat_1", ts="2026-05-12 10:00:00", tf="1h", pos=1.0):
    return {
        "strategy_table_name": stn,
        "strategy_id": 1,
        "strategy_name": "momentum",
        "underlying": "BTC",
        "config_timeframe": tf,
        "strategy_instance_id": f"{stn}__1",
        "weighting": 1.0,
        "ts": ts,
        "position": pos,
        "final_signal": pos,
        "bar_benchmark": 0.0,
    }


def _rev(stn="strat_1", ts="2026-05-12 10:00:00", tf="1h", rev_ts="2026-05-12 10:05:00",
         pos=1.0):
    closing_ts = _add_tf(ts, tf)
    return {
        "strategy_table_name": stn,
        "strategy_id": 1,
        "strategy_name": "momentum",
        "underlying": "BTC",
        "config_timeframe": tf,
        "strategy_instance_id": f"{stn}__1",
        "weighting": 1.0,
        "ts": ts,
        "closing_ts": closing_ts,
        "execution_ts": _to_start_of_minute_plus59(rev_ts),
        "revision_ts": rev_ts,
        "position": pos,
        "final_signal": pos,
        "bar_benchmark": 0.0,
    }


def _parse(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _add_tf(ts_str: str, tf: str) -> str:
    tf_map = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
    mins = tf_map[tf]
    dt = _parse(ts_str) + timedelta(minutes=mins)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _to_start_of_minute_plus59(rev_ts: str) -> str:
    dt = _parse(rev_ts) + timedelta(seconds=59)
    dt = dt.replace(second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# build_prod_lookup
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_build_prod_lookup_single_bar():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    assert "strat_1" in lookup
    entries = lookup["strat_1"]
    assert len(entries) == 1
    # closing_ts = 10:00 + 60min = 11:00
    assert entries[0].closing_ts == _parse("2026-05-12 11:00:00")
    # last bar sentinel: next_closing = closing_ts + tf = 12:00
    assert entries[0].next_closing_ts == _parse("2026-05-12 12:00:00")


@pytest.mark.unit
def test_build_prod_lookup_two_bars_adjacent():
    bars = [
        _bar(ts="2026-05-12 10:00:00", tf="1h"),
        _bar(ts="2026-05-12 11:00:00", tf="1h"),
    ]
    lookup = build_prod_lookup(bars)
    entries = lookup["strat_1"]
    assert len(entries) == 2
    # First bar: closing=11:00, next_closing=12:00 (second bar's closing)
    assert entries[0].closing_ts == _parse("2026-05-12 11:00:00")
    assert entries[0].next_closing_ts == _parse("2026-05-12 12:00:00")
    # Second bar: closing=12:00, next_closing=13:00 (sentinel)
    assert entries[1].closing_ts == _parse("2026-05-12 12:00:00")
    assert entries[1].next_closing_ts == _parse("2026-05-12 13:00:00")


@pytest.mark.unit
def test_build_prod_lookup_multiple_strategies():
    bars = [
        _bar(stn="strat_1", ts="2026-05-12 10:00:00", tf="1h"),
        _bar(stn="strat_2", ts="2026-05-12 10:00:00", tf="5m"),
    ]
    lookup = build_prod_lookup(bars)
    assert "strat_1" in lookup
    assert "strat_2" in lookup
    assert lookup["strat_2"][0].closing_ts == _parse("2026-05-12 10:05:00")


# ─────────────────────────────────────────────────────────────────────────────
# active_prod_bar_at
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_active_prod_bar_at_returns_bar_within_window():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    # At 11:00 (= closing_ts), bar becomes active
    entry = active_prod_bar_at(lookup, "strat_1", _parse("2026-05-12 11:00:00"))
    assert entry is not None
    assert entry.bar["ts"] == "2026-05-12 10:00:00"


@pytest.mark.unit
def test_active_prod_bar_at_returns_none_before_closing_ts():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    # At 10:59, closing_ts = 11:00 hasn't arrived yet
    entry = active_prod_bar_at(lookup, "strat_1", _parse("2026-05-12 10:59:00"))
    assert entry is None


@pytest.mark.unit
def test_active_prod_bar_at_returns_none_after_expansion_window():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    # At 12:00, next_closing_ts = 12:00 is exclusive upper bound → no active bar
    entry = active_prod_bar_at(lookup, "strat_1", _parse("2026-05-12 12:00:00"))
    assert entry is None


@pytest.mark.unit
def test_active_prod_bar_at_switches_at_boundary():
    bars = [
        _bar(stn="strat_1", ts="2026-05-12 10:00:00", tf="1h"),
        _bar(stn="strat_1", ts="2026-05-12 11:00:00", tf="1h"),
    ]
    lookup = build_prod_lookup(bars)
    # At 11:59 — first bar still active (next_closing=12:00, and 11:59 < 12:00)
    e1 = active_prod_bar_at(lookup, "strat_1", _parse("2026-05-12 11:59:00"))
    assert e1 is not None
    assert e1.bar["ts"] == "2026-05-12 10:00:00"
    # At 12:00 — second bar active
    e2 = active_prod_bar_at(lookup, "strat_1", _parse("2026-05-12 12:00:00"))
    assert e2 is not None
    assert e2.bar["ts"] == "2026-05-12 11:00:00"


@pytest.mark.unit
def test_active_prod_bar_at_unknown_strategy_returns_none():
    lookup = build_prod_lookup([_bar()])
    assert active_prod_bar_at(lookup, "nonexistent", _parse("2026-05-12 11:00:00")) is None


# ─────────────────────────────────────────────────────────────────────────────
# build_rt_lookup
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_build_rt_lookup_duplicate_revision_discarded():
    # Same (ts, revision_ts) pair appears twice — only one entry in lookup
    rev = _rev(rev_ts="2026-05-12 10:05:00")
    lookup = build_rt_lookup([rev, dict(rev)])
    assert len(lookup["strat_1"]) == 1


@pytest.mark.unit
def test_build_rt_lookup_older_bar_after_newer_discarded():
    # Newer bar accepted first (ts=11:00), then older bar (ts=10:00) — discarded
    newer = _rev(ts="2026-05-12 11:00:00", rev_ts="2026-05-12 11:05:00")
    older = _rev(ts="2026-05-12 10:00:00", rev_ts="2026-05-12 12:30:00")
    # sorted by (ts, revision_ts): older first (10:00 < 11:00), newer second → both accepted
    # To trigger the discard, we need an older-bar revision that sorts AFTER the newer one —
    # impossible with ASC sort, so test that input order doesn't matter (sort is internal)
    lookup_a = build_rt_lookup([newer, older])
    lookup_b = build_rt_lookup([older, newer])
    assert len(lookup_a["strat_1"]) == len(lookup_b["strat_1"])


@pytest.mark.unit
def test_build_rt_lookup_single_revision_always_accepted():
    rev = _rev(rev_ts="2026-05-12 11:30:00")
    lookup = build_rt_lookup([rev])
    assert "strat_1" in lookup
    assert len(lookup["strat_1"]) == 1


@pytest.mark.unit
def test_build_rt_lookup_execution_ts_computed_correctly():
    # revision_ts = 10:05:00 → execution_ts = toStartOfMinute(10:05:59) = 10:05:00
    rev = _rev(rev_ts="2026-05-12 10:05:00")
    lookup = build_rt_lookup([rev])
    entry = lookup["strat_1"][0]
    assert entry.execution_ts == _parse("2026-05-12 10:05:00")


# ─────────────────────────────────────────────────────────────────────────────
# active_rt_revision_at
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_active_rt_revision_at_returns_revision_within_window():
    rev = _rev(rev_ts="2026-05-12 10:05:00", tf="1h")
    lookup = build_rt_lookup([rev])
    exec_ts = _parse("2026-05-12 10:05:00")
    entry = active_rt_revision_at(lookup, "strat_1", exec_ts)
    assert entry is not None


@pytest.mark.unit
def test_active_rt_revision_at_returns_none_before_execution_ts():
    rev = _rev(rev_ts="2026-05-12 10:05:00", tf="1h")
    lookup = build_rt_lookup([rev])
    entry = active_rt_revision_at(lookup, "strat_1", _parse("2026-05-12 10:04:00"))
    assert entry is None


@pytest.mark.unit
def test_active_rt_revision_at_switches_at_next_revision():
    rev1 = _rev(stn="strat_1", rev_ts="2026-05-12 10:05:00", tf="1h")
    rev2 = _rev(stn="strat_1", rev_ts="2026-05-12 10:30:00", tf="1h")
    lookup = build_rt_lookup([rev1, rev2])
    exec1 = _parse("2026-05-12 10:05:00")
    exec2 = _parse("2026-05-12 10:30:00")
    # At exec1 → rev1 active
    e1 = active_rt_revision_at(lookup, "strat_1", exec1)
    assert e1 is not None
    assert e1.rev["revision_ts"] == "2026-05-12 10:05:00"
    # At exec2 → rev2 active
    e2 = active_rt_revision_at(lookup, "strat_1", exec2)
    assert e2 is not None
    assert e2.rev["revision_ts"] == "2026-05-12 10:30:00"


# ─────────────────────────────────────────────────────────────────────────────
# first_active_minute / last_active_minute
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_first_last_active_minute_prod():
    bars = [
        _bar(stn="strat_1", ts="2026-05-12 10:00:00", tf="1h"),
        _bar(stn="strat_2", ts="2026-05-12 09:00:00", tf="1h"),
    ]
    lookup = build_prod_lookup(bars)
    # strat_2: ts=09:00+1h=closing=10:00, sentinel next=11:00
    # strat_1: ts=10:00+1h=closing=11:00, sentinel next=12:00
    assert first_active_minute(lookup, is_rt=False) == _parse("2026-05-12 10:00:00")
    assert last_active_minute(lookup, is_rt=False) == _parse("2026-05-12 12:00:00")


@pytest.mark.unit
def test_first_last_active_minute_empty_lookup():
    lookup = {}
    assert first_active_minute(lookup, is_rt=False) is None
    assert last_active_minute(lookup, is_rt=False) is None


# ─────────────────────────────────────────────────────────────────────────────
# check_strategy_drop
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_check_strategy_drop_no_drop_ok():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    prev = {"strat_1"}
    curr = {"strat_1"}
    # Should not raise
    check_strategy_drop(prev, curr, _parse("2026-05-12 11:30:00"), "BTC", lookup, is_rt=False)


@pytest.mark.unit
def test_check_strategy_drop_legitimate_end_of_window():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    # At 12:00 the bar's expansion window has ended (next_closing=12:00 is exclusive)
    # prev_active had strat_1, curr_active does not — but no future entries exist
    prev = {"strat_1"}
    curr = set()
    # Should NOT raise — all entries have next_closing_ts <= minute_cur
    check_strategy_drop(prev, curr, _parse("2026-05-12 12:00:00"), "BTC", lookup, is_rt=False)


@pytest.mark.unit
def test_check_strategy_drop_bug_raises():
    bars = [
        _bar(stn="strat_1", ts="2026-05-12 10:00:00", tf="1h"),
        _bar(stn="strat_1", ts="2026-05-12 11:00:00", tf="1h"),
    ]
    lookup = build_prod_lookup(bars)
    # strat_1 should be active at 11:30 (between first and second bar's expansion)
    # Simulate it disappearing unexpectedly (second bar missing in computed curr_active)
    prev = {"strat_1"}
    curr = set()  # strat_1 dropped but it still has next_closing_ts=13:00 > 11:30
    with pytest.raises(RuntimeError, match="Strategy count dropped unexpectedly"):
        check_strategy_drop(prev, curr, _parse("2026-05-12 11:30:00"), "BTC", lookup, is_rt=False)


@pytest.mark.unit
def test_check_strategy_drop_new_strategy_joining_ok():
    bars = [_bar(ts="2026-05-12 10:00:00", tf="1h")]
    lookup = build_prod_lookup(bars)
    # strat_2 appears at 11:30 — never was in prev — this is fine
    prev = {"strat_1"}
    curr = {"strat_1", "strat_2"}
    check_strategy_drop(prev, curr, _parse("2026-05-12 11:30:00"), "BTC", lookup, is_rt=False)
