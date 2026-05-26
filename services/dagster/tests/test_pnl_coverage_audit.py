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
