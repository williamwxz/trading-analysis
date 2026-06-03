"""Unit tests for pnl_coverage_audit pure-Python check functions."""

from datetime import datetime, timedelta

from services.dagster.trading_dagster.assets.pnl_coverage_audit import (
    TOP_N_OFFENDERS,
    AuditReport,
    PositionChange,
    SourceFirstBar,
    StratStat,
    Violation,
    _check_phase1,
    _check_phase2,
    _check_phase3,
    _check_phase3_bucketed,
    _check_position_per_minute,
    _compute_source_changes_prod_bt,
    _compute_source_changes_rt,
    _format_report,
)


def _dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


# ── Phase 1: coverage ────────────────────────────────────────────────────────
#
# _check_phase1 now takes ``expected_minutes`` (the price-window count the driver
# computes server-side) instead of a materialized price set.


class TestPhase1:
    def test_clean_match_returns_no_violation(self):
        """Actual range matches expected, no missing minutes — no violation."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-05-26 09:55:00"),
            actual_rows=3,
        )
        source = SourceFirstBar(
            expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5
        )
        # 3 price minutes in [expected_min, now-5m]; actual_rows == 3 → no holes.
        expected_minutes = 3
        now = _dt("2026-05-26 10:00:00")
        v = _check_phase1("t", "FET", "S", stat, source, expected_minutes, now)
        assert v is None

    def test_detects_start_gap(self):
        """actual_min_ts >> expected_min_ts → start_gap."""
        stat = StratStat(
            actual_min_ts=_dt("2026-05-02 20:44:00"),
            actual_max_ts=_dt("2026-05-26 09:58:00"),
            actual_rows=1000,
        )
        source = SourceFirstBar(
            expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5
        )
        now = _dt("2026-05-26 10:00:00")
        # start_gap is checked before internal_holes, so the count is irrelevant.
        v = _check_phase1("t", "FET", "S", stat, source, 1, now)
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
        source = SourceFirstBar(
            expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5
        )
        now = _dt("2026-05-26 10:00:00")  # 30 min ahead of actual_max
        v = _check_phase1("t", "FET", "S", stat, source, 1, now)
        assert v is not None
        assert v.category == "stale_end"

    def test_detects_internal_holes(self):
        """actual_rows < expected_minutes → internal_holes."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-03-05 09:55:00"),
            actual_rows=2,
        )
        source = SourceFirstBar(
            expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5
        )
        # 4 price minutes in the window vs 2 actual rows → 2 missing.
        expected_minutes = 4
        now = _dt("2026-03-05 10:00:00")
        v = _check_phase1("t", "FET", "S", stat, source, expected_minutes, now)
        assert v is not None
        assert v.category == "internal_holes"
        # 4 expected minutes vs 2 actual → 2 missing.
        assert "2" in v.detail

    def test_stale_end_takes_priority_over_holes(self):
        """stale_end is checked before internal_holes regardless of the count."""
        stat = StratStat(
            actual_min_ts=_dt("2026-03-05 09:05:00"),
            actual_max_ts=_dt("2026-03-05 09:15:00"),
            actual_rows=2,
        )
        source = SourceFirstBar(
            expected_min_ts=_dt("2026-03-05 09:05:00"), tf_minutes=5
        )
        now = _dt("2026-03-05 09:30:00")  # 15m past max; well past actual_max
        v = _check_phase1("t", "FET", "S", stat, source, 2, now)
        # Actual_rows == expected (2), but stale_end fires because actual_max < now-10m.
        assert v is not None
        assert v.category == "stale_end"


# ── Phase 2: gap drilldown ───────────────────────────────────────────────────


class TestPhase2:
    def test_collects_top_gaps_by_duration(self):
        """Returns the top N gaps sorted by gap_minutes desc, descriptor-typed."""
        # Q_gap returns list of (gap_end, gap_secs). Phase 2 trims by price-gap
        # exemption and sorts. has_prices=False → no exemption.
        q_gap_rows = [
            (_dt("2026-03-05 10:00:00"), 600),  # 10 min gap
            (_dt("2026-03-06 11:00:00"), 7200),  # 2-hour gap
            (_dt("2026-03-07 12:01:00"), 120),  # 2 min gap
        ]
        present_counts = [0, 0, 0]  # ignored when has_prices=False
        descriptors = _check_phase2(
            "FET", "S", q_gap_rows, present_counts, has_prices=False, top_n=2
        )
        assert len(descriptors) == 2
        # Sorted by gap_minutes desc.
        assert descriptors[0].gap_minutes == 120
        assert descriptors[0].gap_end == _dt("2026-03-06 11:00:00")
        assert descriptors[1].gap_minutes == 10

    def test_price_gap_exemption_reduces_gap_minutes(self):
        """Missing price minutes inside a gap are subtracted from gap_minutes."""
        # Raw gap is 10 minutes over (09:50, 10:00). One price present strictly
        # inside (e.g. 09:53) → effective = 1 + 1 = 2.
        q_gap_rows = [(_dt("2026-03-05 10:00:00"), 600)]
        present_counts = [1]
        descriptors = _check_phase2(
            "FET", "S", q_gap_rows, present_counts, has_prices=True, top_n=5
        )
        assert len(descriptors) == 1
        assert descriptors[0].gap_minutes == 2

    def test_gap_fully_explained_by_prices_returns_empty(self):
        """If no price minute lies strictly inside, gap is fully exempt."""
        q_gap_rows = [(_dt("2026-03-05 10:00:00"), 600)]
        # Anchors present only at the boundaries 09:50 and 10:00 → 0 strictly inside.
        present_counts = [0]
        descriptors = _check_phase2(
            "FET", "S", q_gap_rows, present_counts, has_prices=True, top_n=5
        )
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
        """Hour slot position should match latest 1-min position <= hour+1h."""
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
        v = _check_phase3_bucketed(
            "t1h",
            "FET",
            "S",
            hour_rows,
            target_min_changes,
            bucket=timedelta(hours=1),
        )
        assert v is None

    def test_hour_slot_position_drift_fails(self):
        target_min_changes = [PositionChange(_dt("2026-03-05 09:30:00"), -1.0)]
        hour_rows = [(_dt("2026-03-05 09:00:00"), 1.0)]  # wrong
        v = _check_phase3_bucketed(
            "t1h",
            "FET",
            "S",
            hour_rows,
            target_min_changes,
            bucket=timedelta(hours=1),
        )
        assert v is not None
        assert v.category == "position_mismatch"

    def test_hour_slot_with_no_prior_minute_change_skips(self):
        """If a 1-hour row exists before any 1-min change, we cannot compare — skip."""
        target_min_changes = [PositionChange(_dt("2026-03-05 10:00:00"), 1.0)]
        hour_rows = [(_dt("2026-03-05 09:00:00"), 0.0)]  # no prior min change
        v = _check_phase3_bucketed(
            "t1h",
            "FET",
            "S",
            hour_rows,
            target_min_changes,
            bucket=timedelta(hours=1),
        )
        assert v is None  # can't verify, skip silently


# ── Phase 3 (day table): slot position match ─────────────────────────────────


class TestPhase3Day:
    def test_day_slot_matches_latest_minute_change(self):
        """Day slot position should match latest 1-min position <= day+1d."""
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
            "t1d",
            "FET",
            "S",
            day_rows,
            target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is None

    def test_day_slot_position_drift_fails(self):
        target_min_changes = [PositionChange(_dt("2026-03-05 23:30:00"), -1.0)]
        day_rows = [(_dt("2026-03-05 00:00:00"), 1.0)]  # wrong
        v = _check_phase3_bucketed(
            "t1d",
            "FET",
            "S",
            day_rows,
            target_min_changes,
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
            "t1d",
            "FET",
            "S",
            day_rows,
            target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is None

    def test_day_slot_with_no_prior_minute_change_skips(self):
        target_min_changes = [PositionChange(_dt("2026-03-06 10:00:00"), 1.0)]
        day_rows = [(_dt("2026-03-05 00:00:00"), 0.0)]  # no prior min change
        v = _check_phase3_bucketed(
            "t1d",
            "FET",
            "S",
            day_rows,
            target_min_changes,
            bucket=timedelta(days=1),
        )
        assert v is None  # can't verify, skip silently


# ── Report formatter ─────────────────────────────────────────────────────────


class TestFormatReport:
    def test_empty_report_returns_clean_message(self):
        from services.dagster.trading_dagster.assets.pnl_coverage_audit import (
            AuditReport,
        )

        report = AuditReport()
        msg = _format_report(report)
        assert "CLEAN" in msg

    def test_groups_by_table_and_sorts_top_n(self):
        from services.dagster.trading_dagster.assets.pnl_coverage_audit import (
            AuditReport,
        )

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


# ── Driver: per-underlying audit ─────────────────────────────────────────────


class TestAuditUnderlying:
    def test_clean_underlying_returns_no_violations(self, monkeypatch):
        """All phases pass → empty violations list."""
        from services.dagster.trading_dagster.assets import pnl_coverage_audit as mod

        monkeypatch.setattr(
            mod,
            "_fetch_q_stat",
            lambda t, u, c: {
                "S1": StratStat(
                    _dt("2026-03-05 09:05:00"), _dt("2026-05-26 09:55:00"), 2
                ),
            },
        )
        monkeypatch.setattr(mod, "_has_prices", lambda u, c: True)
        # Source bar at 09:00, tf=5 → first-bar 09:05, one change at 09:05 (pos 1.0).
        monkeypatch.setattr(
            mod,
            "_load_source_prod_bt",
            lambda st, u, stn, c, tfv: (
                SourceFirstBar(_dt("2026-03-05 09:05:00"), 5),
                [PositionChange(_dt("2026-03-05 09:05:00"), 1.0)],
            ),
        )
        # actual_rows == 2 matches expected_minutes → no internal_holes.
        monkeypatch.setattr(mod, "_count_prices", lambda u, s, e, c: 2)
        monkeypatch.setattr(mod, "_fetch_q_gap", lambda t, u, s, c: [])
        # Phase 3: streamed target series, position matches source (1.0).
        monkeypatch.setattr(
            mod,
            "_iter_q_target_full",
            lambda t, u, s, c: iter(
                [
                    (_dt("2026-03-05 09:05:00"), 1.0),
                    (_dt("2026-05-26 09:55:00"), 1.0),
                ]
            ),
        )

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
        monkeypatch.setattr(
            mod,
            "_fetch_q_stat",
            lambda t, u, c: {
                "S1": StratStat(
                    _dt("2026-05-02 20:44:00"), _dt("2026-05-26 09:55:00"), 100
                ),
            },
        )
        monkeypatch.setattr(mod, "_has_prices", lambda u, c: True)
        monkeypatch.setattr(
            mod,
            "_load_source_prod_bt",
            lambda st, u, stn, c, tfv: (
                SourceFirstBar(_dt("2026-03-05 09:05:00"), 5),
                [PositionChange(_dt("2026-03-05 09:05:00"), 1.0)],
            ),
        )
        monkeypatch.setattr(mod, "_count_prices", lambda u, s, e, c: 1)
        monkeypatch.setattr(mod, "_fetch_q_gap", lambda t, u, s, c: [])
        # Streamed per-minute target series; position matches source (1.0).
        monkeypatch.setattr(
            mod,
            "_iter_q_target_full",
            lambda t, u, s, c: iter(
                [
                    (_dt("2026-05-02 20:45:00"), 1.0),
                    (_dt("2026-05-26 09:55:00"), 1.0),
                ]
            ),
        )

        report = mod._audit_underlying(
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            mode="prod",
            underlying="FET",
            client=None,
            now_ts=_dt("2026-05-26 10:00:00"),
        )
        # Phase 1 reports start_gap; Phase 3 (now always runs) finds no mismatch.
        assert any(v.category == "start_gap" for v in report.violations)
        assert not any(v.category == "position_mismatch" for v in report.violations)


# ── Driver: per-table audit ──────────────────────────────────────────────────


class TestAuditTable:
    def test_loops_underlyings_collects_all_violations(self, monkeypatch):
        from services.dagster.trading_dagster.assets import pnl_coverage_audit as mod

        # Two underlyings each with one violation.
        def fake_audit_underlying(
            target_table, source_table, mode, underlying, client, now_ts
        ):
            r = AuditReport()
            r.strategies_checked = 1
            r.violations = [
                Violation(target_table, underlying, "S1", "start_gap", "...", 100)
            ]
            return r

        monkeypatch.setattr(mod, "_audit_underlying", fake_audit_underlying)
        monkeypatch.setattr(mod, "_list_underlyings", lambda t, c: ["FET", "ETH"])
        monkeypatch.setattr(mod, "get_client", lambda: None)

        report = mod._audit_table(
            target_table="strategy_pnl_1min_prod_v2",
            source_table="strategy_output_history_v2",
            mode="prod",
            client=None,
            now_ts=_dt("2026-05-26 10:00:00"),
        )
        assert len(report.violations) == 2
        assert report.strategies_checked == 2


# ── Per-minute position check ────────────────────────────────────────────────


class TestPositionPerMinute:
    def test_clean_match_no_mismatches(self):
        source = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 09:10:00"), -1.0),
        ]
        target = [
            (_dt("2026-03-05 09:05:00"), 1.0),
            (_dt("2026-03-05 09:06:00"), 1.0),
            (_dt("2026-03-05 09:09:00"), 1.0),
            (_dt("2026-03-05 09:10:00"), -1.0),
            (_dt("2026-03-05 09:11:00"), -1.0),
        ]
        mismatches, orphans, samples = _check_position_per_minute(source, target)
        assert mismatches == 0
        assert orphans == 0
        assert samples == []

    def test_detects_wrong_position(self):
        source = [PositionChange(_dt("2026-03-05 09:05:00"), 1.0)]
        target = [
            (_dt("2026-03-05 09:05:00"), 1.0),
            (_dt("2026-03-05 09:06:00"), -1.0),  # WRONG: should still be 1.0
            (_dt("2026-03-05 09:07:00"), -1.0),  # WRONG
        ]
        mismatches, orphans, samples = _check_position_per_minute(source, target)
        assert mismatches == 2
        assert orphans == 0
        assert len(samples) == 2
        assert samples[0].ts == _dt("2026-03-05 09:06:00")
        assert samples[0].expected == 1.0
        assert samples[0].actual == -1.0

    def test_orphan_rows_before_first_source_change(self):
        source = [PositionChange(_dt("2026-03-05 09:10:00"), 1.0)]
        target = [
            (_dt("2026-03-05 09:05:00"), 0.0),  # orphan: before any source change
            (_dt("2026-03-05 09:10:00"), 1.0),
            (_dt("2026-03-05 09:11:00"), 1.0),
        ]
        mismatches, orphans, samples = _check_position_per_minute(source, target)
        assert mismatches == 0
        assert orphans == 1

    def test_handles_position_transition_at_exact_ts(self):
        """Target row AT source.effective_ts uses source change (>= boundary)."""
        source = [
            PositionChange(_dt("2026-03-05 09:05:00"), 1.0),
            PositionChange(_dt("2026-03-05 09:10:00"), -1.0),
        ]
        target = [
            (_dt("2026-03-05 09:09:00"), 1.0),
            (_dt("2026-03-05 09:10:00"), -1.0),  # AT boundary — uses -1.0
            (_dt("2026-03-05 09:11:00"), -1.0),
        ]
        mismatches, _, _ = _check_position_per_minute(source, target)
        assert mismatches == 0

    def test_empty_inputs_return_zero(self):
        assert _check_position_per_minute([], []) == (0, 0, [])
        assert _check_position_per_minute(
            [PositionChange(_dt("2026-03-05 09:00:00"), 1.0)], []
        ) == (0, 0, [])
        assert _check_position_per_minute([], [(_dt("2026-03-05 09:00:00"), 1.0)]) == (
            0,
            0,
            [],
        )

    def test_samples_capped_at_top_n(self):
        source = [PositionChange(_dt("2026-03-05 09:00:00"), 1.0)]
        # 20 wrong rows.
        target = [
            (_dt("2026-03-05 09:00:00") + timedelta(minutes=i + 1), -1.0)
            for i in range(20)
        ]
        mismatches, _, samples = _check_position_per_minute(source, target)
        assert mismatches == 20
        assert len(samples) == TOP_N_OFFENDERS


# ── Target-side reads must be bounded to GLOBAL_START_TS ──


class TestTargetReadsBounded:
    """Every target-side query must filter ts >= GLOBAL_START_TS.

    Streaming keeps Python memory bounded, but without the ts bound these
    queries still scan the bt target's full 2020->now history (1.24B rows) every
    run — high ClickHouse query memory and runtime. The audit only compares
    against source/price data starting at GLOBAL_START_TS, so target reads must
    match that window.
    """

    def _cap(self, monkeypatch):
        from services.dagster.trading_dagster.assets import pnl_coverage_audit as mod

        captured: list[str] = []
        monkeypatch.setattr(
            mod, "query_rows", lambda sql, client=None, **k: captured.append(sql) or []
        )

        def fake_stream(sql, client=None, **k):
            captured.append(sql)
            return iter(())

        monkeypatch.setattr(mod, "query_rows_stream", fake_stream)
        return mod, captured

    def test_fetch_q_stat_is_bounded(self, monkeypatch):
        mod, cap = self._cap(monkeypatch)
        mod._fetch_q_stat("strategy_pnl_1min_bt_v2", "BTC", None)
        assert mod.GLOBAL_START_TS in cap[0]

    def test_iter_q_target_full_is_bounded(self, monkeypatch):
        mod, cap = self._cap(monkeypatch)
        list(mod._iter_q_target_full("strategy_pnl_1min_bt_v2", "BTC", "S1", None))
        assert mod.GLOBAL_START_TS in cap[0]

    def test_fetch_q_gap_is_bounded(self, monkeypatch):
        mod, cap = self._cap(monkeypatch)
        mod._fetch_q_gap("strategy_pnl_1min_bt_v2", "BTC", "S1", None)
        assert mod.GLOBAL_START_TS in cap[0]

    def test_fetch_q_trans_is_bounded(self, monkeypatch):
        mod, cap = self._cap(monkeypatch)
        mod._fetch_q_trans("strategy_pnl_1min_bt_v2", "BTC", "S1", None)
        assert mod.GLOBAL_START_TS in cap[0]

    def test_fetch_q_stat_bucketed_is_bounded(self, monkeypatch):
        mod, cap = self._cap(monkeypatch)
        mod._fetch_q_stat_bucketed("strategy_pnl_1hour_bt_v2", "BTC", None)
        assert mod.GLOBAL_START_TS in cap[0]

    def test_fetch_q_stat_bucketed_deduplicates_by_updated_at(self, monkeypatch):
        """ReplacingMergeTree rollup slots can have multiple rows before merges
        run. The fetch must keep only the latest row per (strategy_table_name,
        ts) using LIMIT 1 BY with updated_at DESC as the tiebreaker — otherwise
        the audit may compare against a stale row and report spurious mismatches.
        """
        mod, cap = self._cap(monkeypatch)
        mod._fetch_q_stat_bucketed("strategy_pnl_1day_prod_v2", "FET", None)
        sql = cap[0]
        assert "updated_at DESC" in sql
        assert "LIMIT 1 BY strategy_table_name, ts" in sql
