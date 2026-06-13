"""Unit tests for audit_pnl pure-Python helpers (no ClickHouse needed)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from scripts.audit_pnl import (
    AuditReport,
    Category,
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

    def test_mixed_strategies_raises(self):
        from scripts.audit_pnl import resolve_strategy_fix

        v1 = _v("start_gap")
        v2 = _v("start_gap")
        v2.strategy_table_name = "different_strat"
        with pytest.raises(ValueError, match="mixed-strategy"):
            resolve_strategy_fix([v1, v2])


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


class TestValidateUnderlying:
    def test_valid_underlying_uppercased(self):
        from scripts.audit_pnl import _validate_underlying

        assert _validate_underlying("btc") == "BTC"
        assert _validate_underlying("ETHUSDT") == "ETHUSDT"

    def test_none_passes_through(self):
        from scripts.audit_pnl import _validate_underlying

        assert _validate_underlying(None) is None

    def test_invalid_raises(self):
        import pytest

        from scripts.audit_pnl import _validate_underlying

        for bad in ["BTC'", "BTC OR 1=1", "btc-usd", "", " "]:
            with pytest.raises(ValueError):
                _validate_underlying(bad)


class TestQuoteEscape:
    def test_basic_escape(self):
        from scripts.audit_pnl import _q

        assert _q("foo") == "foo"
        assert _q("foo's") == "foo''s"
        assert _q("'") == "''"


class TestBarFetchLookback:
    """Regression: a 1440-min (24h) lookback silently dropped 1d-tf strategies
    when their active bar's ts fell more than 24h before `failure_ts`. The
    lookback must be at least 2 × max(timeframe) so any active bar at the
    window boundary is included.

    Concrete failure mode (observed 2026-06-09): 79 BTC/ETH/SOL 1d-tf
    strategies missing rows for ~14h after `failure_ts=06/07 10:00` because
    the bar at `ts=06/06 00:00` (active 06/07 00:00 → 06/08 00:00) was
    excluded by `bar_fetch_start = failure_ts - 1440min = 06/06 10:00`.
    """

    def test_lookback_covers_largest_timeframe_with_margin(self):
        from libs.computation.pnl_formula import TIMEFRAME_MAP

        from scripts.audit_pnl import _BAR_FETCH_LOOKBACK_MINUTES

        max_tf = max(TIMEFRAME_MAP.values())  # 1440 (1d) at time of writing
        # 2× covers the worst case: bar at ts = failure_ts - max_tf - 1min
        # (whose execution_ts is just before failure_ts, still active at it).
        assert _BAR_FETCH_LOOKBACK_MINUTES >= 2 * max_tf, (
            f"lookback {_BAR_FETCH_LOOKBACK_MINUTES} must be >= 2 × max tf "
            f"({2 * max_tf}); see regression note in this class"
        )

    def test_1d_bar_at_failure_minus_25h_is_inside_lookback_window(self):
        """The exact scenario from the production incident."""
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        from scripts.audit_pnl import _BAR_FETCH_LOOKBACK_MINUTES

        failure_ts = _dt(2026, 6, 7, 10, 0, 0)
        bar_ts = _dt(
            2026, 6, 6, 0, 0, 0
        )  # exec_ts = 06/07 00:00, active through 06/08 00:00
        bar_fetch_start = failure_ts - _td(minutes=_BAR_FETCH_LOOKBACK_MINUTES)
        assert bar_ts >= bar_fetch_start, (
            f"1d bar at {bar_ts} must be >= bar_fetch_start "
            f"({bar_fetch_start}); was failing with 1440-min lookback"
        )


class TestGroupViolationsByStrategy:
    def test_empty_input_returns_empty_dict(self):
        from scripts.audit_pnl import group_violations_by_strategy

        assert group_violations_by_strategy([]) == {}

    def test_single_violation_one_key(self):
        from scripts.audit_pnl import group_violations_by_strategy

        v = _v("start_gap")
        result = group_violations_by_strategy([v])
        assert list(result.keys()) == [("prod", "BTC", "my_strat_v3")]
        assert result[("prod", "BTC", "my_strat_v3")] == [v]

    def test_same_key_grouped_together(self):
        from scripts.audit_pnl import group_violations_by_strategy

        v1 = _v("start_gap", ts="2026-05-25 06:00:00")
        v2 = _v("mid_gap", ts="2026-05-26 06:00:00")
        result = group_violations_by_strategy([v1, v2])
        assert len(result) == 1
        assert result[("prod", "BTC", "my_strat_v3")] == [v1, v2]

    def test_different_keys_separated(self):
        from scripts.audit_pnl import group_violations_by_strategy

        v1 = _v("start_gap")
        v2 = _v("start_gap")
        v2.strategy_table_name = "alpha_v1"
        result = group_violations_by_strategy([v1, v2])
        assert len(result) == 2
        assert ("prod", "BTC", "my_strat_v3") in result
        assert ("prod", "BTC", "alpha_v1") in result


class TestSeedAnchor:
    """fetch_seed_anchor is a thin SQL wrapper; we test that it returns a
    zero anchor when the query returns no rows, and parses fields correctly
    when it does."""

    def test_zero_anchor_when_no_prior_row(self):
        from scripts.audit_pnl import fetch_seed_anchor

        class FakeClient:
            def query(self, _sql):
                class R:
                    result_rows = []

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
                class R:
                    result_rows = [(123.45, 50000.0, 0.5)]

                return R()

        seed = fetch_seed_anchor(
            type_="prod",
            strategy_table_name="alpha",
            failure_ts=_dt("2026-05-29 12:00:00"),
            client=FakeClient(),
        )
        assert seed == {"pnl": 123.45, "price": 50000.0, "position": 0.5}


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
        report.by_type["prod"] = TypeSummary(
            type="prod", strategies_checked=12, violations=2
        )
        report.violations.append(
            Violation(
                type="prod",
                underlying="BTC",
                strategy_table_name="my_strat_v3",
                category="mid_gap",
                detail="4320m missing",
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
        assert rec["violations"][0]["categories"] == ["mid_gap"]

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


class TestRenderReport:
    def _sample_report(self) -> AuditReport:
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
            "real_trade": TypeSummary(
                type="real_trade", strategies_checked=45, violations=0
            ),
        }
        report.violations.append(
            Violation(
                type="prod",
                underlying="BTC",
                strategy_table_name="my_strat_v3",
                category="mid_gap",
                detail="4320m missing from 2026-05-25 06:00",
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

        r = AuditReport(
            started_at=_dt("2026-05-30 14:30:00"),
            finished_at=_dt("2026-05-30 14:30:05"),
            scope_types=["prod"],
            run_id="x",
        )
        r.by_type["prod"] = TypeSummary(
            type="prod", strategies_checked=10, violations=0
        )
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


class TestGetClientMemoryCaps:
    """get_client must cap per-query memory + enable disk-spill so a window
    recompute over the bt tables cannot OOM ClickHouse Cloud. The fix_bt_strategies
    worker threads resolve this same module-level get_client, so the caps reach
    them too."""

    def test_get_client_passes_memory_cap_settings(self, monkeypatch):
        import clickhouse_connect

        captured = {}

        def fake_get_client(**kwargs):
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(clickhouse_connect, "get_client", fake_get_client)
        for k in ("CLICKHOUSE_HOST", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"):
            monkeypatch.setenv(k, "x")

        from scripts.audit_pnl import _DEFAULT_QUERY_SETTINGS, get_client

        get_client()
        settings = captured.get("settings")
        assert settings is not None
        assert (
            settings["max_memory_usage"] == _DEFAULT_QUERY_SETTINGS["max_memory_usage"]
        )
        assert 0 < settings["max_memory_usage"]
        # spill thresholds sit below the per-query cap so spilling engages first
        assert (
            settings["max_bytes_before_external_group_by"]
            < settings["max_memory_usage"]
        )
        assert settings["max_bytes_before_external_sort"] < settings["max_memory_usage"]


class _CmdRecorder:
    """Minimal client stub that records command() SQL."""

    def __init__(self):
        self.commands: list[str] = []

    def command(self, sql: str):
        self.commands.append(sql)


class TestRefreshDayTable:
    def test_rebuilds_day_from_hour_with_delete_then_insert(self):
        from scripts.audit_pnl import refresh_day_table

        client = _CmdRecorder()
        refresh_day_table("bt", _dt("2026-06-07 12:30:00"), client, dry_run=False)

        assert len(client.commands) == 2
        delete_sql, insert_sql = client.commands
        # DELETE scoped to whole days >= the window start, on the 1day table
        assert "DELETE FROM analytics.strategy_pnl_1day_bt_v2" in delete_sql
        assert "toStartOfDay(toDateTime('2026-06-07 12:30:00'))" in delete_sql
        # INSERT rebuilds the 1day table FROM the 1hour table, day-bucketed argMax
        assert "INSERT INTO analytics.strategy_pnl_1day_bt_v2" in insert_sql
        assert "analytics.strategy_pnl_1hour_bt_v2" in insert_sql
        assert "toStartOfDay(hour_ts)" in insert_sql
        assert "argMax(cumulative_pnl, hour_ts)" in insert_sql

    def test_dry_run_issues_no_commands(self):
        from scripts.audit_pnl import refresh_day_table

        client = _CmdRecorder()
        refresh_day_table("prod", _dt("2026-06-11 00:00:00"), client, dry_run=True)
        assert client.commands == []

    def test_underlying_filter_scopes_both_statements(self):
        from scripts.audit_pnl import refresh_day_table

        client = _CmdRecorder()
        refresh_day_table(
            "real_trade",
            _dt("2026-06-11 00:00:00"),
            client,
            dry_run=False,
            underlying="BTC",
        )
        for sql in client.commands:
            assert "underlying = 'BTC'" in sql
