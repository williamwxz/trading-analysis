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
            find_bt_source_drift,
            find_midgap,
            find_missing_or_start_gap,
            find_stale_end,
        )

        assert callable(find_missing_or_start_gap)
        assert callable(find_midgap)
        assert callable(find_stale_end)
        assert callable(audit_positions)
        assert callable(audit_hour_sync)
        assert callable(find_bt_source_drift)


class TestDiagnosticCategorySkippedByFix:
    """bt_source_drift violations must not feed into the fix path."""

    def test_resolve_strategy_fix_skipped_for_drift_only_group(self):
        from scripts.audit_pnl import _DIAGNOSTIC_CATEGORIES

        assert "bt_source_drift" in _DIAGNOSTIC_CATEGORIES

    def test_fix_path_filter_drops_diagnostic_violations(self):
        from scripts.audit_pnl import _DIAGNOSTIC_CATEGORIES, Violation

        viols = [
            Violation(
                type="bt", underlying="BTC", strategy_table_name="(aggregate)",
                category="bt_source_drift", detail="42 strats drift",
                failure_ts=_dt("2026-06-02 22:04:00"),
            ),
            Violation(
                type="bt", underlying="BTC", strategy_table_name="sid=1|sno=1",
                category="position_mismatch", detail="x",
                failure_ts=_dt("2026-06-02 22:04:00"),
            ),
        ]
        fixable = [v for v in viols if v.category not in _DIAGNOSTIC_CATEGORIES]
        assert len(fixable) == 1
        assert fixable[0].category == "position_mismatch"


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
