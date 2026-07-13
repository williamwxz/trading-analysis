"""Unit tests for real_trade revision acceptance in minute_loop / pnl_formula.

Regression fixture: the FET 2026-07-13 incident. The external strategy service
re-published OLD bars late (bar ts=2026-07-12 20:00 got a fresh revision at
revision_ts=2026-07-13 04:14:12). The batch acceptance guard must reject that
stale re-revision — exactly like AnchorState.should_apply_revision in the live
consumer — otherwise:
  - last_active_minute() collapses to before the repair window (silent 0-row
    writes after the DELETE → data loss), and
  - with a wider window the stale old-bar position overwrites correct live rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from libs.computation.minute_loop import (
    active_rt_revision_at,
    build_rt_lookup,
    last_active_minute,
)
from libs.computation.pnl_formula import TIMEFRAME_MAP, compute_real_trade_pnl


def _dt(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _rev(
    ts: str,
    revision_ts: str,
    position: float,
    stn: str = "fet_strat_v3",
    tf: str = "1h",
) -> dict:
    """Build a revision dict shaped like fetch_new_bars_real_trade output."""
    tf_minutes = TIMEFRAME_MAP[tf]
    exec_dt = (_dt(revision_ts) + timedelta(seconds=59)).replace(second=0)
    closing_dt = _dt(ts) + timedelta(minutes=tf_minutes)
    return {
        "strategy_table_name": stn,
        "strategy_id": 1,
        "strategy_name": stn,
        "underlying": "FET",
        "config_timeframe": tf,
        "strategy_instance_id": f"{stn}|FET|{tf}",
        "weighting": 1.0,
        "ts": ts,
        "closing_ts": closing_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "execution_ts": exec_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "revision_ts": revision_ts,
        "position": position,
        "final_signal": position,
        "bar_benchmark": 0.0,
    }


def _fet_fixture() -> list[dict]:
    """Two genuine bars plus a stale late re-revision of the older bar.

    bar A  ts=07-12 20:00, revision 20:00:05 → execution 20:01
    bar B  ts=07-12 23:00, revision 07-13 00:12:30 → execution 00:13
    stale  ts=07-12 20:00 re-published at 07-13 04:14:12 → execution 04:15
    """
    return [
        _rev("2026-07-12 20:00:00", "2026-07-12 20:00:05", position=1.0),
        _rev("2026-07-12 23:00:00", "2026-07-13 00:12:30", position=-1.0),
        _rev("2026-07-12 20:00:00", "2026-07-13 04:14:12", position=1.0),
    ]


class TestBuildRtLookupStaleReRevision:
    def test_stale_re_revision_of_old_bar_is_rejected(self):
        lookup = build_rt_lookup(_fet_fixture())
        entries = lookup["fet_strat_v3"]
        revision_tss = [e.rev["revision_ts"] for e in entries]
        assert "2026-07-13 04:14:12" not in revision_tss
        assert len(entries) == 2

    def test_last_active_minute_covers_newest_bar(self):
        # Newest genuine bar B: closing 07-13 00:00 + 60m hold = 01:00 —
        # NOT 07-12 22:00 (the stale entry's bar-A-derived tail).
        lookup = build_rt_lookup(_fet_fixture())
        assert last_active_minute(lookup, is_rt=True) == _dt("2026-07-13 01:00:00")

    def test_repair_window_minute_resolves_to_newest_bar(self):
        lookup = build_rt_lookup(_fet_fixture())
        minute = _dt("2026-07-13 00:25:00")
        entry = active_rt_revision_at(lookup, "fet_strat_v3", minute)
        assert entry is not None
        assert entry.rev["ts"] == "2026-07-12 23:00:00"
        assert entry.rev["position"] == -1.0
        assert entry.next_execution_ts == _dt("2026-07-13 01:00:00")

    def test_wide_window_not_overwritten_by_stale_old_bar(self):
        # At 04:20 (after the stale re-revision's execution_ts) the active
        # entry must NOT be the re-published bar A.
        lookup = build_rt_lookup(_fet_fixture())
        minute = _dt("2026-07-13 04:20:00")
        entry = active_rt_revision_at(lookup, "fet_strat_v3", minute)
        assert entry is None or entry.rev["ts"] != "2026-07-12 20:00:00"


class TestBuildRtLookupNormalSequences:
    def test_same_bar_newer_revision_accepted(self):
        revs = [
            _rev("2026-07-12 20:00:00", "2026-07-12 20:00:05", position=1.0),
            _rev("2026-07-12 20:00:00", "2026-07-12 20:03:05", position=0.5),
        ]
        lookup = build_rt_lookup(revs)
        entries = lookup["fet_strat_v3"]
        assert [e.rev["position"] for e in entries] == [1.0, 0.5]

    def test_exact_duplicate_discarded(self):
        revs = [
            _rev("2026-07-12 20:00:00", "2026-07-12 20:00:05", position=1.0),
            _rev("2026-07-12 20:00:00", "2026-07-12 20:00:05", position=1.0),
        ]
        lookup = build_rt_lookup(revs)
        assert len(lookup["fet_strat_v3"]) == 1

    def test_tail_hold_never_before_execution_ts(self):
        # A single very-late revision: guard accepts it (first ever), but its
        # closing_ts+tf (07-12 22:00) is before its own execution_ts (07-13
        # 04:15). The tail hold must not go backwards.
        revs = [_rev("2026-07-12 20:00:00", "2026-07-13 04:14:12", position=1.0)]
        lookup = build_rt_lookup(revs)
        entry = lookup["fet_strat_v3"][0]
        assert entry.next_execution_ts >= entry.execution_ts


class TestComputeRealTradePnlStaleReRevision:
    def test_stale_re_revision_not_expanded(self):
        bars = _fet_fixture()
        prices = {}
        cur = _dt("2026-07-12 20:00:00")
        while cur <= _dt("2026-07-13 06:00:00"):
            prices[cur.strftime("%Y-%m-%d %H:%M:%S")] = 100.0
            cur += timedelta(minutes=1)
        rows = compute_real_trade_pnl(bars, {"fet_strat_v3": (0.0, 100.0, 1.0)}, prices)
        assert rows, "expected rows for genuine revisions"
        # ts at index 7, position at index 10 of INSERT_COLUMNS
        by_ts = {r[7]: r for r in rows}
        # Repair-window minute is covered by bar B's position.
        assert by_ts["2026-07-13 00:25:00"][10] == -1.0
        # No minute at/after the stale execution_ts carries bar A's position.
        stale_rows = [
            r for r in rows if r[7] >= "2026-07-13 04:15:00" and r[10] == 1.0
        ]
        assert stale_rows == []
        # Expansion ends at the genuine tail (bar B closing 00:00 + 60m).
        assert max(by_ts) == "2026-07-13 00:59:00"


class TestZeroRowWarning:
    def test_zero_row_strategies_logged_as_error(self, caplog):
        from scripts.audit_pnl import StrategyFix, report_zero_row_strategies

        fixes = [
            StrategyFix(
                type="real_trade",
                underlying="FET",
                strategy_table_name="ok_strat",
                failure_ts=_dt("2026-07-13 00:25:00"),
                fix_applied=True,
                rows_written=3,
            ),
            StrategyFix(
                type="real_trade",
                underlying="FET",
                strategy_table_name="broken_strat",
                failure_ts=_dt("2026-07-13 00:25:00"),
                fix_applied=True,
                rows_written=0,
            ),
        ]
        with caplog.at_level("ERROR"):
            zero = report_zero_row_strategies("real_trade", "FET", fixes)
        assert zero == ["broken_strat"]
        assert any("0 rows" in r.message for r in caplog.records)

    def test_no_error_when_all_strategies_wrote_rows(self, caplog):
        from scripts.audit_pnl import StrategyFix, report_zero_row_strategies

        fixes = [
            StrategyFix(
                type="real_trade",
                underlying="FET",
                strategy_table_name="ok_strat",
                failure_ts=_dt("2026-07-13 00:25:00"),
                fix_applied=True,
                rows_written=3,
            ),
        ]
        with caplog.at_level("ERROR"):
            zero = report_zero_row_strategies("real_trade", "FET", fixes)
        assert zero == []
        assert not caplog.records
