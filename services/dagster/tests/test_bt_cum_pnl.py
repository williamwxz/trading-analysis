"""Unit tests for the cum-table-sourced BT computation."""

from libs.computation.pnl_formula import (
    BtAnchor,
    compute_bt_live_cpnl,
    compute_bt_pnl,
    parse_strategy_table_name,
)

STN = "sid=1|sno=2|u=BTC|name=test|inst=abc"


def _anchor(ts, cum_pnl, pos, tf="1h", w=1.0):
    return BtAnchor(STN, tf, ts, cum_pnl, pos, w)


class TestComputeBtLiveCpnl:
    def test_reset_from_anchor(self):
        assert abs(compute_bt_live_cpnl(0.10, 1.0, 110.0, 100.0) - 0.20) < 1e-9

    def test_zero_anchor_price_holds_pnl(self):
        assert compute_bt_live_cpnl(0.10, 1.0, 110.0, 0.0) == 0.10

    def test_short_position(self):
        assert abs(compute_bt_live_cpnl(0.10, -1.0, 110.0, 100.0)) < 1e-9


class TestComputeBtPnl:
    def test_single_1h_anchor_expands_minutes(self):
        anchors = [_anchor("2024-01-01 00:00:00", 0.0, 1.0)]
        prices = {f"2024-01-01 00:{m:02d}:00": 100.0 + m for m in range(0, 5)}
        rows = compute_bt_pnl(
            anchors,
            prices,
            benchmarks={},
            window_start="2024-01-01 00:00:00",
            window_end="2024-01-01 00:05:00",
        )
        assert len(rows) == 5
        assert rows[0][7] == "2024-01-01 00:00:00"
        assert rows[0][5] == "backtest"
        assert rows[0][8] == 0.0
        assert abs(rows[4][8] - 0.04) < 1e-9
        assert rows[0][1] == 1 and rows[0][3] == "BTC"
        assert len(rows[0]) == 16

    def test_reset_snaps_to_each_anchor(self):
        anchors = [
            _anchor("2024-01-01 00:00:00", 0.0, 1.0),
            _anchor("2024-01-01 00:03:00", 0.50, -1.0),
        ]
        prices = {f"2024-01-01 00:{m:02d}:00": 100.0 for m in range(0, 6)}
        rows = compute_bt_pnl(
            anchors,
            prices,
            benchmarks={},
            window_start="2024-01-01 00:00:00",
            window_end="2024-01-01 00:06:00",
        )
        assert all(abs(r[8] - 0.0) < 1e-9 for r in rows[:3])
        assert all(abs(r[8] - 0.50) < 1e-9 for r in rows[3:])
        assert rows[3][10] == -1.0

    def test_straddling_anchor_seeds_window_start(self):
        anchors = [_anchor("2024-01-01 00:00:00", 0.0, 1.0)]
        prices = {f"2024-01-01 00:{m:02d}:00": 100.0 + m for m in range(0, 5)}
        rows = compute_bt_pnl(
            anchors,
            prices,
            benchmarks={},
            window_start="2024-01-01 00:02:00",
            window_end="2024-01-01 00:05:00",
        )
        assert len(rows) == 3
        assert rows[0][7] == "2024-01-01 00:02:00"
        assert abs(rows[0][8] - 0.02) < 1e-9

    def test_benchmark_joined_on_anchor_ts(self):
        anchors = [_anchor("2024-01-01 00:00:00", 0.0, 0.0)]
        prices = {"2024-01-01 00:00:00": 100.0}
        benchmarks = {(STN, "2024-01-01 00:00:00"): 0.123}
        rows = compute_bt_pnl(
            anchors,
            prices,
            benchmarks,
            window_start="2024-01-01 00:00:00",
            window_end="2024-01-01 00:01:00",
        )
        assert abs(rows[0][9] - 0.123) < 1e-9
        assert rows[0][12] == 0.0

    def test_missing_minute_price_skips_minute(self):
        anchors = [_anchor("2024-01-01 00:00:00", 0.0, 1.0)]
        prices = {"2024-01-01 00:00:00": 100.0, "2024-01-01 00:02:00": 102.0}
        rows = compute_bt_pnl(
            anchors,
            prices,
            benchmarks={},
            window_start="2024-01-01 00:00:00",
            window_end="2024-01-01 00:03:00",
        )
        assert [r[7] for r in rows] == ["2024-01-01 00:00:00", "2024-01-01 00:02:00"]


class TestParseStrategyTableName:
    def test_parses_all_fields(self):
        stn = "sid=18|sno=641|u=SOL|name=SOL_10Dskew_d_atm_iv|inst=a0273b14"
        strategy_id, name, underlying, siid = parse_strategy_table_name(stn)
        assert strategy_id == 18
        assert name == "SOL_10Dskew_d_atm_iv"
        assert underlying == "SOL"
        assert siid == "a0273b14"

    def test_missing_or_bad_sid_defaults_zero(self):
        strategy_id, name, underlying, siid = parse_strategy_table_name("u=BTC|inst=x")
        assert strategy_id == 0
        assert name == ""
        assert underlying == "BTC"
        assert siid == "x"
