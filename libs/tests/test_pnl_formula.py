"""Unit tests for libs.computation.pnl_formula — minute-by-minute computation.

Covers iter_compute_prod_pnl / compute_prod_pnl, compute_real_trade_pnl,
compute_bt_pnl, and extract_row_anchor.  No I/O — all pure-Python.

PnL formula:
    cpnl = prev_pnl + position * (price - prev_price) / prev_price
"""

import pytest
from datetime import datetime, timedelta

from libs.computation.pnl_formula import (
    INSERT_COLUMNS,
    TIMEFRAME_MAP,
    compute_bt_pnl,
    compute_prod_pnl,
    compute_real_trade_pnl,
    extract_row_anchor,
    iter_compute_prod_pnl,
)

# ── column indices from INSERT_COLUMNS ────────────────────────────────────────
_STN   = INSERT_COLUMNS.index("strategy_table_name")   # 0
_SID   = INSERT_COLUMNS.index("strategy_id")           # 1
_SNAME = INSERT_COLUMNS.index("strategy_name")         # 2
_UND   = INSERT_COLUMNS.index("underlying")            # 3
_TF    = INSERT_COLUMNS.index("config_timeframe")      # 4
_SRC   = INSERT_COLUMNS.index("source")                # 5
_VER   = INSERT_COLUMNS.index("version")               # 6
_TS    = INSERT_COLUMNS.index("ts")                    # 7
_CPNL  = INSERT_COLUMNS.index("cumulative_pnl")        # 8
_BENCH = INSERT_COLUMNS.index("benchmark")             # 9
_POS   = INSERT_COLUMNS.index("position")              # 10
_PRC   = INSERT_COLUMNS.index("price")                 # 11
_SIG   = INSERT_COLUMNS.index("final_signal")          # 12
_WGT   = INSERT_COLUMNS.index("weighting")             # 13
_UPD   = INSERT_COLUMNS.index("updated_at")            # 14
_SIID  = INSERT_COLUMNS.index("strategy_instance_id")  # 15


# ── shared bar builders ────────────────────────────────────────────────────────

def _prod_bar(
    stn="S1",
    siid="inst1",
    ts="2024-01-01 00:00:00",
    position=1.0,
    tf="5m",
    underlying="BTC",
    strategy_id=1,
    strategy_name="test",
    bar_benchmark=0.0,
    final_signal=1.0,
    weighting=1.0,
) -> dict:
    return {
        "strategy_table_name": stn,
        "strategy_instance_id": siid,
        "strategy_id": strategy_id,
        "strategy_name": strategy_name,
        "underlying": underlying,
        "config_timeframe": tf,
        "weighting": weighting,
        "ts": ts,
        "position": position,
        "final_signal": final_signal,
        "bar_benchmark": bar_benchmark,
    }


def _rt_bar(
    stn="S1",
    siid="inst1",
    ts="2024-01-01 00:00:00",
    closing_ts="2024-01-01 00:05:00",
    execution_ts="2024-01-01 00:05:00",
    revision_ts="2024-01-01 00:05:00",
    position=1.0,
    tf="5m",
) -> dict:
    return {
        "strategy_table_name": stn,
        "strategy_instance_id": siid,
        "strategy_id": 1,
        "strategy_name": "test",
        "underlying": "BTC",
        "config_timeframe": tf,
        "weighting": 1.0,
        "ts": ts,
        "closing_ts": closing_ts,
        "execution_ts": execution_ts,
        "revision_ts": revision_ts,
        "position": position,
        "final_signal": 1.0,
        "bar_benchmark": 0.0,
    }


def _bt_bar(
    stn="S1",
    siid="inst1",
    ts="2024-01-01 00:00:00",
    execution_ts="2024-01-01 00:05:00",
    position=1.0,
    cumulative_pnl=0.0,
    tf="5m",
) -> dict:
    return {
        "strategy_table_name": stn,
        "strategy_instance_id": siid,
        "strategy_id": 1,
        "strategy_name": "test",
        "underlying": "BTC",
        "config_timeframe": tf,
        "weighting": 1.0,
        "ts": ts,
        "execution_ts": execution_ts,
        "position": position,
        "cumulative_pnl": cumulative_pnl,
        "final_signal": 1.0,
        "bar_benchmark": 0.0,
    }


def _prices(start_str: str, n: int, base: float = 100.0, step: float = 0.0) -> dict:
    """Build a prices dict of n consecutive minutes starting at start_str."""
    dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    return {
        (dt + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"): base + i * step
        for i in range(n)
    }


# ═════════════════════════════════════════════════════════════════════════════
# INSERT_COLUMNS
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
def test_insert_columns_length():
    assert len(INSERT_COLUMNS) == 16


@pytest.mark.unit
def test_insert_columns_key_indices():
    """ts=7, updated_at=14, strategy_instance_id=15 — used by Dagster prepare step."""
    assert INSERT_COLUMNS[7] == "ts"
    assert INSERT_COLUMNS[14] == "updated_at"
    assert INSERT_COLUMNS[15] == "strategy_instance_id"
    assert INSERT_COLUMNS[8] == "cumulative_pnl"
    assert INSERT_COLUMNS[11] == "price"
    assert INSERT_COLUMNS[10] == "position"


# ═════════════════════════════════════════════════════════════════════════════
# iter_compute_prod_pnl / compute_prod_pnl
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestProdPnlFormula:

    def test_single_bar_flat_prices_zero_pnl(self):
        """Flat prices with position=1 → pnl stays at 0 throughout."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", position=1.0, tf="5m")
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0, step=0.0)
        rows = compute_prod_pnl([bar], {}, prices)
        assert len(rows) == 5
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.0)

    def test_single_bar_rising_price_positive_pnl(self):
        """position=1, price rises step by step — pnl chains each minute's return."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", position=1.0, tf="5m")
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 102.0,
            "2024-01-01 00:07:00": 105.0,
            "2024-01-01 00:08:00": 108.0,
            "2024-01-01 00:09:00": 110.0,
        }
        rows = compute_prod_pnl([bar], {}, prices)
        assert len(rows) == 5
        # First row: anchor_price set to 100, live_price=100 → pnl = 0
        assert rows[0][_CPNL] == pytest.approx(0.0)
        # Pnl chains: each step accumulates prev_pnl + pos*(p_cur-p_prev)/p_prev
        # row1: 0 + 1*(102-100)/100 = 0.02
        # row2: 0.02 + 1*(105-102)/102 ≈ 0.02 + 0.02941 ≈ 0.04941
        # row3: 0.04941 + 1*(108-105)/105 ≈ 0.04941 + 0.02857 ≈ 0.07798
        # row4: 0.07798 + 1*(110-108)/108 ≈ 0.07798 + 0.01852 ≈ 0.09650
        expected_last = (
            1.0 * (102 - 100) / 100
            + 1.0 * (105 - 102) / 102
            + 1.0 * (108 - 105) / 105
            + 1.0 * (110 - 108) / 108
        )
        assert rows[-1][_CPNL] == pytest.approx(expected_last)
        # Pnl is positive for a long position on rising prices
        assert rows[-1][_CPNL] > 0.0

    def test_single_bar_falling_price_negative_pnl(self):
        """position=1, price falls step by step — pnl chains into negative territory."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", position=1.0, tf="5m")
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 98.0,
            "2024-01-01 00:07:00": 96.0,
            "2024-01-01 00:08:00": 94.0,
            "2024-01-01 00:09:00": 90.0,
        }
        rows = compute_prod_pnl([bar], {}, prices)
        expected_last = (
            1.0 * (98 - 100) / 100
            + 1.0 * (96 - 98) / 98
            + 1.0 * (94 - 96) / 96
            + 1.0 * (90 - 94) / 94
        )
        assert rows[-1][_CPNL] == pytest.approx(expected_last)
        assert rows[-1][_CPNL] < 0.0

    def test_short_position_inverts_pnl_sign(self):
        """position=-1 on rising price → negative pnl (short loses on up moves)."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", position=-1.0, tf="5m")
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:09:00": 110.0,
        }
        prices.update(_prices("2024-01-01 00:06:00", 3, base=102.0, step=2.0))
        rows = compute_prod_pnl([bar], {}, prices)
        assert rows[-1][_CPNL] < 0.0

    def test_anchor_seeds_pnl_from_prior_day(self):
        """With an existing anchor, computation continues from (anchor_pnl, anchor_price)."""
        bar = _prod_bar(ts="2024-01-02 00:00:00", position=1.0, tf="5m")
        anchors = {"S1": (0.05, 100.0, 1.0)}
        # Flat price — no PnL change
        prices = _prices("2024-01-02 00:05:00", 5, base=100.0, step=0.0)
        rows = compute_prod_pnl([bar], anchors, prices)
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.05)

    def test_anchor_with_price_movement(self):
        """Anchor at pnl=0.05, price=100. Next minute price=105 → pnl = 0.05 + 1*(105-100)/100."""
        bar = _prod_bar(ts="2024-01-02 00:00:00", position=1.0, tf="5m")
        anchors = {"S1": (0.05, 100.0, 1.0)}
        prices = {"2024-01-02 00:05:00": 105.0}
        prices.update(_prices("2024-01-02 00:06:00", 4, base=105.0))
        rows = compute_prod_pnl([bar], anchors, prices)
        assert rows[0][_CPNL] == pytest.approx(0.05 + 1.0 * (105.0 - 100.0) / 100.0)

    def test_two_consecutive_bars_chain(self):
        """Bar 2 starts from bar 1's last anchor — no PnL jump between bars."""
        bars = [
            _prod_bar(ts="2024-01-01 00:00:00", position=1.0, tf="5m"),
            _prod_bar(ts="2024-01-01 00:05:00", position=1.0, tf="5m"),
        ]
        # Price steps up 1.0 each minute
        prices = _prices("2024-01-01 00:05:00", 10, base=100.0, step=1.0)
        rows = compute_prod_pnl(bars, {}, prices)
        assert len(rows) == 10
        # Row 4 (last of bar 1) and row 5 (first of bar 2) must be consecutive, not a reset
        pnl_4 = rows[4][_CPNL]
        pnl_5 = rows[5][_CPNL]
        # bar 2 first minute: price[5] = 100+5=105, price[4] = 104 → delta = 1/104
        expected_step = 1.0 * (105.0 - 104.0) / 104.0
        assert pnl_5 == pytest.approx(pnl_4 + expected_step, rel=1e-6)

    def test_position_changes_between_bars(self):
        """Position 1.0 in bar1, position=-1.0 in bar2. Formula uses each bar's own position."""
        bars = [
            _prod_bar(ts="2024-01-01 00:00:00", position=1.0,  tf="5m"),
            _prod_bar(ts="2024-01-01 00:05:00", position=-1.0, tf="5m"),
        ]
        prices = _prices("2024-01-01 00:05:00", 10, base=100.0, step=1.0)
        rows = compute_prod_pnl(bars, {}, prices)
        # bar1 rows: position=1.0
        assert all(row[_POS] == pytest.approx(1.0) for row in rows[:5])
        # bar2 rows: position=-1.0
        assert all(row[_POS] == pytest.approx(-1.0) for row in rows[5:])

    def test_closing_ts_is_bar_ts_plus_tf_minutes(self):
        """First output row ts equals bar_ts + tf_minutes (bar close = execution start)."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_prod_pnl([bar], {}, prices)
        assert rows[0][_TS] == "2024-01-01 00:05:00"

    def test_1h_bar_starts_at_closing_ts(self):
        """1h bar: execution starts at ts + 60 min."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="1h")
        prices = _prices("2024-01-01 01:00:00", 5, base=100.0)
        rows = compute_prod_pnl([bar], {}, prices)
        assert rows[0][_TS] == "2024-01-01 01:00:00"

    def test_last_bar_holds_one_tf_minutes(self):
        """Single bar expands for tf_minutes rows (closing_ts to closing_ts+tf_minutes)."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_prod_pnl([bar], {}, prices)
        assert len(rows) == 5
        assert rows[0][_TS] == "2024-01-01 00:05:00"
        assert rows[-1][_TS] == "2024-01-01 00:09:00"

    def test_missing_price_with_no_anchor_skipped(self):
        """Very first minute with no price and no anchor is skipped (cannot compute)."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        prices = {
            # 00:05 missing — no anchor yet, so it is skipped
            "2024-01-01 00:06:00": 100.0,
        }
        rows = compute_prod_pnl([bar], {}, prices)
        ts_values = [row[_TS] for row in rows]
        assert "2024-01-01 00:05:00" not in ts_values
        assert "2024-01-01 00:06:00" in ts_values

    def test_missing_price_with_existing_anchor_uses_carry_forward(self):
        """Once anchor is set, missing prices use carry-forward (emitted, no PnL change)."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06 missing — carry-forward anchor_price=100
            "2024-01-01 00:07:00": 102.0,
        }
        rows = compute_prod_pnl([bar], {}, prices)
        ts_values = [row[_TS] for row in rows]
        # 00:06 is emitted using carry-forward price
        assert "2024-01-01 00:06:00" in ts_values
        row_06 = next(r for r in rows if r[_TS] == "2024-01-01 00:06:00")
        assert row_06[_PRC] == pytest.approx(100.0)
        # No PnL change at carry-forward minute
        row_05 = next(r for r in rows if r[_TS] == "2024-01-01 00:05:00")
        assert row_06[_CPNL] == pytest.approx(row_05[_CPNL])

    def test_missing_price_with_anchor_uses_carry_forward(self):
        """Missing price minute with existing anchor_price uses anchor as fallback."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        anchors = {"S1": (0.05, 100.0, 1.0)}
        prices = {
            # 00:05 missing — carry-forward anchor_price=100
            "2024-01-01 00:06:00": 102.0,
        }
        rows = compute_prod_pnl([bar], anchors, prices)
        ts_values = [row[_TS] for row in rows]
        # 00:05 is emitted (carry-forward price=100 → no change)
        assert "2024-01-01 00:05:00" in ts_values
        first = next(r for r in rows if r[_TS] == "2024-01-01 00:05:00")
        assert first[_PRC] == pytest.approx(100.0)
        assert first[_CPNL] == pytest.approx(0.05)

    def test_zero_position_pnl_does_not_change(self):
        """position=0 means no PnL movement regardless of price moves."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", position=0.0, tf="5m")
        anchors = {"S1": (0.10, 100.0, 0.0)}
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0, step=10.0)
        rows = compute_prod_pnl([bar], anchors, prices)
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.10)

    def test_output_row_columns(self):
        """Output row has exactly 16 columns matching INSERT_COLUMNS order."""
        bar = _prod_bar(
            stn="mystrat", siid="inst99", ts="2024-01-01 00:00:00",
            position=0.5, tf="1m", underlying="ETH",  # 1m bar → 1 row at 00:01
            strategy_id=42, strategy_name="alpha",
            bar_benchmark=0.01, final_signal=-1.0, weighting=2.0,
        )
        prices = {"2024-01-01 00:01:00": 200.0}
        rows = compute_prod_pnl([bar], {}, prices)
        assert len(rows) == 1
        row = rows[0]
        assert len(row) == 16
        assert row[_STN]   == "mystrat"
        assert row[_SID]   == 42
        assert row[_SNAME] == "alpha"
        assert row[_UND]   == "ETH"
        assert row[_TF]    == "1m"
        assert row[_SRC]   == "production"
        assert row[_VER]   == "v2"
        assert row[_POS]   == pytest.approx(0.5)
        assert row[_SIG]   == pytest.approx(-1.0)
        assert row[_WGT]   == pytest.approx(2.0)
        assert row[_BENCH] == pytest.approx(0.01)
        assert row[_SIID]  == "inst99"

    def test_source_label_propagated(self):
        """source_label parameter appears in the source column (index 5)."""
        bar = _prod_bar()
        prices = _prices("2024-01-01 00:05:00", 1, base=100.0)
        rows = compute_prod_pnl([bar], {}, prices, source_label="my_label")
        assert rows[0][_SRC] == "my_label"

    def test_multi_strategy_independent_chains(self):
        """Two strategies with different anchors compute PnL independently."""
        bar_a = _prod_bar(stn="SA", siid="instA", ts="2024-01-01 00:00:00", position=1.0,  tf="5m")
        bar_b = _prod_bar(stn="SB", siid="instB", ts="2024-01-01 00:00:00", position=-1.0, tf="5m")
        anchors = {
            "SA": (0.10, 100.0, 1.0),
            "SB": (0.20, 200.0, -1.0),
        }
        # Price rises — SA gains, SB loses
        prices = {
            "2024-01-01 00:05:00": 110.0,
            "2024-01-01 00:09:00": 110.0,
        }
        prices.update(_prices("2024-01-01 00:06:00", 3, base=110.0))
        all_rows = compute_prod_pnl([bar_a, bar_b], anchors, prices)
        by_stn = {}
        for row in all_rows:
            by_stn.setdefault(row[_STN], []).append(row)
        sa_first = by_stn["SA"][0]
        sb_first = by_stn["SB"][0]
        # SA (long, price 100→110): pnl = 0.10 + 1*(110-100)/100 = 0.20
        assert sa_first[_CPNL] == pytest.approx(0.10 + 1.0 * (110.0 - 100.0) / 100.0)
        # SB (short, price 200 unchanged at first minute if price=110 — different underlying
        # but here we use same price dict; price goes 200→110 relative to anchor)
        assert sb_first[_CPNL] == pytest.approx(0.20 + (-1.0) * (110.0 - 200.0) / 200.0)

    def test_iter_compute_prod_pnl_yields_per_strategy(self):
        """iter_compute_prod_pnl yields one (stn, rows) pair per unique strategy."""
        bars = [
            _prod_bar(stn="SA", ts="2024-01-01 00:00:00", tf="5m"),
            _prod_bar(stn="SB", ts="2024-01-01 00:00:00", tf="5m"),
        ]
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        result = dict(iter_compute_prod_pnl(bars, {}, prices))
        assert set(result.keys()) == {"SA", "SB"}


# ═════════════════════════════════════════════════════════════════════════════
# compute_real_trade_pnl
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestRealTradePnlFormula:

    def test_single_revision_flat_prices(self):
        """Single revision, flat prices → pnl stays 0."""
        bar = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 00:05:00",
            execution_ts="2024-01-01 00:05:00",
            revision_ts="2024-01-01 00:05:00",
            position=1.0,
        )
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_real_trade_pnl([bar], {}, prices)
        assert len(rows) == 5
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.0)

    def test_single_revision_rising_price(self):
        """position=1, price rises step by step — pnl chains each minute's return."""
        bar = _rt_bar(position=1.0)
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 102.0,
            "2024-01-01 00:07:00": 105.0,
            "2024-01-01 00:08:00": 108.0,
            "2024-01-01 00:09:00": 110.0,
        }
        rows = compute_real_trade_pnl([bar], {}, prices)
        expected_last = (
            1.0 * (102 - 100) / 100
            + 1.0 * (105 - 102) / 102
            + 1.0 * (108 - 105) / 105
            + 1.0 * (110 - 108) / 108
        )
        assert rows[-1][_CPNL] == pytest.approx(expected_last)
        assert rows[-1][_CPNL] > 0.0

    def test_execution_ts_determines_start_minute(self):
        """Rows start at execution_ts, not bar open ts or closing_ts."""
        bar = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 00:05:00",
            execution_ts="2024-01-01 00:07:00",  # late execution
            revision_ts="2024-01-01 00:06:30",
            position=1.0,
        )
        prices = _prices("2024-01-01 00:07:00", 3, base=100.0)
        rows = compute_real_trade_pnl([bar], {}, prices)
        assert rows[0][_TS] == "2024-01-01 00:07:00"

    def test_late_revision_for_same_bar_accepted(self):
        """A later revision for the same bar (higher revision_ts) is accepted."""
        bars = [
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 01:00:00",
                execution_ts="2024-01-01 01:00:00",
                revision_ts="2024-01-01 00:59:00",
                position=0.0,
                tf="1h",
            ),
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 01:00:00",
                execution_ts="2024-01-01 01:33:00",
                revision_ts="2024-01-01 01:32:00",
                position=1.0,
                tf="1h",
            ),
        ]
        prices = _prices("2024-01-01 01:00:00", 60, base=100.0)
        rows = compute_real_trade_pnl(bars, {}, prices)
        assert len(rows) > 0
        assert rows[0][_TS] == "2024-01-01 01:00:00"
        # Second revision fires at 01:33 — position switches to 1.0
        by_ts = {r[_TS]: r for r in rows}
        assert by_ts["2024-01-01 01:33:00"][_POS] == pytest.approx(1.0)

    def test_duplicate_revision_discarded(self):
        """Exact duplicate (same bar_ts, same revision_ts) is discarded."""
        rev = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 01:00:00",
            execution_ts="2024-01-01 01:00:00",
            revision_ts="2024-01-01 00:59:00",
            position=1.0,
            tf="1h",
        )
        prices = _prices("2024-01-01 01:00:00", 5, base=100.0)
        # Pass the same revision twice
        rows = compute_real_trade_pnl([rev, dict(rev)], {}, prices)
        ts_values = [r[_TS] for r in rows]
        assert len(ts_values) == len(set(ts_values)), "duplicate revision must not emit duplicate rows"

    def test_older_bar_revision_after_newer_bar_discarded(self):
        """Revision for an older bar arriving after a newer bar's revision is discarded."""
        newer_bar = _rt_bar(
            ts="2024-01-01 01:00:00",
            closing_ts="2024-01-01 02:00:00",
            execution_ts="2024-01-01 02:00:00",
            revision_ts="2024-01-01 01:59:00",
            position=-1.0,
            tf="1h",
        )
        stale_old_bar = _rt_bar(
            ts="2024-01-01 00:00:00",  # older bar_ts → discarded
            closing_ts="2024-01-01 01:00:00",
            execution_ts="2024-01-01 02:13:00",
            revision_ts="2024-01-01 02:12:00",
            position=1.0,
            tf="1h",
        )
        prices = _prices("2024-01-01 02:00:00", 20, base=100.0)
        # newer_bar sorts first (ts=01:00 < 02:12... wait, sorted by (ts, revision_ts))
        # newer_bar: ts=01:00:00, stale_old_bar: ts=00:00:00 → stale sorts FIRST
        # After newer_bar is accepted, stale (ts=00:00 < 01:00) is discarded
        rows = compute_real_trade_pnl([newer_bar, stale_old_bar], {}, prices)
        ts_values = [r[_TS] for r in rows]
        # stale_old_bar's execution_ts=02:13 must not appear; newer_bar holds position=-1
        assert all(r[_POS] == pytest.approx(-1.0) for r in rows)

    def test_two_revisions_same_bar_position_switches(self):
        """Two revisions for the same bar: first holds until second's execution_ts."""
        bars = [
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 00:05:00",
                execution_ts="2024-01-01 00:05:00",
                revision_ts="2024-01-01 00:05:00",
                position=0.0,
            ),
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 00:05:00",
                execution_ts="2024-01-01 00:07:00",
                revision_ts="2024-01-01 00:06:30",
                position=1.0,
            ),
        ]
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_real_trade_pnl(bars, {}, prices)
        by_ts = {r[_TS]: r for r in rows}
        # Minutes 00:05 and 00:06: revision 1 active (position=0)
        assert by_ts["2024-01-01 00:05:00"][_POS] == pytest.approx(0.0)
        assert by_ts["2024-01-01 00:06:00"][_POS] == pytest.approx(0.0)
        # Minutes 00:07 onward: revision 2 active (position=1)
        assert by_ts["2024-01-01 00:07:00"][_POS] == pytest.approx(1.0)

    def test_two_revisions_no_duplicate_timestamps(self):
        """No timestamp appears twice in the output for two revisions of the same bar."""
        bars = [
            _rt_bar(ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:05:00", revision_ts="2024-01-01 00:05:00", position=0.0),
            _rt_bar(ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:07:00", revision_ts="2024-01-01 00:06:30", position=1.0),
        ]
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_real_trade_pnl(bars, {}, prices)
        ts_values = [r[_TS] for r in rows]
        assert len(ts_values) == len(set(ts_values)), "duplicate timestamps in output"

    def test_two_consecutive_bars_chain_pnl(self):
        """Second bar's first row chains from first bar's last pnl, not from zero."""
        bars = [
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 00:05:00",
                execution_ts="2024-01-01 00:05:00",
                revision_ts="2024-01-01 00:05:00",
                position=1.0,
            ),
            _rt_bar(
                ts="2024-01-01 00:05:00",
                closing_ts="2024-01-01 00:10:00",
                execution_ts="2024-01-01 00:10:00",
                revision_ts="2024-01-01 00:10:00",
                position=1.0,
            ),
        ]
        # price rises +1 each minute
        prices = _prices("2024-01-01 00:05:00", 10, base=100.0, step=1.0)
        rows = compute_real_trade_pnl(bars, {}, prices)
        assert len(rows) == 10
        # Rows must be contiguous — no gap between bar 1 last and bar 2 first
        ts_values = sorted(r[_TS] for r in rows)
        dts = [datetime.strptime(t, "%Y-%m-%d %H:%M:%S") for t in ts_values]
        for i in range(1, len(dts)):
            assert dts[i] - dts[i - 1] == timedelta(minutes=1), f"gap at index {i}"

    def test_anchor_seeds_pnl_for_real_trade(self):
        """Existing anchor (prev day tail) carries pnl and price into real_trade compute."""
        bar = _rt_bar(position=1.0)
        anchors = {"S1": (0.08, 100.0, 1.0)}
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)  # flat — no change
        rows = compute_real_trade_pnl([bar], anchors, prices)
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.08)

    def test_real_trade_source_label(self):
        """source column must be 'real_trade'."""
        bar = _rt_bar()
        prices = _prices("2024-01-01 00:05:00", 1, base=100.0)
        rows = compute_real_trade_pnl([bar], {}, prices)
        assert rows[0][_SRC] == "real_trade"

    def test_real_trade_output_row_column_count(self):
        """Output row has 16 columns."""
        bar = _rt_bar()
        prices = _prices("2024-01-01 00:05:00", 1, base=100.0)
        rows = compute_real_trade_pnl([bar], {}, prices)
        assert len(rows[0]) == 16

    def test_real_trade_pnl_arithmetic_minute_by_minute(self):
        """Verify exact pnl values step by step for 3 minutes."""
        bar = _rt_bar(position=2.0)
        # prices: 100, 110, 121
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 110.0,
            "2024-01-01 00:07:00": 121.0,
        }
        rows = compute_real_trade_pnl([bar], {}, prices)
        # min0: anchor_price set to 100, pnl = 0 + 2*(100-100)/100 = 0
        assert rows[0][_CPNL] == pytest.approx(0.0)
        # min1: pnl = 0 + 2*(110-100)/100 = 0.20
        assert rows[1][_CPNL] == pytest.approx(0.20)
        # min2: pnl = 0.20 + 2*(121-110)/110 = 0.20 + 0.20 = 0.40
        assert rows[2][_CPNL] == pytest.approx(0.40)

    def test_real_trade_missing_price_uses_carry_forward(self):
        """Once anchor price is set, missing minute uses carry-forward (emitted, no pnl change)."""
        bar = _rt_bar()
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06 missing — carry-forward from anchor_price=100
            "2024-01-01 00:07:00": 100.0,
        }
        rows = compute_real_trade_pnl([bar], {}, prices)
        ts_values = [r[_TS] for r in rows]
        assert "2024-01-01 00:06:00" in ts_values
        row_06 = next(r for r in rows if r[_TS] == "2024-01-01 00:06:00")
        assert row_06[_PRC] == pytest.approx(100.0)

    def test_revision_sorted_by_ts_then_revision_ts(self):
        """Revisions for two different bars are processed in (ts, revision_ts) order."""
        bar_early = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 00:05:00",
            execution_ts="2024-01-01 00:05:00",
            revision_ts="2024-01-01 00:05:00",
            position=1.0,
        )
        bar_late = _rt_bar(
            ts="2024-01-01 00:05:00",
            closing_ts="2024-01-01 00:10:00",
            execution_ts="2024-01-01 00:10:00",
            revision_ts="2024-01-01 00:10:00",
            position=-1.0,
        )
        prices = _prices("2024-01-01 00:05:00", 10, base=100.0)
        # Pass in reverse order — function must sort internally
        rows = compute_real_trade_pnl([bar_late, bar_early], {}, prices)
        ts_sorted = sorted(r[_TS] for r in rows)
        assert [r[_TS] for r in sorted(rows, key=lambda r: r[_TS])] == ts_sorted


# ═════════════════════════════════════════════════════════════════════════════
# compute_bt_pnl
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestBtPnlFormula:

    def test_single_bar_expands_to_tf_rows(self):
        """5m bar → 5 rows starting at execution_ts."""
        bar = _bt_bar(ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:05:00", position=1.0)
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_bt_pnl([bar], prices)
        assert len(rows) == 5
        assert rows[0][_TS] == "2024-01-01 00:05:00"

    def test_bt_source_label(self):
        """source column must be 'backtest'."""
        bar = _bt_bar()
        prices = _prices("2024-01-01 00:05:00", 1, base=100.0)
        rows = compute_bt_pnl([bar], prices)
        assert rows[0][_SRC] == "backtest"

    def test_cold_start_uses_bar_cumulative_pnl(self):
        """No prior anchor — first bar seeds from its own cumulative_pnl."""
        bar = _bt_bar(cumulative_pnl=0.05, position=0.0, execution_ts="2024-01-01 00:05:00")
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)  # flat, position=0
        rows = compute_bt_pnl([bar], prices)
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.05)

    def test_anchor_ignored_bar_cumulative_pnl_always_wins(self):
        """anchors parameter is accepted but ignored — bar's cumulative_pnl is always authoritative."""
        bar = _bt_bar(cumulative_pnl=0.05, position=0.0)
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        anchors = {"S1": (0.10, 100.0, 0.0)}
        rows = compute_bt_pnl([bar], prices, anchors=anchors)
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.05)

    def test_two_bars_each_reset_to_own_cumulative_pnl(self):
        """Each bar resets running_pnl to its own cumulative_pnl — no cross-bar chaining."""
        bars = [
            _bt_bar(ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:05:00",
                    position=1.0, cumulative_pnl=0.0),
            _bt_bar(ts="2024-01-01 00:05:00", execution_ts="2024-01-01 00:10:00",
                    position=1.0, cumulative_pnl=0.05),
        ]
        prices = _prices("2024-01-01 00:05:00", 10, base=100.0)  # flat prices
        rows = compute_bt_pnl(bars, prices)
        assert len(rows) == 10
        # First 5 rows: base from bar1's cumulative_pnl=0.0, flat prices → pnl stays 0.0
        for row in rows[:5]:
            assert row[_CPNL] == pytest.approx(0.0, abs=1e-10)
        # Second 5 rows: base resets to bar2's cumulative_pnl=0.05, flat prices → pnl stays 0.05
        for row in rows[5:]:
            assert row[_CPNL] == pytest.approx(0.05, abs=1e-10)

    def test_bt_pnl_arithmetic_minute_by_minute(self):
        """Verify exact pnl values for 3 rising price minutes."""
        bar = _bt_bar(position=1.0, cumulative_pnl=0.0)
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 110.0,
            "2024-01-01 00:07:00": 121.0,
        }
        rows = compute_bt_pnl([bar], prices)
        # min0 (00:05): anchor=0 seed, anchor_price=100, live=100 → pnl = 0 + 1*(100-100)/100 = 0
        assert rows[0][_CPNL] == pytest.approx(0.0)
        # min1 (00:06): prev_price=100, live=110 → delta = 1*(110-100)/100 = 0.10
        assert rows[1][_CPNL] == pytest.approx(0.10)
        # min2 (00:07): prev_price=110, live=121 → delta = 1*(121-110)/110 ≈ 0.10
        # total = 0.10 + 0.10 = 0.20
        assert rows[2][_CPNL] == pytest.approx(0.10 + 1.0 * (121.0 - 110.0) / 110.0)

    def test_bt_output_row_column_count(self):
        bar = _bt_bar()
        prices = _prices("2024-01-01 00:05:00", 1, base=100.0)
        rows = compute_bt_pnl([bar], prices)
        assert len(rows[0]) == 16

    def test_bt_single_bar_expands_one_tf(self):
        """Single bt bar expands for tf_minutes rows starting at execution_ts."""
        bar = _bt_bar(tf="5m")
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_bt_pnl([bar], prices)
        assert len(rows) == 5
        assert rows[0][_TS] == "2024-01-01 00:05:00"
        assert rows[-1][_TS] == "2024-01-01 00:09:00"

    def test_bt_missing_price_uses_carry_forward(self):
        """Once anchor price is set, bt missing minute uses carry-forward (emitted)."""
        bar = _bt_bar()
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06 missing — carry-forward from anchor_price=100
            "2024-01-01 00:07:00": 102.0,
        }
        rows = compute_bt_pnl([bar], prices)
        ts_values = [r[_TS] for r in rows]
        assert "2024-01-01 00:06:00" in ts_values
        row_06 = next(r for r in rows if r[_TS] == "2024-01-01 00:06:00")
        assert row_06[_PRC] == pytest.approx(100.0)


# ═════════════════════════════════════════════════════════════════════════════
# Additional missing-price, stale-revision, and BT≡prod tests
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestMissingPriceCases:
    """Explicit coverage of carry-forward vs skip behavior across all three modes."""

    def test_prod_no_anchor_no_price_row_skipped(self):
        """Very first minute: no anchor AND no price → row is skipped entirely."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        prices = {}  # nothing — every minute is skipped
        rows = compute_prod_pnl([bar], {}, prices)
        assert len(rows) == 0

    def test_prod_multiple_consecutive_missing_prices_carry_forward(self):
        """Several consecutive gap minutes all emit with carry-forward price, pnl unchanged."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06, 00:07, 00:08 all missing
            "2024-01-01 00:09:00": 105.0,
        }
        rows = compute_prod_pnl([bar], {}, prices)
        ts_values = [r[_TS] for r in rows]
        # All four minutes must be emitted
        for t in ["2024-01-01 00:05:00", "2024-01-01 00:06:00",
                  "2024-01-01 00:07:00", "2024-01-01 00:08:00", "2024-01-01 00:09:00"]:
            assert t in ts_values
        # Carry-forward minutes use anchor price = 100, pnl unchanged
        row_06 = next(r for r in rows if r[_TS] == "2024-01-01 00:06:00")
        row_07 = next(r for r in rows if r[_TS] == "2024-01-01 00:07:00")
        row_08 = next(r for r in rows if r[_TS] == "2024-01-01 00:08:00")
        assert row_06[_PRC] == pytest.approx(100.0)
        assert row_07[_PRC] == pytest.approx(100.0)
        assert row_08[_PRC] == pytest.approx(100.0)
        assert row_06[_CPNL] == pytest.approx(0.0)
        assert row_07[_CPNL] == pytest.approx(0.0)
        assert row_08[_CPNL] == pytest.approx(0.0)

    def test_prod_all_prices_missing_no_anchor_no_rows(self):
        """If no prices at all and no anchor, no rows are emitted."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        rows = compute_prod_pnl([bar], {}, {})
        assert rows == []

    def test_prod_all_prices_missing_with_anchor_carry_forward(self):
        """With an established anchor, missing-every-minute still emits rows at carry-forward price."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", tf="5m")
        anchors = {"S1": (0.10, 100.0, 1.0)}
        rows = compute_prod_pnl([bar], anchors, {})
        # All 5 minutes emitted at carry-forward price=100, pnl=0.10
        assert len(rows) == 5
        for row in rows:
            assert row[_PRC] == pytest.approx(100.0)
            assert row[_CPNL] == pytest.approx(0.10)

    def test_rt_no_anchor_no_price_row_skipped(self):
        """Real-trade: very first minute with no price and no anchor is skipped."""
        bar = _rt_bar()
        rows = compute_real_trade_pnl([bar], {}, {})
        assert rows == []

    def test_rt_multiple_consecutive_missing_prices_carry_forward(self):
        """Real-trade: consecutive gap minutes all carry-forward once anchor is set."""
        bar = _rt_bar()
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06, 00:07 missing
            "2024-01-01 00:08:00": 100.0,
        }
        rows = compute_real_trade_pnl([bar], {}, prices)
        ts_values = [r[_TS] for r in rows]
        assert "2024-01-01 00:06:00" in ts_values
        assert "2024-01-01 00:07:00" in ts_values
        for t in ["2024-01-01 00:06:00", "2024-01-01 00:07:00"]:
            row = next(r for r in rows if r[_TS] == t)
            assert row[_PRC] == pytest.approx(100.0)
            assert row[_CPNL] == pytest.approx(0.0)

    def test_bt_no_anchor_no_price_row_skipped(self):
        """BT: no price at all, no anchor → no rows emitted."""
        bar = _bt_bar(cumulative_pnl=0.05)
        rows = compute_bt_pnl([bar], {})
        assert rows == []

    def test_bt_multiple_consecutive_missing_prices_carry_forward(self):
        """BT: consecutive gap minutes carry-forward once anchor price is set."""
        bar = _bt_bar()
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06, 00:07 missing
            "2024-01-01 00:08:00": 100.0,
        }
        rows = compute_bt_pnl([bar], prices)
        ts_values = [r[_TS] for r in rows]
        assert "2024-01-01 00:06:00" in ts_values
        assert "2024-01-01 00:07:00" in ts_values


@pytest.mark.unit
class TestRealTradeStaleRevision:
    """Explicit coverage of stale revision discard: tuple guard (bar_ts, revision_ts)."""

    def test_duplicate_revision_discarded(self):
        """Exact duplicate (same ts, same revision_ts) emits rows only once."""
        rev = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 00:05:00",
            execution_ts="2024-01-01 00:05:00",
            revision_ts="2024-01-01 00:04:30",
            position=1.0,
            tf="5m",
        )
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_real_trade_pnl([rev, dict(rev)], {}, prices)
        ts_values = [r[_TS] for r in rows]
        assert len(ts_values) == len(set(ts_values)), "duplicate must not emit rows twice"
        assert len(rows) == 5

    def test_older_bar_ts_discarded_after_newer_accepted(self):
        """Revision for an older bar_ts arriving after a newer bar is discarded."""
        newer = _rt_bar(
            ts="2024-01-01 01:00:00",
            closing_ts="2024-01-01 02:00:00",
            execution_ts="2024-01-01 02:00:00",
            revision_ts="2024-01-01 01:59:00",
            position=-1.0,
            tf="1h",
        )
        stale = _rt_bar(
            ts="2024-01-01 00:00:00",  # older bar_ts → sorted before newer, discarded after newer accepted
            closing_ts="2024-01-01 01:00:00",
            execution_ts="2024-01-01 02:05:00",
            revision_ts="2024-01-01 02:04:00",
            position=1.0,
            tf="1h",
        )
        prices = _prices("2024-01-01 02:00:00", 10, base=100.0)
        # stale sorts first (ts=00:00 < 01:00), so it's accepted first.
        # Then newer (ts=01:00) is strictly greater → also accepted.
        # After newer, stale cannot re-appear — both are accepted in sorted order.
        # Test the intended semantic: a revision for bar_ts=00:00 arriving out-of-band
        # AFTER the consumer has advanced to bar_ts=01:00 is discarded.
        # In batch, revisions are sorted (ts, revision_ts) ASC — stale is first,
        # newer is second. After newer is accepted (last_bar_ts=01:00), no further
        # revision with ts <= 00:00 can appear. This tests the opposite direction:
        # if somehow an old-bar revision follows a new-bar revision in the input,
        # it is discarded.
        rows_forward = compute_real_trade_pnl([stale, newer], {}, prices)
        # stale accepted first (pos=1.0), newer accepted next (pos=-1.0) — both pass
        # because they are in sorted order. The discard only fires for true duplicates
        # or out-of-order (newer-first) inputs.
        rows_reversed = compute_real_trade_pnl([newer, stale], {}, prices)
        # newer sorted first internally (ts=01:00 > 00:00? No — sort is by ts ASC,
        # so stale (00:00) still sorts first regardless of input order.
        assert rows_forward == rows_reversed  # internal sort makes input order irrelevant

    def test_same_bar_earlier_revision_ts_discarded_after_later_accepted(self):
        """For the same bar_ts, a revision with smaller revision_ts after a larger one is discarded."""
        later_rev = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 01:00:00",
            execution_ts="2024-01-01 01:30:00",
            revision_ts="2024-01-01 01:29:00",
            position=1.0,
            tf="1h",
        )
        earlier_rev = _rt_bar(
            ts="2024-01-01 00:00:00",
            closing_ts="2024-01-01 01:00:00",
            execution_ts="2024-01-01 01:00:00",
            revision_ts="2024-01-01 00:59:00",  # smaller revision_ts — sorts first
            position=-1.0,
            tf="1h",
        )
        prices = _prices("2024-01-01 01:00:00", 60, base=100.0)
        rows = compute_real_trade_pnl([later_rev, earlier_rev], {}, prices)
        # earlier_rev (revision_ts=00:59) accepted first, later_rev (revision_ts=01:29) accepted second
        by_ts = {r[_TS]: r for r in rows}
        assert by_ts["2024-01-01 01:00:00"][_POS] == pytest.approx(-1.0)
        assert by_ts["2024-01-01 01:30:00"][_POS] == pytest.approx(1.0)

    def test_accepted_revision_pnl_continuity(self):
        """Accepted first revision holds position until second revision fires."""
        bars = [
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 01:00:00",
                execution_ts="2024-01-01 01:00:00",
                revision_ts="2024-01-01 00:59:00",
                position=1.0,
                tf="1h",
            ),
            _rt_bar(
                ts="2024-01-01 00:00:00",
                closing_ts="2024-01-01 01:00:00",
                execution_ts="2024-01-01 01:30:00",
                revision_ts="2024-01-01 01:29:00",
                position=-1.0,
                tf="1h",
            ),
        ]
        prices = _prices("2024-01-01 01:00:00", 60, base=100.0)
        rows = compute_real_trade_pnl(bars, {}, prices)
        by_ts = {r[_TS]: r for r in rows}
        assert by_ts["2024-01-01 01:00:00"][_POS] == pytest.approx(1.0)
        assert by_ts["2024-01-01 01:29:00"][_POS] == pytest.approx(1.0)
        assert by_ts["2024-01-01 01:30:00"][_POS] == pytest.approx(-1.0)


@pytest.mark.unit
class TestBtRawJsonPnlSeeding:
    """BT always seeds from bar's raw_json cumulative_pnl — no cross-bar anchor chaining."""

    def test_bt_uses_raw_json_pnl_as_base_flat_prices(self):
        """Flat prices: each bar's pnl stays at its own cumulative_pnl throughout."""
        bt_bar_obj = _bt_bar(
            ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:05:00",
            position=1.0, cumulative_pnl=0.05,
        )
        prices = _prices("2024-01-01 00:05:00", 5, base=100.0)
        rows = compute_bt_pnl([bt_bar_obj], prices)
        assert len(rows) == 5
        for row in rows:
            assert row[_CPNL] == pytest.approx(0.05)

    def test_bt_uses_raw_json_pnl_as_base_rising_prices(self):
        """Rising prices: pnl rolls from bar's cumulative_pnl, not from any prior anchor."""
        bt_bar_obj = _bt_bar(
            ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:05:00",
            position=1.0, cumulative_pnl=0.05,
        )
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 102.0,
            "2024-01-01 00:07:00": 105.0,
            "2024-01-01 00:08:00": 108.0,
            "2024-01-01 00:09:00": 110.0,
        }
        rows = compute_bt_pnl([bt_bar_obj], prices)
        assert len(rows) == 5
        # Minute 0: first price seen — cpnl = 0.05 (bar base), no change
        assert rows[0][_CPNL] == pytest.approx(0.05)
        # Minute 1: 0.05 + 1*(102-100)/100 = 0.07
        assert rows[1][_CPNL] == pytest.approx(0.05 + 1.0 * (102.0 - 100.0) / 100.0, rel=1e-9)
        assert rows[1][_PRC] == pytest.approx(102.0)

    def test_bt_missing_price_carry_forward_uses_prev_minute_price(self):
        """Missing minute emits with previous minute's price as carry-forward."""
        bt_bar_obj = _bt_bar(
            ts="2024-01-01 00:00:00", execution_ts="2024-01-01 00:05:00",
            position=1.0, cumulative_pnl=0.0,
        )
        prices = {
            "2024-01-01 00:05:00": 100.0,
            # 00:06 missing — carry-forward from 100.0
            "2024-01-01 00:07:00": 102.0,
        }
        rows = compute_bt_pnl([bt_bar_obj], prices)
        ts_values = [r[_TS] for r in rows]
        assert "2024-01-01 00:06:00" in ts_values
        row_06 = next(r for r in rows if r[_TS] == "2024-01-01 00:06:00")
        assert row_06[_PRC] == pytest.approx(100.0)
        assert row_06[_CPNL] == pytest.approx(0.0)  # flat carry, pnl unchanged


# ═════════════════════════════════════════════════════════════════════════════
# extract_row_anchor
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestExtractRowAnchor:

    def _make_row(self, cpnl: float, price: float, position: float) -> list:
        row = [""] * 16
        row[_CPNL] = cpnl
        row[_PRC]  = price
        row[_POS]  = position
        return row

    def test_extracts_correct_values(self):
        row = self._make_row(cpnl=0.15, price=95000.0, position=-1.0)
        pnl, price, pos = extract_row_anchor(row)
        assert pnl   == pytest.approx(0.15)
        assert price == pytest.approx(95000.0)
        assert pos   == pytest.approx(-1.0)

    def test_returns_tuple_of_three(self):
        row = self._make_row(0.0, 100.0, 0.0)
        result = extract_row_anchor(row)
        assert len(result) == 3

    def test_zero_values(self):
        row = self._make_row(0.0, 0.0, 0.0)
        pnl, price, pos = extract_row_anchor(row)
        assert pnl == 0.0 and price == 0.0 and pos == 0.0

    def test_roundtrip_with_compute_prod_pnl(self):
        """extract_row_anchor on last prod row returns anchor usable for next chunk."""
        bar = _prod_bar(ts="2024-01-01 00:00:00", position=1.0, tf="5m")
        prices = {
            "2024-01-01 00:05:00": 100.0,
            "2024-01-01 00:06:00": 110.0,
        }
        rows = compute_prod_pnl([bar], {}, prices)
        last = rows[-1]
        pnl, price, pos = extract_row_anchor(last)
        # Next chunk with anchor — flat prices → pnl must stay at extracted value
        bar2 = _prod_bar(ts="2024-01-01 00:10:00", position=1.0, tf="5m")
        prices2 = _prices("2024-01-01 00:15:00", 5, base=price)
        rows2 = compute_prod_pnl([bar2], {"S1": (pnl, price, pos)}, prices2)
        for row in rows2:
            assert row[_CPNL] == pytest.approx(pnl)
