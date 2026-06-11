"""bt streaming consumer must reset cumulative_pnl to row_json at every new bar.

The offline batch path (libs.computation.pnl_formula.compute_bt_pnl) treats the
row_json cumulative_pnl as authoritative: at each bar boundary it resets cpnl to
that value and chains only *within* the bar. The streaming consumer historically
chained forward across bars, so it drifted from the offline (authoritative) values.

These tests pin the streaming bt path to the offline semantics:
  1. New bar  -> reset cpnl to the bar's row_json cumulative_pnl.
  2. Same bar -> chain forward via the price-return formula (no reset).
  3. End-to-end: streaming output == compute_bt_pnl output, per minute.
"""

from datetime import datetime
from unittest.mock import patch

import pytest
from pnl_consumer.pnl_consumer import SinkConfig, process_candle
from streaming.models import CandleEvent

from libs.computation import AnchorRecord, AnchorState, StrategyBar, compute_bt_pnl

_MOD = "pnl_consumer.pnl_consumer"


def _candle(open_: float, ts: datetime, instrument: str = "BTCUSDT") -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=ts,
        open=open_,
        high=open_,
        low=open_,
        close=open_,
        volume=1.0,
    )


def _bt_bar(
    *,
    bar_ts: datetime,
    cumulative_pnl: float,
    position: float,
    stn: str = "strat_bt_1",
    underlying: str = "BTC",
) -> StrategyBar:
    return StrategyBar(
        strategy_table_name=stn,
        strategy_instance_id="inst_bt_001",
        strategy_id=2,
        strategy_name="bt_mom",
        underlying=underlying,
        config_timeframe="1h",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        bar_ts=bar_ts,
        cumulative_pnl=cumulative_pnl,
    )


_BT_CFG = SinkConfig(price=False, prod=False, real_trade=False, bt=True)


@pytest.mark.unit
def test_bt_resets_pnl_on_new_bar():
    """A new bar (bar_ts newer than the anchor) resets cpnl to row_json's value.

    The anchor carries a stale chained pnl (99.0) at a different price (100.0).
    When a new bar arrives carrying row_json cumulative_pnl=10.0, the emitted
    pnl must be exactly 10.0 — the price re-anchors to candle.open so the
    within-minute price-return term is zero. The old chain-forward behavior
    would have produced 99.0 + 2.0*(200-100)/100 = 101.0.
    """
    state = AnchorState()
    state.set(
        "strat_bt_1",
        AnchorRecord(pnl=99.0, price=100.0, bar_ts=datetime(2026, 4, 26, 1, 0)),
    )
    new_bar = _bt_bar(
        bar_ts=datetime(2026, 4, 26, 2, 0), cumulative_pnl=10.0, position=2.0
    )
    candle = _candle(open_=200.0, ts=datetime(2026, 4, 26, 3, 0))

    with patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[new_bar]):
        rows, _, _, _ = process_candle(
            candle, AnchorState(), AnchorState(), state, _BT_CFG
        )

    bt_rows = [r for r in rows if r["_sink"] == "pnl_bt"]
    assert len(bt_rows) == 1
    assert bt_rows[0]["_row"][8] == pytest.approx(10.0)


@pytest.mark.unit
def test_bt_chains_within_same_bar():
    """A second candle on the SAME bar chains via the formula (no reset).

    The anchor is mid-bar at pnl=10.0, price=200.0, bar_ts=B. The same bar B
    arrives again (row_json still says 10.0). The emitted pnl must chain:
    10.0 + 2.0*(202-200)/200 = 10.02 — NOT reset back to row_json's 10.0.
    """
    bar_ts = datetime(2026, 4, 26, 2, 0)
    state = AnchorState()
    state.set("strat_bt_1", AnchorRecord(pnl=10.0, price=200.0, bar_ts=bar_ts))
    same_bar = _bt_bar(bar_ts=bar_ts, cumulative_pnl=10.0, position=2.0)
    candle = _candle(open_=202.0, ts=datetime(2026, 4, 26, 2, 30))

    with patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[same_bar]):
        rows, _, _, _ = process_candle(
            candle, AnchorState(), AnchorState(), state, _BT_CFG
        )

    bt_rows = [r for r in rows if r["_sink"] == "pnl_bt"]
    assert len(bt_rows) == 1
    assert bt_rows[0]["_row"][8] == pytest.approx(10.02)


@pytest.mark.unit
def test_bt_matches_offline_compute_bt_pnl():
    """End-to-end: streaming bt path == offline compute_bt_pnl, per minute.

    Two bars over four minutes. Feed the streaming consumer one candle per
    minute (patching the lookup to return the active bar), collect cpnl per ts.
    Run compute_bt_pnl over the equivalent bar list + price map. Assert equal.
    """
    # Per-minute prices.
    prices = {
        "2026-04-26 02:00:00": 100.0,
        "2026-04-26 02:01:00": 110.0,
        "2026-04-26 02:02:00": 120.0,
        "2026-04-26 02:03:00": 90.0,
    }
    # Bar A active for 02:00-02:01 (open 01:00); Bar B active 02:02-02:03 (open 02:00).
    bar_a_ts = datetime(2026, 4, 26, 1, 0)
    bar_b_ts = datetime(2026, 4, 26, 2, 0)
    schedule = [
        (
            datetime(2026, 4, 26, 2, 0),
            100.0,
            _bt_bar(bar_ts=bar_a_ts, cumulative_pnl=5.0, position=1.0),
        ),
        (
            datetime(2026, 4, 26, 2, 1),
            110.0,
            _bt_bar(bar_ts=bar_a_ts, cumulative_pnl=5.0, position=1.0),
        ),
        (
            datetime(2026, 4, 26, 2, 2),
            120.0,
            _bt_bar(bar_ts=bar_b_ts, cumulative_pnl=8.0, position=-1.0),
        ),
        (
            datetime(2026, 4, 26, 2, 3),
            90.0,
            _bt_bar(bar_ts=bar_b_ts, cumulative_pnl=8.0, position=-1.0),
        ),
    ]

    state = AnchorState()
    streaming: dict[str, float] = {}
    for ts, open_, bar in schedule:
        candle = _candle(open_=open_, ts=ts)
        with patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[bar]):
            rows, _, _, _ = process_candle(
                candle, AnchorState(), AnchorState(), state, _BT_CFG
            )
        bt_rows = [r for r in rows if r["_sink"] == "pnl_bt"]
        assert len(bt_rows) == 1
        streaming[ts.strftime("%Y-%m-%d %H:%M:%S")] = bt_rows[0]["_row"][8]

    # Equivalent offline bars: execution_ts = bar open + tf. Here Bar A executes
    # at 02:00 and Bar B at 02:02 (we set execution_ts directly to align minutes).
    offline_bars = [
        {
            "strategy_table_name": "strat_bt_1",
            "strategy_id": 2,
            "strategy_name": "bt_mom",
            "underlying": "BTC",
            "config_timeframe": "1h",
            "weighting": 1.0,
            "strategy_instance_id": "inst_bt_001",
            "final_signal": 1.0,
            "bar_benchmark": 0.0,
            "position": 1.0,
            "cumulative_pnl": 5.0,
            "execution_ts": "2026-04-26 02:00:00",
        },
        {
            "strategy_table_name": "strat_bt_1",
            "strategy_id": 2,
            "strategy_name": "bt_mom",
            "underlying": "BTC",
            "config_timeframe": "1h",
            "weighting": 1.0,
            "strategy_instance_id": "inst_bt_001",
            "final_signal": 1.0,
            "bar_benchmark": 0.0,
            "position": -1.0,
            "cumulative_pnl": 8.0,
            "execution_ts": "2026-04-26 02:02:00",
        },
    ]
    offline_rows = compute_bt_pnl(offline_bars, prices)
    offline = {r[7]: r[8] for r in offline_rows}

    assert set(streaming) == set(prices)
    for ts_str in prices:
        msg = (
            f"mismatch at {ts_str}: "
            f"streaming={streaming[ts_str]} offline={offline[ts_str]}"
        )
        assert streaming[ts_str] == pytest.approx(offline[ts_str], abs=1e-6), msg
