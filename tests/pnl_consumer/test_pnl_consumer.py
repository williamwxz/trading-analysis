from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import BtStrategyBar, StrategyBar, StrategyRevision
from pnl_consumer.pnl_consumer import _aggregate_hourly, process_candle
from streaming.models import CandleEvent
from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

_MOD = "pnl_consumer.pnl_consumer"


def _make_candle(instrument="BTCUSDT", close=93200.0) -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=datetime(2026, 4, 26, 0, 1, 0),
        open=93100.0,
        high=93250.0,
        low=93050.0,
        close=close,
        volume=12.34,
    )


def _make_strategy(position=1.0, instrument="BTCUSDT") -> StrategyBar:
    return StrategyBar(
        strategy_table_name="strat_prod_1",
        strategy_id=1,
        strategy_name="momentum",
        underlying=instrument,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
    )


@pytest.mark.unit
def test_process_candle_produces_pnl_rows():
    state_prod = AnchorState()
    state_prod.update(
        "strat_prod_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(close=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, AnchorState())

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl_prod"]
    assert len(pnl_rows) == 1
    row = pnl_rows[0]
    assert row["strategy_table_name"] == "strat_prod_1"
    assert row["underlying"] == "BTCUSDT"
    assert row["price"] == 93200.0
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_no_strategies_returns_only_price_row():
    candle = _make_candle()

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState())

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl_prod"]
    assert pnl_rows == []
    price_rows = [r for r in rows if r.get("_sink") == "price"]
    assert len(price_rows) == 1


@pytest.mark.unit
def test_process_candle_always_emits_price_row():
    candle = _make_candle()
    strategies = [_make_strategy()]

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=strategies),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, AnchorState(), AnchorState())

    price_row = next((r for r in rows if r.get("_sink") == "price"), None)
    assert price_row is not None
    assert price_row["instrument"] == "BTCUSDT"
    assert price_row["exchange"] == "binance"


COL = {name: i for i, name in enumerate(PROD_INSERT_COLUMNS)}


def _make_row(strategy: str, ts: datetime, cumulative_pnl: float) -> list:
    row = [None] * len(PROD_INSERT_COLUMNS)
    row[COL["strategy_table_name"]] = strategy
    row[COL["strategy_id"]] = 1
    row[COL["strategy_name"]] = strategy
    row[COL["underlying"]] = "BTC"
    row[COL["config_timeframe"]] = "5m"
    row[COL["source"]] = "production"
    row[COL["version"]] = "v2"
    row[COL["ts"]] = ts
    row[COL["cumulative_pnl"]] = cumulative_pnl
    row[COL["benchmark"]] = 0.0
    row[COL["position"]] = 1.0
    row[COL["price"]] = 100.0
    row[COL["final_signal"]] = 1.0
    row[COL["weighting"]] = 1.0
    row[COL["updated_at"]] = datetime(2026, 3, 1, 10, 5, 0)
    return row


class TestAggregateHourly:
    def test_single_row_snaps_to_hour(self):
        ts = datetime(2026, 3, 1, 10, 5, 0)
        rows = _aggregate_hourly([_make_row("strat_a", ts, 1.5)])
        assert len(rows) == 1
        assert rows[0][COL["ts"]] == datetime(2026, 3, 1, 10, 0, 0)
        assert rows[0][COL["cumulative_pnl"]] == 1.5

    def test_two_rows_same_strategy_same_hour_picks_latest(self):
        ts1 = datetime(2026, 3, 1, 10, 5, 0)
        ts2 = datetime(2026, 3, 1, 10, 10, 0)
        rows = _aggregate_hourly(
            [
                _make_row("strat_a", ts1, 1.0),
                _make_row("strat_a", ts2, 2.0),
            ]
        )
        assert len(rows) == 1
        assert rows[0][COL["cumulative_pnl"]] == 2.0

    def test_two_strategies_same_hour_produces_two_rows(self):
        ts = datetime(2026, 3, 1, 10, 5, 0)
        rows = _aggregate_hourly(
            [
                _make_row("strat_a", ts, 1.0),
                _make_row("strat_b", ts, 2.0),
            ]
        )
        assert len(rows) == 2

    def test_same_strategy_different_hours_produces_two_rows(self):
        ts1 = datetime(2026, 3, 1, 10, 5, 0)
        ts2 = datetime(2026, 3, 1, 11, 5, 0)
        rows = _aggregate_hourly(
            [
                _make_row("strat_a", ts1, 1.0),
                _make_row("strat_a", ts2, 2.0),
            ]
        )
        assert len(rows) == 2
        hours = {r[COL["ts"]] for r in rows}
        assert datetime(2026, 3, 1, 10, 0, 0) in hours
        assert datetime(2026, 3, 1, 11, 0, 0) in hours

    def test_empty_batch_returns_empty(self):
        assert _aggregate_hourly([]) == []

    def test_updated_at_is_refreshed(self):
        """Each hourly row gets a fresh updated_at, not the source row's value."""
        ts = datetime(2026, 3, 1, 10, 5, 0)
        original_updated_at = datetime(2026, 1, 1)
        row = _make_row("strat_a", ts, 1.0)
        row[COL["updated_at"]] = original_updated_at
        result = _aggregate_hourly([row])
        assert result[0][COL["updated_at"]] != original_updated_at


def _make_revision(
    position=1.0,
    revision_ts=datetime(2026, 4, 26, 0, 1, 10),
    closing_ts=datetime(2026, 4, 26, 0, 5, 59),
) -> StrategyRevision:
    return StrategyRevision(
        strategy_table_name="strat_rt_1",
        strategy_id=1,
        strategy_name="momentum",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        revision_ts=revision_ts,
        closing_ts=closing_ts,
    )


def _make_bt_bar(cumulative_pnl=0.05) -> BtStrategyBar:
    return BtStrategyBar(
        strategy_table_name="strat_bt_1",
        strategy_id=2,
        strategy_name="bt_momentum",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        position=1.0,
        final_signal=1.0,
        benchmark=0.0,
        cumulative_pnl=cumulative_pnl,
    )


@pytest.mark.unit
def test_process_candle_produces_pnl_real_trade_rows():
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    state_real_trade.update(
        "strat_rt_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(close=93200.0)
    revision = _make_revision(position=1.0)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(
            f"{_MOD}.fetch_real_trade_revisions_for_candle",
            return_value=[revision],
        ),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, state_real_trade)

    rt_rows = [r for r in rows if r.get("_sink") == "pnl_real_trade"]
    assert len(rt_rows) == 1
    row = rt_rows[0]
    assert row["strategy_table_name"] == "strat_rt_1"
    assert row["source"] == "real_trade"
    assert row["closing_ts"] == revision.closing_ts
    # execution_ts = toStartOfMinute(revision_ts + 59s)
    expected_exec_ts = (revision.revision_ts + timedelta(seconds=59)).replace(second=0)
    assert row["execution_ts"] == expected_exec_ts
    assert row["traded"] is False
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_multiple_revisions_chains_anchor():
    """Second revision uses the updated anchor from the first revision."""
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    candle = _make_candle(close=93200.0)
    rev1 = _make_revision(position=1.0, revision_ts=datetime(2026, 4, 26, 0, 1, 10))
    rev2 = _make_revision(position=-1.0, revision_ts=datetime(2026, 4, 26, 0, 1, 30))

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(
            f"{_MOD}.fetch_real_trade_revisions_for_candle",
            return_value=[rev1, rev2],
        ),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[]),
    ):
        rows = process_candle(candle, state_prod, state_real_trade)

    rt_rows = [r for r in rows if r.get("_sink") == "pnl_real_trade"]
    assert len(rt_rows) == 2
    assert rt_rows[0]["position"] == 1.0
    assert rt_rows[1]["position"] == -1.0


@pytest.mark.unit
def test_process_candle_produces_pnl_bt_rows():
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    candle = _make_candle(close=93200.0)
    bt_bar = _make_bt_bar(cumulative_pnl=0.05)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[bt_bar]),
    ):
        rows = process_candle(candle, state_prod, state_real_trade)

    bt_rows = [r for r in rows if r.get("_sink") == "pnl_bt"]
    assert len(bt_rows) == 1
    row = bt_rows[0]
    assert row["strategy_table_name"] == "strat_bt_1"
    assert row["source"] == "backtest"
    assert row["cumulative_pnl"] == 0.05


@pytest.mark.unit
def test_process_candle_bt_uses_cumulative_pnl_from_bar_not_anchor():
    """bt rows must use row_json cumulative_pnl directly, not compute from anchor."""
    state_prod = AnchorState()
    state_real_trade = AnchorState()
    candle = _make_candle(close=93200.0)
    bt_bar = _make_bt_bar(cumulative_pnl=0.99)

    with (
        patch(f"{_MOD}.fetch_strategies_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_real_trade_revisions_for_candle", return_value=[]),
        patch(f"{_MOD}.fetch_bt_strategies_for_candle", return_value=[bt_bar]),
    ):
        rows = process_candle(candle, state_prod, state_real_trade)

    bt_rows = [r for r in rows if r.get("_sink") == "pnl_bt"]
    assert bt_rows[0]["cumulative_pnl"] == 0.99
