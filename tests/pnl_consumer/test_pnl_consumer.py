from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import StrategyBar
from pnl_consumer.pnl_consumer import process_candle, _aggregate_hourly
from streaming.models import CandleEvent
from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS


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
    state = AnchorState()
    state.update(
        "strat_prod_1",
        AnchorRecord(anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0),
    )
    candle = _make_candle(close=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with patch(
        "pnl_consumer.pnl_consumer.fetch_strategies_for_candle",
        return_value=strategies,
    ):
        rows = process_candle(candle, state)

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl"]
    assert len(pnl_rows) == 1
    row = pnl_rows[0]
    assert row["strategy_table_name"] == "strat_prod_1"
    assert row["underlying"] == "BTCUSDT"
    assert row["price"] == 93200.0
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_no_strategies_returns_only_price_row():
    state = AnchorState()
    candle = _make_candle()

    with patch("pnl_consumer.pnl_consumer.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state)

    pnl_rows = [r for r in rows if r.get("_sink") == "pnl"]
    assert pnl_rows == []
    price_rows = [r for r in rows if r.get("_sink") == "price"]
    assert len(price_rows) == 1


@pytest.mark.unit
def test_process_candle_always_emits_price_row():
    state = AnchorState()
    candle = _make_candle()
    strategies = [_make_strategy()]

    with patch(
        "pnl_consumer.pnl_consumer.fetch_strategies_for_candle",
        return_value=strategies,
    ):
        rows = process_candle(candle, state)

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
        rows = _aggregate_hourly([
            _make_row("strat_a", ts1, 1.0),
            _make_row("strat_a", ts2, 2.0),
        ])
        assert len(rows) == 1
        assert rows[0][COL["cumulative_pnl"]] == 2.0

    def test_two_strategies_same_hour_produces_two_rows(self):
        ts = datetime(2026, 3, 1, 10, 5, 0)
        rows = _aggregate_hourly([
            _make_row("strat_a", ts, 1.0),
            _make_row("strat_b", ts, 2.0),
        ])
        assert len(rows) == 2

    def test_same_strategy_different_hours_produces_two_rows(self):
        ts1 = datetime(2026, 3, 1, 10, 5, 0)
        ts2 = datetime(2026, 3, 1, 11, 5, 0)
        rows = _aggregate_hourly([
            _make_row("strat_a", ts1, 1.0),
            _make_row("strat_a", ts2, 2.0),
        ])
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
