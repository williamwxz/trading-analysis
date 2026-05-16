"""Unit tests for flink_pnl.clickhouse_sink.ClickHouseSinkFunction."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from flink_pnl.clickhouse_sink import ClickHouseSinkFunction
from flink_pnl.sink_config import SinkConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PRICE_COLUMNS = ["instrument", "ts", "open", "high", "low", "close", "volume_base"]

_INSERT_COLUMNS = [
    "strategy_table_name",
    "strategy_id",
    "strategy_name",
    "underlying",
    "config_timeframe",
    "source",
    "version",
    "ts",
    "cumulative_pnl",
    "benchmark",
    "position",
    "price",
    "final_signal",
    "weighting",
    "updated_at",
    "strategy_instance_id",
]

_TS = datetime(2026, 5, 15, 10, 0, 0)
_UPDATED_AT = datetime(2026, 5, 15, 10, 1, 0)


def _price_row() -> dict:
    return {
        "_sink": "price",
        "instrument": "BTCUSDT",
        "ts": _TS,
        "open": 50000.0,
        "high": 50100.0,
        "low": 49900.0,
        "close": 50050.0,
        "volume": 1.5,
    }


def _pnl_row(sink_key: str = "pnl_prod") -> dict:
    row = [
        "strat_btc_5m",  # 0 strategy_table_name
        1,  # 1 strategy_id
        "momentum",  # 2 strategy_name
        "BTC",  # 3 underlying
        "5m",  # 4 config_timeframe
        "production",  # 5 source
        "v2",  # 6 version
        _TS,  # 7 ts
        0.05,  # 8 cumulative_pnl
        0.0,  # 9 benchmark
        1.0,  # 10 position
        50000.0,  # 11 price
        1.0,  # 12 final_signal
        1.0,  # 13 weighting
        _UPDATED_AT,  # 14 updated_at
        "sid1",  # 15 strategy_instance_id
    ]
    return {"_sink": sink_key, "_row": row}


def _cfg(price=True, prod=True, bt=True, real_trade=True) -> SinkConfig:
    return SinkConfig(price=price, prod=prod, bt=bt, real_trade=real_trade)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_invoke_routes_price_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_price_row())
    assert len(sink._price_buf) == 1
    assert len(sink._prod_buf) == 0
    assert len(sink._bt_buf) == 0
    assert len(sink._rt_buf) == 0


@pytest.mark.unit
def test_invoke_routes_pnl_prod_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod"))
    assert len(sink._prod_buf) == 1
    assert len(sink._price_buf) == 0
    assert len(sink._bt_buf) == 0
    assert len(sink._rt_buf) == 0


@pytest.mark.unit
def test_invoke_routes_pnl_bt_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_bt"))
    assert len(sink._bt_buf) == 1
    assert len(sink._price_buf) == 0
    assert len(sink._prod_buf) == 0
    assert len(sink._rt_buf) == 0


@pytest.mark.unit
def test_invoke_routes_pnl_real_trade_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_real_trade"))
    assert len(sink._rt_buf) == 1
    assert len(sink._price_buf) == 0
    assert len(sink._prod_buf) == 0
    assert len(sink._bt_buf) == 0


@pytest.mark.unit
@patch("libs.clickhouse_client.get_client")
def test_flush_empty_buffers_makes_no_clickhouse_calls(mock_get_client):
    sink = ClickHouseSinkFunction(_cfg())
    sink.flush()
    mock_get_client.assert_not_called()


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_flush_inserts_price_rows(mock_get_client, mock_insert_rows):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_price_row())
    sink.invoke(_price_row())
    sink.flush()

    mock_get_client.assert_called_once()
    mock_insert_rows.assert_called_once()
    args = mock_insert_rows.call_args
    assert args[0][0] == "analytics.futures_price_1min"
    assert args[0][1] == _PRICE_COLUMNS


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_flush_inserts_pnl_prod_rows(mock_get_client, mock_insert_rows):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod"))
    sink.flush()

    mock_get_client.assert_called_once()
    mock_insert_rows.assert_called_once()
    args = mock_insert_rows.call_args
    assert args[0][0] == "analytics.strategy_pnl_1min_prod_v2"
    assert args[0][1] == _INSERT_COLUMNS


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_flush_clears_buffers_after_success(mock_get_client, mock_insert_rows):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_price_row())
    sink.invoke(_pnl_row("pnl_prod"))
    sink.invoke(_pnl_row("pnl_bt"))
    sink.invoke(_pnl_row("pnl_real_trade"))
    sink.flush()

    assert sink._price_buf == []
    assert sink._prod_buf == []
    assert sink._bt_buf == []
    assert sink._rt_buf == []


@pytest.mark.unit
@patch("libs.clickhouse_client.get_client")
def test_invoke_unknown_sink_key_is_ignored(mock_get_client):
    sink = ClickHouseSinkFunction(_cfg())
    # Should not raise
    sink.invoke({"_sink": "unknown_key", "data": "whatever"})

    assert sink._price_buf == []
    assert sink._prod_buf == []
    assert sink._bt_buf == []
    assert sink._rt_buf == []

    sink.flush()
    mock_get_client.assert_not_called()


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_volume_key_mapped_to_volume_base(mock_get_client, mock_insert_rows):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    sink = ClickHouseSinkFunction(_cfg())
    row = _price_row()
    sink.invoke(row)
    sink.flush()

    args = mock_insert_rows.call_args
    columns = args[0][1]
    rows = args[0][2]

    # Column name must be volume_base, not volume
    assert "volume_base" in columns
    assert "volume" not in columns

    # The value at the volume_base index must equal the original row["volume"]
    vol_idx = columns.index("volume_base")
    assert rows[0][vol_idx] == row["volume"]
