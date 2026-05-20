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


def _pnl_row(sink_key: str = "pnl_prod", siid: str = "sid1") -> dict:
    row = [
        "strat_btc_5m",  # 0 strategy_table_name
        1,               # 1 strategy_id
        "momentum",      # 2 strategy_name
        "BTC",           # 3 underlying
        "5m",            # 4 config_timeframe
        "production",    # 5 source
        "v2",            # 6 version
        _TS,             # 7 ts
        0.05,            # 8 cumulative_pnl
        0.0,             # 9 benchmark
        1.0,             # 10 position
        50000.0,         # 11 price
        1.0,             # 12 final_signal
        1.0,             # 13 weighting
        _UPDATED_AT,     # 14 updated_at
        siid,            # 15 strategy_instance_id
    ]
    return {"_sink": sink_key, "_row": row}


def _cfg(prod=True, bt=True, real_trade=True) -> SinkConfig:
    return SinkConfig(price=False, prod=prod, bt=bt, real_trade=real_trade)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_invoke_price_row_is_silently_ignored():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke({"_sink": "price", "instrument": "BTCUSDT"})
    assert sink._prod_buf == []
    assert sink._bt_buf == []
    assert sink._rt_buf == []


@pytest.mark.unit
def test_invoke_routes_pnl_prod_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod"))
    assert len(sink._prod_buf) == 1
    assert sink._bt_buf == []
    assert sink._rt_buf == []


@pytest.mark.unit
def test_invoke_routes_pnl_bt_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_bt"))
    assert len(sink._bt_buf) == 1
    assert sink._prod_buf == []
    assert sink._rt_buf == []


@pytest.mark.unit
def test_invoke_routes_pnl_real_trade_row():
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_real_trade"))
    assert len(sink._rt_buf) == 1
    assert sink._prod_buf == []
    assert sink._bt_buf == []


@pytest.mark.unit
@patch("libs.clickhouse_client.get_client")
def test_flush_empty_buffers_makes_no_clickhouse_calls(mock_get_client):
    sink = ClickHouseSinkFunction(_cfg())
    sink.flush()
    mock_get_client.assert_not_called()


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_flush_inserts_pnl_prod_rows(mock_get_client, mock_insert_rows):
    mock_get_client.return_value = MagicMock()

    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod"))
    sink.flush(expected_prod=1)

    mock_get_client.assert_called_once()
    mock_insert_rows.assert_called_once()
    args = mock_insert_rows.call_args
    assert args[0][0] == "analytics.strategy_pnl_1min_prod_v2"
    assert args[0][1] == _INSERT_COLUMNS


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_flush_clears_buffers_after_success(mock_get_client, mock_insert_rows):
    mock_get_client.return_value = MagicMock()

    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod"))
    sink.invoke(_pnl_row("pnl_bt"))
    sink.invoke(_pnl_row("pnl_real_trade"))
    sink.flush(expected_prod=1, expected_bt=1, expected_real_trade=1)

    assert sink._prod_buf == []
    assert sink._bt_buf == []
    assert sink._rt_buf == []


@pytest.mark.unit
@patch("libs.clickhouse_client.get_client")
def test_invoke_unknown_sink_key_is_ignored(mock_get_client):
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke({"_sink": "unknown_key", "data": "whatever"})

    assert sink._prod_buf == []
    assert sink._bt_buf == []
    assert sink._rt_buf == []

    sink.flush()
    mock_get_client.assert_not_called()


@pytest.mark.unit
def test_completeness_check_raises_when_partial():
    sink = ClickHouseSinkFunction(_cfg())
    # Two strategies expected but only one row buffered (distinct siid count = 1)
    sink.invoke(_pnl_row("pnl_prod", siid="sid1"))

    with pytest.raises(RuntimeError, match="completeness check failed"):
        sink.flush(expected_prod=2)


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_completeness_check_passes_when_counts_match(mock_get_client, mock_insert_rows):
    mock_get_client.return_value = MagicMock()

    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod", siid="sid1"))
    sink.invoke(_pnl_row("pnl_prod", siid="sid2"))

    # Should not raise — 2 distinct siids matches expected=2
    sink.flush(expected_prod=2)
    assert mock_insert_rows.call_count == 1


@pytest.mark.unit
@patch("libs.clickhouse_client.insert_rows")
@patch("libs.clickhouse_client.get_client")
def test_completeness_check_skipped_when_expected_is_zero(mock_get_client, mock_insert_rows):
    mock_get_client.return_value = MagicMock()
    sink = ClickHouseSinkFunction(_cfg())
    sink.invoke(_pnl_row("pnl_prod", siid="sid1"))
    # expected=0 means no check (e.g. carry-forward only candle)
    # Should not raise
    sink.flush(expected_prod=0)
