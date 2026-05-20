import json
from collections import deque
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from streaming.binance_ws_consumer import (
    INSTRUMENTS,
    _WS_BASE_URL,
    build_subscribe_msg,
    parse_and_filter,
    publish_candle,
)
from streaming.models import CandleEvent


@pytest.mark.unit
def test_build_subscribe_msg_contains_all_instruments():
    msg = json.loads(build_subscribe_msg(INSTRUMENTS))
    assert msg["method"] == "SUBSCRIBE"
    for sym in INSTRUMENTS:
        assert f"{sym.lower()}@kline_1m" in msg["params"]
    assert _WS_BASE_URL == "wss://stream.binance.com:443/ws"


@pytest.mark.unit
def test_parse_and_filter_returns_none_for_open_candle():
    msg = json.dumps(
        {
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "t": 1745625600000,
                "o": "93100.0",
                "h": "93250.0",
                "l": "93050.0",
                "c": "93200.0",
                "v": "12.34",
                "x": False,  # not closed
            },
        }
    )
    assert parse_and_filter(msg) is None


@pytest.mark.unit
def test_parse_and_filter_returns_candle_for_closed():
    msg = json.dumps(
        {
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "t": 1745625600000,
                "o": "93100.0",
                "h": "93250.0",
                "l": "93050.0",
                "c": "93200.0",
                "v": "12.34",
                "x": True,  # closed
            },
        }
    )
    candle = parse_and_filter(msg)
    assert isinstance(candle, CandleEvent)
    assert candle.instrument == "BTCUSDT"
    assert candle.close == 93200.0


@pytest.mark.unit
def test_publish_candle_calls_producer_produce():
    mock_producer = MagicMock()
    candle = CandleEvent(
        exchange="binance",
        instrument="BTCUSDT",
        ts=datetime(2026, 4, 26, 0, 0, 0),
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
    )
    publish_candle(mock_producer, candle, topic="binance.price.ticks")
    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args
    assert call_kwargs[1]["topic"] == "binance.price.ticks"
    assert call_kwargs[1]["key"] == b"BTCUSDT"
    payload = json.loads(call_kwargs[1]["value"])
    assert payload["instrument"] == "BTCUSDT"
