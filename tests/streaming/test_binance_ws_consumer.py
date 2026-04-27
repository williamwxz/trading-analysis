import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from streaming.binance_ws_consumer import (
    INSTRUMENTS,
    build_stream_url,
    parse_and_filter,
    publish_candle,
)
from streaming.models import CandleEvent


@pytest.mark.unit
def test_build_stream_url_contains_all_instruments():
    url = build_stream_url(INSTRUMENTS)
    for sym in INSTRUMENTS:
        assert sym.lower() + "@kline_1m" in url
    assert url.startswith("wss://fstream.binance.com/stream?streams=")


@pytest.mark.unit
def test_parse_and_filter_returns_none_for_open_candle():
    msg = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
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
            },
        }
    )
    assert parse_and_filter(msg) is None


@pytest.mark.unit
def test_parse_and_filter_returns_candle_for_closed():
    msg = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
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
            },
        }
    )
    candle = parse_and_filter(msg)
    assert isinstance(candle, CandleEvent)
    assert candle.instrument == "BTCUSDT"


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
