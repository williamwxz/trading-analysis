import json
from datetime import datetime

import pytest

from streaming.models import CandleEvent


@pytest.mark.unit
def test_from_binance_kline_parses_correctly():
    msg = {
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "k": {
                "s": "BTCUSDT",
                "t": 1777161600000,  # 2026-04-26T00:00:00 UTC in ms
                "o": "93100.0",
                "h": "93250.0",
                "l": "93050.0",
                "c": "93200.0",
                "v": "12.34",
                "x": True,
            },
        },
    }
    candle = CandleEvent.from_binance_kline(msg)
    assert candle.exchange == "binance"
    assert candle.instrument == "BTCUSDT"
    assert candle.ts == datetime(2026, 4, 26, 0, 0, 0)
    assert candle.open == 93100.0
    assert candle.close == 93200.0
    assert candle.volume == 12.34


@pytest.mark.unit
def test_to_json_round_trips():
    candle = CandleEvent(
        exchange="binance",
        instrument="ETHUSDT",
        ts=datetime(2026, 4, 26, 0, 1, 0),
        open=1800.0,
        high=1810.0,
        low=1790.0,
        close=1805.0,
        volume=100.0,
    )
    payload = json.loads(candle.to_json())
    assert payload["instrument"] == "ETHUSDT"
    assert payload["ts"] == "2026-04-26T00:01:00"
    assert payload["close"] == 1805.0


@pytest.mark.unit
def test_from_binance_kline_raises_for_open_candle():
    msg = {
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "k": {
                "s": "BTCUSDT",
                "t": 1777161600000,
                "o": "93100.0",
                "h": "93250.0",
                "l": "93050.0",
                "c": "93200.0",
                "v": "12.34",
                "x": False,  # NOT closed
            },
        },
    }
    with pytest.raises(ValueError, match="not closed"):
        CandleEvent.from_binance_kline(msg)
