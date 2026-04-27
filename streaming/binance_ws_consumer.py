"""
Binance Futures WebSocket consumer.

Subscribes to kline_1m combined stream for INSTRUMENTS.
Filters for closed candles only (k.x == true).
Publishes JSON CandleEvent to Redpanda topic binance.price.ticks.
Reconnects with exponential backoff on disconnect.
"""

import asyncio
import json
import logging
import os
from collections import deque

import websockets
from confluent_kafka import Producer

from .models import CandleEvent

logger = logging.getLogger(__name__)

INSTRUMENTS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "DOGEUSDT",
    "XRPUSDT",
    "FETUSDT",
]

TOPIC = "binance.price.ticks"
_BACKOFF_CAPS = [1, 2, 4, 8, 16, 32, 60]  # seconds


def build_stream_url(instruments: list[str]) -> str:
    streams = "/".join(f"{s.lower()}@kline_1m" for s in instruments)
    return f"wss://fstream.binance.com/stream?streams={streams}"


def parse_and_filter(raw_message: str) -> CandleEvent | None:
    """Return CandleEvent only for closed candles; None otherwise."""
    try:
        data = json.loads(raw_message)
        if data.get("data", {}).get("k", {}).get("x") is not True:
            return None
        return CandleEvent.from_binance_kline(data)
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Failed to parse message: %s — %s", raw_message[:200], exc)
        return None


def publish_candle(producer: Producer, candle: CandleEvent, topic: str = TOPIC) -> None:
    producer.produce(
        topic=topic,
        key=candle.instrument.encode(),
        value=candle.to_json().encode(),
    )
    producer.poll(0)  # trigger delivery callbacks without blocking


def _make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
            "acks": "all",
            "retries": 5,
        }
    )


async def _consume_forever(producer: Producer, buffer: deque) -> None:
    url = build_stream_url(INSTRUMENTS)
    backoff_idx = 0

    while True:
        try:
            logger.info("Connecting to Binance WS: %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff_idx = 0  # reset on successful connection
                # flush buffer accumulated during outage
                while buffer:
                    candle = buffer.popleft()
                    publish_candle(producer, candle)
                logger.info("Connected. Listening for closed candles...")
                async for raw in ws:
                    msg_str = raw if isinstance(raw, str) else raw.decode()
                    candle = parse_and_filter(msg_str)
                    if candle is not None:
                        publish_candle(producer, candle)
        except Exception as exc:
            delay = _BACKOFF_CAPS[min(backoff_idx, len(_BACKOFF_CAPS) - 1)]
            logger.warning("WS error: %s. Reconnecting in %ds...", exc, delay)
            backoff_idx += 1
            await asyncio.sleep(delay)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    producer = _make_producer()
    buffer: deque = deque(maxlen=120)  # 2 min of candles during Redpanda outage
    await _consume_forever(producer, buffer)


if __name__ == "__main__":
    asyncio.run(main())
