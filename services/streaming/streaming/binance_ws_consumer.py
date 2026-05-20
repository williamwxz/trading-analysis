"""
Binance Spot WebSocket consumer.

Subscribes to kline_1m stream for INSTRUMENTS via SUBSCRIBE message.
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


_WS_BASE_URL = "wss://stream.binance.com:443/ws"


def build_subscribe_msg(instruments: list[str], req_id: int = 1) -> str:
    params = [f"{s.lower()}@kline_1m" for s in instruments]
    return json.dumps({"method": "SUBSCRIBE", "params": params, "id": req_id})


def parse_and_filter(raw_message: str) -> CandleEvent | None:
    """Return CandleEvent only for closed candles; None otherwise.

    Handles raw kline payload: {"e":"kline","s":"BTCUSDT","k":{...}}
    """
    try:
        data = json.loads(raw_message)
        k = data.get("k", {})
        if k.get("x") is not True:
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
    subscribe_msg = build_subscribe_msg(INSTRUMENTS)
    backoff_idx = 0

    while True:
        try:
            logger.info("Connecting to Binance WS: %s", _WS_BASE_URL)
            async with websockets.connect(_WS_BASE_URL, ping_interval=20, ping_timeout=10) as ws:
                backoff_idx = 0
                await ws.send(subscribe_msg)
                logger.info("Sent SUBSCRIBE for %d instruments", len(INSTRUMENTS))
                # flush buffer accumulated during outage
                while buffer:
                    candle = buffer[0]
                    publish_candle(producer, candle)
                    buffer.popleft()
                frame_count = 0
                async for raw in ws:
                    msg_str = raw if isinstance(raw, str) else raw.decode()
                    data = json.loads(msg_str)
                    # subscription ack — {"result": null, "id": 1}
                    if "result" in data:
                        logger.info("Subscription ack: %s", msg_str)
                        continue
                    frame_count += 1
                    if frame_count <= 10:
                        logger.info("RAW frame #%d: %s", frame_count, msg_str[:500])
                    elif frame_count % 10 == 0:
                        logger.info("WS frames received: %d (still connected)", frame_count)
                    candle = parse_and_filter(msg_str)
                    if candle is not None:
                        try:
                            publish_candle(producer, candle)
                            logger.info(
                                "Published %s %s close=%.2f ts=%s",
                                candle.instrument, candle.exchange,
                                candle.close, candle.ts,
                            )
                        except Exception as publish_exc:
                            logger.warning("Failed to publish candle, buffering: %s", publish_exc)
                            buffer.append(candle)
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
