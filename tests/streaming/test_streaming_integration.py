"""
Integration test: Binance REST -> Redpanda (testcontainers) -> process_candle().

Spins up a real Redpanda broker in Docker, fetches a live closed candle from
Binance Futures REST (same data Binance WS would deliver), publishes it via
publish_candle(), consumes it back, runs process_candle() with a stubbed CH
lookup, and asserts the output rows are structurally correct.

No ClickHouse writes occur. Requires Docker on the runner and outbound access
to fstream.binance.com (available from GitHub Actions, not needed from local
if Binance is geo-blocked -- test auto-skips in that case).
"""

import json
import time
from datetime import datetime, UTC
from unittest.mock import patch

import pytest

pytest.importorskip("testcontainers", reason="testcontainers not installed")
pytest.importorskip("ccxt", reason="ccxt not installed")

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from confluent_kafka import Consumer, Producer
from streaming.models import CandleEvent
from streaming.binance_ws_consumer import publish_candle, TOPIC
from flink_job.pnl_stream_job import process_candle
from flink_job.anchor_state import AnchorState
from flink_job.ch_lookup import StrategyBar


def _fetch_live_candle() -> CandleEvent | None:
    import ccxt
    try:
        exchange = ccxt.binanceusdm({"enableRateLimit": True})
        ohlcv = exchange.fetch_ohlcv("BTC/USDT:USDT", timeframe="1m", limit=2)
        if not ohlcv or len(ohlcv) < 2:
            return None
        ts_ms, o, h, l, c, v = ohlcv[-2]  # [-2] is last closed candle; [-1] is still open
        return CandleEvent(
            exchange="binance",
            instrument="BTCUSDT",
            ts=datetime.fromtimestamp(ts_ms / 1000, tz=UTC).replace(tzinfo=None),
            open=float(o),
            high=float(h),
            low=float(l),
            close=float(c),
            volume=float(v),
        )
    except Exception:
        return None


def _stub_strategy_bar(instrument: str) -> list[StrategyBar]:
    return [
        StrategyBar(
            strategy_table_name="strat_btc_test",
            strategy_id=1,
            strategy_name="TestStrategy",
            underlying=instrument,
            config_timeframe="1m",
            weighting=1.0,
            position=0.5,
            final_signal=1.0,
            benchmark=0.0,
        )
    ]


@pytest.fixture(scope="module")
def redpanda_broker():
    container = (
        DockerContainer("redpandadata/redpanda:latest")
        .with_command(
            "redpanda start "
            "--smp 1 "
            "--memory 512M "
            "--reserve-memory 0M "
            "--node-id 0 "
            "--kafka-addr PLAINTEXT://0.0.0.0:9092 "
            "--advertise-kafka-addr PLAINTEXT://localhost:9092 "
            "--set redpanda.auto_create_topics_enabled=true"
        )
        .with_exposed_ports(9092)
    )
    with container:
        wait_for_logs(container, "Successfully started Redpanda!", timeout=60)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(9092)
        yield f"{host}:{port}"


@pytest.fixture(scope="module")
def producer(redpanda_broker):
    p = Producer({"bootstrap.servers": redpanda_broker, "acks": "all"})
    yield p
    p.flush(timeout=5)


@pytest.fixture(scope="module")
def consumer(redpanda_broker):
    c = Consumer({
        "bootstrap.servers": redpanda_broker,
        "group.id": "integration-test",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])
    yield c
    c.close()


@pytest.mark.streaming_integration
def test_live_candle_fetch():
    candle = _fetch_live_candle()
    if candle is None:
        pytest.skip("Binance Futures REST unreachable (geo-block or no network)")

    assert candle.exchange == "binance"
    assert candle.instrument == "BTCUSDT"
    assert candle.close > 0
    assert candle.ts.tzinfo is None  # naive UTC


@pytest.mark.streaming_integration
def test_publish_and_consume_roundtrip(producer, consumer):
    candle = _fetch_live_candle()
    if candle is None:
        pytest.skip("Binance Futures REST unreachable (geo-block or no network)")

    publish_candle(producer, candle)
    producer.flush(timeout=10)

    deadline = time.monotonic() + 15
    received = None
    while time.monotonic() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        assert msg.error() is None, f"Kafka error: {msg.error()}"
        received = json.loads(msg.value().decode())
        break

    assert received is not None, "No message received from Redpanda within 15s"
    assert received["exchange"] == "binance"
    assert received["instrument"] == "BTCUSDT"
    assert received["close"] == candle.close
    assert received["ts"] == candle.ts.strftime("%Y-%m-%dT%H:%M:%S")


@pytest.mark.streaming_integration
def test_process_candle_no_clickhouse_writes(producer, consumer):
    candle = _fetch_live_candle()
    if candle is None:
        pytest.skip("Binance Futures REST unreachable (geo-block or no network)")

    state = AnchorState()

    with patch("flink_job.pnl_stream_job.fetch_strategies_for_candle",
               side_effect=_stub_strategy_bar):
        rows = process_candle(candle, state)

    price_rows = [r for r in rows if r["_sink"] == "price"]
    pnl_rows = [r for r in rows if r["_sink"] == "pnl"]

    assert len(price_rows) == 1
    assert price_rows[0]["instrument"] == "BTCUSDT"
    assert price_rows[0]["close"] == candle.close

    assert len(pnl_rows) == 1
    assert pnl_rows[0]["strategy_table_name"] == "strat_btc_test"
    assert pnl_rows[0]["cumulative_pnl"] == 0.0  # anchor_price==0.0 on first call
    assert isinstance(pnl_rows[0]["ts"], datetime)

    # Second call with same price: anchor advances but delta is 0
    with patch("flink_job.pnl_stream_job.fetch_strategies_for_candle",
               side_effect=_stub_strategy_bar):
        rows2 = process_candle(candle, state)

    pnl_rows2 = [r for r in rows2 if r["_sink"] == "pnl"]
    assert pnl_rows2[0]["cumulative_pnl"] == pytest.approx(0.0)
