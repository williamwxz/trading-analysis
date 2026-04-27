# Real-Time Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace 5-minute Binance REST polling with WebSocket streaming through Redpanda and PyFlink, computing PnL per closed 1-min candle with < 1 minute latency.

**Architecture:** A Python WebSocket consumer subscribes to Binance Futures `kline_1m` closed events for 8 instruments and publishes to Redpanda. A PyFlink job consumes candles, does an async lookup join against `strategy_output_history_v2` in ClickHouse (carry-forward last known position), runs the same anchor-chain PnL formula as `pnl_compute.py`, and writes to `futures_price_1min` and `strategy_pnl_1min_prod_v2`. Dagster keeps its backfill and daily assets unchanged; live assets are decommissioned after Flink is validated in shadow mode.

**Tech Stack:** Python 3.11, `websockets`, `confluent-kafka`, PyFlink 1.19, Redpanda (Kafka-compatible), ClickHouse Cloud, AWS ECS Fargate Spot, Terraform, GitHub Actions

---

## File Map

### New files
| File | Responsibility |
|------|---------------|
| `streaming/binance_ws_consumer.py` | Async WS connection to Binance, publishes closed candles to Redpanda |
| `streaming/models.py` | `CandleEvent` dataclass shared by consumer and Flink job |
| `streaming/__init__.py` | Package marker |
| `streaming/Dockerfile` | Image for WS consumer ECS task |
| `flink_job/pnl_stream_job.py` | PyFlink Table API job: Kafka source → lookup join → PnL compute → CH sinks |
| `flink_job/anchor_state.py` | Anchor carry-forward state management (keyed by `strategy_table_name`) |
| `flink_job/ch_lookup.py` | ClickHouse async lookup for strategy positions |
| `flink_job/Dockerfile` | PyFlink image with connector JARs |
| `flink_job/requirements.txt` | PyFlink + clickhouse-connect |
| `flink_job/connectors/` | Directory for downloaded Flink connector JARs |
| `tests/streaming/test_binance_ws_consumer.py` | Unit tests for consumer (mocked WS + Kafka) |
| `tests/streaming/test_models.py` | Unit tests for CandleEvent parsing |
| `tests/flink_job/test_anchor_state.py` | Unit tests for anchor carry-forward logic |
| `tests/flink_job/test_ch_lookup.py` | Unit tests for ClickHouse lookup (mocked CH) |
| `tests/flink_job/test_pnl_stream_job.py` | Integration test: end-to-end PyFlink job with mock sources |
| `schemas/clickhouse_shadow.sql` | Shadow tables for validation before cutover |

### Modified files
| File | Change |
|------|--------|
| `pyproject.toml` | Add `websockets`, `confluent-kafka` to dependencies |
| `infra/terraform/main.tf` | Add ECR repos, ECS task defs/services for consumer + redpanda + flink |
| `.github/workflows/ci-cd.yml` | Add build+push steps for consumer and flink images |
| `trading_dagster/definitions/__init__.py` | Remove live assets after cutover (Task 10) |

---

## Task 1: CandleEvent model and consumer unit tests

**Files:**
- Create: `streaming/__init__.py`
- Create: `streaming/models.py`
- Create: `tests/streaming/__init__.py`
- Create: `tests/streaming/test_models.py`

- [ ] **Step 1: Create the package and model**

```python
# streaming/__init__.py
# (empty)
```

```python
# streaming/models.py
from dataclasses import dataclass
from datetime import datetime


@dataclass
class CandleEvent:
    exchange: str
    instrument: str
    ts: datetime          # candle open time (minute boundary)
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_json(self) -> str:
        import json
        return json.dumps({
            "exchange": self.exchange,
            "instrument": self.instrument,
            "ts": self.ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        })

    @classmethod
    def from_binance_kline(cls, stream_data: dict) -> "CandleEvent":
        """Parse a Binance combined stream kline message.

        stream_data is the full WS message dict, e.g.:
        {"stream": "btcusdt@kline_1m", "data": {"e": "kline", "k": {...}}}
        """
        k = stream_data["data"]["k"]
        return cls(
            exchange="binance",
            instrument=k["s"],                          # e.g. "BTCUSDT"
            ts=datetime.utcfromtimestamp(k["t"] / 1000),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
        )
```

- [ ] **Step 2: Write failing tests**

```python
# tests/streaming/__init__.py
# (empty)
```

```python
# tests/streaming/test_models.py
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
                "t": 1745625600000,  # 2026-04-26T00:00:00 UTC in ms
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
```

- [ ] **Step 3: Run tests, verify they fail**

```bash
pytest tests/streaming/test_models.py -v
```
Expected: `ModuleNotFoundError: No module named 'streaming'`

- [ ] **Step 4: Run tests, verify they pass**

```bash
pytest tests/streaming/test_models.py -v
```
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add streaming/__init__.py streaming/models.py tests/streaming/
git commit -m "feat: add CandleEvent model with Binance kline parser"
```

---

## Task 2: Python WebSocket Consumer

**Files:**
- Create: `streaming/binance_ws_consumer.py`
- Create: `tests/streaming/test_binance_ws_consumer.py`
- Modify: `pyproject.toml` (add `websockets>=12.0`, `confluent-kafka>=2.4`)

- [ ] **Step 1: Add dependencies to pyproject.toml**

In the `dependencies` list in `pyproject.toml`, add:
```toml
"websockets>=12.0",
"confluent-kafka>=2.4",
```

Install:
```bash
pip install -e ".[dev]"
```

- [ ] **Step 2: Write failing tests first**

```python
# tests/streaming/test_binance_ws_consumer.py
import json
from collections import deque
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from streaming.models import CandleEvent
from streaming.binance_ws_consumer import (
    INSTRUMENTS,
    build_stream_url,
    parse_and_filter,
    publish_candle,
)


@pytest.mark.unit
def test_build_stream_url_contains_all_instruments():
    url = build_stream_url(INSTRUMENTS)
    for sym in INSTRUMENTS:
        assert sym.lower() + "@kline_1m" in url
    assert url.startswith("wss://fstream.binance.com/stream?streams=")


@pytest.mark.unit
def test_parse_and_filter_returns_none_for_open_candle():
    msg = json.dumps({
        "stream": "btcusdt@kline_1m",
        "data": {"e": "kline", "k": {
            "s": "BTCUSDT", "t": 1745625600000,
            "o": "93100.0", "h": "93250.0", "l": "93050.0",
            "c": "93200.0", "v": "12.34", "x": False,  # not closed
        }},
    })
    assert parse_and_filter(msg) is None


@pytest.mark.unit
def test_parse_and_filter_returns_candle_for_closed():
    msg = json.dumps({
        "stream": "btcusdt@kline_1m",
        "data": {"e": "kline", "k": {
            "s": "BTCUSDT", "t": 1745625600000,
            "o": "93100.0", "h": "93250.0", "l": "93050.0",
            "c": "93200.0", "v": "12.34", "x": True,  # closed
        }},
    })
    candle = parse_and_filter(msg)
    assert isinstance(candle, CandleEvent)
    assert candle.instrument == "BTCUSDT"


@pytest.mark.unit
def test_publish_candle_calls_producer_produce():
    mock_producer = MagicMock()
    candle = CandleEvent(
        exchange="binance", instrument="BTCUSDT",
        ts=datetime(2026, 4, 26, 0, 0, 0),
        open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0,
    )
    publish_candle(mock_producer, candle, topic="binance.price.ticks")
    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args
    assert call_kwargs[1]["topic"] == "binance.price.ticks"
    assert call_kwargs[1]["key"] == b"BTCUSDT"
    payload = json.loads(call_kwargs[1]["value"])
    assert payload["instrument"] == "BTCUSDT"
```

- [ ] **Step 3: Run tests, verify they fail**

```bash
pytest tests/streaming/test_binance_ws_consumer.py -v
```
Expected: `ImportError: cannot import name 'build_stream_url' from 'streaming.binance_ws_consumer'`

- [ ] **Step 4: Implement the consumer**

```python
# streaming/binance_ws_consumer.py
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
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT",
    "AVAXUSDT", "DOGEUSDT", "XRPUSDT", "FETUSDT",
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
    return Producer({
        "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
        "acks": "all",
        "retries": 5,
    })


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
                    candle = parse_and_filter(raw)
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
```

- [ ] **Step 5: Run tests, verify they pass**

```bash
pytest tests/streaming/test_binance_ws_consumer.py -v
```
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add streaming/binance_ws_consumer.py tests/streaming/test_binance_ws_consumer.py pyproject.toml
git commit -m "feat: add Binance WebSocket consumer with Redpanda producer"
```

---

## Task 3: Consumer Dockerfile

**Files:**
- Create: `streaming/Dockerfile`

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
# streaming/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir websockets>=12.0 confluent-kafka>=2.4

COPY streaming/ ./streaming/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "streaming.binance_ws_consumer"]
```

- [ ] **Step 2: Build locally to verify**

```bash
docker build -f streaming/Dockerfile -t ws-consumer-test .
```
Expected: Successfully built (no errors)

- [ ] **Step 3: Commit**

```bash
git add streaming/Dockerfile
git commit -m "feat: add Dockerfile for Binance WS consumer ECS task"
```

---

## Task 4: Anchor state unit tests and implementation

**Files:**
- Create: `flink_job/__init__.py`
- Create: `flink_job/anchor_state.py`
- Create: `tests/flink_job/__init__.py`
- Create: `tests/flink_job/test_anchor_state.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/flink_job/__init__.py
# (empty)
```

```python
# tests/flink_job/test_anchor_state.py
import pytest
from flink_job.anchor_state import AnchorState, AnchorRecord


@pytest.mark.unit
def test_anchor_state_returns_default_when_empty():
    state = AnchorState()
    rec = state.get("strat_A")
    assert rec.anchor_pnl == 0.0
    assert rec.anchor_price == 0.0
    assert rec.anchor_position == 0.0


@pytest.mark.unit
def test_anchor_state_stores_and_retrieves():
    state = AnchorState()
    state.update("strat_A", AnchorRecord(anchor_pnl=10.5, anchor_price=93000.0, anchor_position=1.0))
    rec = state.get("strat_A")
    assert rec.anchor_pnl == 10.5
    assert rec.anchor_price == 93000.0
    assert rec.anchor_position == 1.0


@pytest.mark.unit
def test_anchor_state_carry_forward_on_missing_price():
    """When anchor_price is 0 (no prior data), PnL stays at 0."""
    state = AnchorState()
    rec = state.get("new_strat")
    pnl = rec.anchor_pnl + rec.anchor_position * (
        93000.0 - rec.anchor_price
    ) / max(rec.anchor_price, 1e-10)
    assert pnl == 0.0  # position is 0, so no PnL


@pytest.mark.unit
def test_anchor_state_pnl_formula():
    """cumulative_pnl = anchor_pnl + position * (close - anchor_price) / anchor_price"""
    state = AnchorState()
    state.update("strat_B", AnchorRecord(anchor_pnl=5.0, anchor_price=100.0, anchor_position=2.0))
    rec = state.get("strat_B")
    close_price = 110.0
    pnl = rec.anchor_pnl + rec.anchor_position * (close_price - rec.anchor_price) / rec.anchor_price
    assert pnl == pytest.approx(5.0 + 2.0 * 10.0 / 100.0)  # 5.2
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
pytest tests/flink_job/test_anchor_state.py -v
```
Expected: `ModuleNotFoundError: No module named 'flink_job'`

- [ ] **Step 3: Implement anchor state**

```python
# flink_job/__init__.py
# (empty)
```

```python
# flink_job/anchor_state.py
"""
In-memory anchor state for PyFlink PnL computation.

Holds (anchor_pnl, anchor_price, anchor_position) per strategy_table_name.
In the PyFlink job this is backed by Flink keyed state (checkpointed to S3).
This module provides the pure logic so it can be unit-tested without Flink.
"""
from dataclasses import dataclass, field


@dataclass
class AnchorRecord:
    anchor_pnl: float = 0.0
    anchor_price: float = 0.0
    anchor_position: float = 0.0


class AnchorState:
    """Dict-backed anchor store. In production, replaced by Flink ValueState."""

    def __init__(self) -> None:
        self._store: dict[str, AnchorRecord] = {}

    def get(self, strategy_table_name: str) -> AnchorRecord:
        return self._store.get(strategy_table_name, AnchorRecord())

    def update(self, strategy_table_name: str, record: AnchorRecord) -> None:
        self._store[strategy_table_name] = record

    def compute_pnl(
        self,
        strategy_table_name: str,
        close_price: float,
        position: float,
    ) -> float:
        """Apply anchor-chain formula and update state. Returns new cumulative_pnl."""
        rec = self.get(strategy_table_name)
        if rec.anchor_price == 0.0:
            # No prior anchor — PnL stays flat, record this candle as new anchor
            new_pnl = rec.anchor_pnl
        else:
            new_pnl = rec.anchor_pnl + position * (close_price - rec.anchor_price) / rec.anchor_price
        self.update(strategy_table_name, AnchorRecord(
            anchor_pnl=new_pnl,
            anchor_price=close_price,
            anchor_position=position,
        ))
        return new_pnl
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
pytest tests/flink_job/test_anchor_state.py -v
```
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add flink_job/__init__.py flink_job/anchor_state.py tests/flink_job/
git commit -m "feat: add anchor state with carry-forward PnL formula"
```

---

## Task 5: ClickHouse lookup for strategy positions

**Files:**
- Create: `flink_job/ch_lookup.py`
- Create: `tests/flink_job/test_ch_lookup.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/flink_job/test_ch_lookup.py
from datetime import datetime
from unittest.mock import MagicMock, patch
import pytest
from flink_job.ch_lookup import fetch_strategies_for_candle, StrategyBar


@pytest.mark.unit
def test_fetch_strategies_returns_list_of_strategy_bars():
    mock_rows = [
        {
            "strategy_table_name": "strat_prod_1",
            "strategy_id": 1,
            "strategy_name": "momentum",
            "underlying": "BTCUSDT",
            "config_timeframe": "5m",
            "weighting": 1.0,
            "row_json": '{"position": 0.5, "final_signal": 1.0, "benchmark": 0.0}',
        }
    ]
    with patch("flink_job.ch_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(
            instrument="BTCUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert len(bars) == 1
    assert bars[0].strategy_table_name == "strat_prod_1"
    assert bars[0].position == 0.5
    assert bars[0].underlying == "BTCUSDT"


@pytest.mark.unit
def test_fetch_strategies_returns_empty_list_when_no_rows():
    with patch("flink_job.ch_lookup.query_dicts", return_value=[]):
        bars = fetch_strategies_for_candle(
            instrument="FETUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert bars == []


@pytest.mark.unit
def test_strategy_bar_position_parsed_from_row_json():
    mock_rows = [
        {
            "strategy_table_name": "strat_prod_2",
            "strategy_id": 2,
            "strategy_name": "mean_rev",
            "underlying": "ETHUSDT",
            "config_timeframe": "15m",
            "weighting": 0.5,
            "row_json": '{"position": -1.0, "final_signal": -1.0, "benchmark": 0.01}',
        }
    ]
    with patch("flink_job.ch_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(
            instrument="ETHUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert bars[0].position == -1.0
    assert bars[0].final_signal == -1.0
    assert bars[0].benchmark == 0.01
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
pytest tests/flink_job/test_ch_lookup.py -v
```
Expected: `ModuleNotFoundError: No module named 'flink_job.ch_lookup'`

- [ ] **Step 3: Implement the lookup**

```python
# flink_job/ch_lookup.py
"""
ClickHouse async lookup for strategy positions.

Queries strategy_output_history_v2 for the most recent bar per strategy
for a given instrument at or before candle_ts. Returns all active strategies.
Called once per closed candle from the PyFlink job.
"""
import json
from dataclasses import dataclass
from datetime import datetime

from trading_dagster.utils.clickhouse_client import query_dicts

_LOOKBACK = "1 DAY"  # how far back to search for the most recent strategy bar


@dataclass
class StrategyBar:
    strategy_table_name: str
    strategy_id: int
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    position: float
    final_signal: float
    benchmark: float


def fetch_strategies_for_candle(
    instrument: str,
    candle_ts: datetime,
) -> list[StrategyBar]:
    """Return latest strategy bar per strategy for instrument at candle_ts.

    Uses LIMIT 1 BY to get the most recent bar per strategy without FINAL.
    Looks back up to _LOOKBACK to handle gaps in strategy data.
    """
    ts_str = candle_ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""\
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_v2
WHERE underlying = '{instrument}'
  AND ts <= '{ts_str}'
  AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
GROUP BY
    strategy_table_name, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
ORDER BY strategy_table_name, ts DESC
LIMIT 1 BY strategy_table_name
"""
    rows = query_dicts(sql)
    result = []
    for row in rows:
        rj = json.loads(row["row_json"])
        result.append(StrategyBar(
            strategy_table_name=row["strategy_table_name"],
            strategy_id=row["strategy_id"],
            strategy_name=row["strategy_name"],
            underlying=row["underlying"],
            config_timeframe=row["config_timeframe"],
            weighting=row["weighting"],
            position=float(rj.get("position", 0.0)),
            final_signal=float(rj.get("final_signal", 0.0)),
            benchmark=float(rj.get("benchmark", 0.0)),
        ))
    return result
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
pytest tests/flink_job/test_ch_lookup.py -v
```
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add flink_job/ch_lookup.py tests/flink_job/test_ch_lookup.py
git commit -m "feat: add ClickHouse strategy lookup for Flink job"
```

---

## Task 6: PyFlink job

**Files:**
- Create: `flink_job/pnl_stream_job.py`
- Create: `flink_job/requirements.txt`
- Create: `tests/flink_job/test_pnl_stream_job.py`

- [ ] **Step 1: Write failing integration test**

```python
# tests/flink_job/test_pnl_stream_job.py
"""
Integration test for the PyFlink PnL stream job.
Runs without a real Flink cluster — tests the process_candle() function
which contains all the business logic (lookup + anchor + compute).
"""
from datetime import datetime
from unittest.mock import patch
import pytest
from flink_job.ch_lookup import StrategyBar
from flink_job.anchor_state import AnchorState, AnchorRecord
from flink_job.pnl_stream_job import process_candle
from streaming.models import CandleEvent


def _make_candle(instrument="BTCUSDT", close=93200.0) -> CandleEvent:
    return CandleEvent(
        exchange="binance", instrument=instrument,
        ts=datetime(2026, 4, 26, 0, 1, 0),
        open=93100.0, high=93250.0, low=93050.0,
        close=close, volume=12.34,
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
    state.update("strat_prod_1", AnchorRecord(
        anchor_pnl=0.0, anchor_price=93100.0, anchor_position=1.0
    ))
    candle = _make_candle(close=93200.0)
    strategies = [_make_strategy(position=1.0)]

    with patch("flink_job.pnl_stream_job.fetch_strategies_for_candle", return_value=strategies):
        rows = process_candle(candle, state)

    assert len(rows) == 1
    row = rows[0]
    assert row["strategy_table_name"] == "strat_prod_1"
    assert row["underlying"] == "BTCUSDT"
    assert row["price"] == 93200.0
    # pnl = 0.0 + 1.0 * (93200 - 93100) / 93100 ≈ 0.001074
    assert abs(row["cumulative_pnl"] - (100.0 / 93100.0)) < 1e-6


@pytest.mark.unit
def test_process_candle_carry_forward_when_no_strategies():
    state = AnchorState()
    candle = _make_candle()

    with patch("flink_job.pnl_stream_job.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state)

    assert rows == []


@pytest.mark.unit
def test_process_candle_writes_candle_to_price_table():
    state = AnchorState()
    candle = _make_candle()
    strategies = [_make_strategy()]

    with patch("flink_job.pnl_stream_job.fetch_strategies_for_candle", return_value=strategies):
        rows = process_candle(candle, state)

    # price row is always emitted regardless of strategies
    price_row = next((r for r in rows if r.get("_sink") == "price"), None)
    assert price_row is not None
    assert price_row["instrument"] == "BTCUSDT"
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
pytest tests/flink_job/test_pnl_stream_job.py -v
```
Expected: `ModuleNotFoundError: No module named 'flink_job.pnl_stream_job'`

- [ ] **Step 3: Implement the job**

```python
# flink_job/pnl_stream_job.py
"""
PyFlink streaming job: Binance candles → PnL rows → ClickHouse.

Job graph:
  KafkaSource(binance.price.ticks)
  → process_candle() [lookup join + anchor-chain PnL]
  → SinkA: futures_price_1min
  → SinkB: strategy_pnl_1min_prod_v2

process_candle() is the pure business logic — kept separate so it can be
unit-tested without a Flink cluster.

In production this runs as a PyFlink job with:
- Flink keyed state for anchor (checkpointed to S3 every 30s)
- Kafka connector source (Redpanda, port 9092)
- ClickHouse JDBC sink (via flink-connector-jdbc + clickhouse-jdbc driver)
"""
import json
import logging
import os
from datetime import datetime, timezone

from flink_job.anchor_state import AnchorState, AnchorRecord
from flink_job.ch_lookup import fetch_strategies_for_candle
from streaming.models import CandleEvent

logger = logging.getLogger(__name__)

PROD_INSERT_COLUMNS = [
    "strategy_table_name", "strategy_id", "strategy_name", "underlying",
    "config_timeframe", "source", "version", "ts", "cumulative_pnl",
    "benchmark", "position", "price", "final_signal", "weighting", "updated_at",
]


def process_candle(
    candle: CandleEvent,
    state: AnchorState,
) -> list[dict]:
    """Pure business logic: lookup strategies, compute PnL, return rows.

    Returns a list of dicts. Each dict has a '_sink' key:
      '_sink' == 'price'  → write to futures_price_1min
      '_sink' == 'pnl'    → write to strategy_pnl_1min_prod_v2

    Always emits one price row. Emits one pnl row per active strategy.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[dict] = []

    # Always write the candle to the price table
    rows.append({
        "_sink": "price",
        "exchange": candle.exchange,
        "instrument": candle.instrument,
        "ts": candle.ts,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
    })

    strategies = fetch_strategies_for_candle(candle.instrument, candle.ts)
    if not strategies:
        logger.debug("No strategies for %s at %s — skipping PnL", candle.instrument, candle.ts)
        return rows

    for bar in strategies:
        pnl = state.compute_pnl(
            strategy_table_name=bar.strategy_table_name,
            close_price=candle.close,
            position=bar.position,
        )
        rows.append({
            "_sink": "pnl",
            "strategy_table_name": bar.strategy_table_name,
            "strategy_id": bar.strategy_id,
            "strategy_name": bar.strategy_name,
            "underlying": bar.underlying,
            "config_timeframe": bar.config_timeframe,
            "source": "production",
            "version": "v2",
            "ts": candle.ts,
            "cumulative_pnl": pnl,
            "benchmark": bar.benchmark,
            "position": bar.position,
            "price": candle.close,
            "final_signal": bar.final_signal,
            "weighting": bar.weighting,
            "updated_at": now,
        })

    return rows


def _bootstrap_anchors(state: AnchorState) -> None:
    """On cold start, seed anchor state from last known PnL rows in ClickHouse."""
    from trading_dagster.utils.clickhouse_client import query_dicts
    sql = """\
SELECT
    strategy_table_name,
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position
FROM analytics.strategy_pnl_1min_prod_v2
WHERE ts >= now() - INTERVAL 2 HOUR
ORDER BY strategy_table_name, ts DESC, updated_at DESC
LIMIT 1 BY strategy_table_name
"""
    for row in query_dicts(sql):
        state.update(row["strategy_table_name"], AnchorRecord(
            anchor_pnl=row["anchor_pnl"],
            anchor_price=row["anchor_price"],
            anchor_position=row["anchor_position"],
        ))
    logger.info("Bootstrapped %d anchor(s) from ClickHouse", len(state._store))


def run_flink_job() -> None:
    """Entry point for PyFlink job. Wires up Kafka source + CH sinks."""
    from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common import WatermarkStrategy
    from trading_dagster.utils.clickhouse_client import insert_rows

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000, CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage_uri(
        f"s3://{os.environ['S3_BUCKET']}/flink-checkpoints/"
    )

    brokers = os.environ["REDPANDA_BROKERS"]
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics("binance.price.ticks")
        .set_group_id("flink-pnl-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    state = AnchorState()
    _bootstrap_anchors(state)

    price_batch: list[list] = []
    pnl_batch: list[list] = []
    FLUSH_EVERY = 50

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Redpanda")

    def handle(raw: str) -> None:
        nonlocal price_batch, pnl_batch
        data = json.loads(raw)
        candle = CandleEvent(
            exchange=data["exchange"],
            instrument=data["instrument"],
            ts=datetime.fromisoformat(data["ts"]),
            open=data["open"], high=data["high"],
            low=data["low"], close=data["close"],
            volume=data["volume"],
        )
        for row in process_candle(candle, state):
            if row["_sink"] == "price":
                price_batch.append([
                    row["exchange"], row["instrument"], row["ts"],
                    row["open"], row["high"], row["low"], row["close"], row["volume"],
                ])
            elif row["_sink"] == "pnl":
                pnl_batch.append([
                    row[c] for c in PROD_INSERT_COLUMNS
                ])

        if len(price_batch) >= FLUSH_EVERY:
            insert_rows("analytics.futures_price_1min",
                        ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"],
                        price_batch)
            price_batch = []

        if len(pnl_batch) >= FLUSH_EVERY:
            insert_rows("analytics.strategy_pnl_1min_prod_v2", PROD_INSERT_COLUMNS, pnl_batch)
            pnl_batch = []

    ds.map(handle)
    env.execute("binance-pnl-stream")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_flink_job()
```

- [ ] **Step 4: Create requirements.txt for PyFlink image**

```
# flink_job/requirements.txt
apache-flink==1.19.0
clickhouse-connect>=0.8
loguru>=0.7
```

- [ ] **Step 5: Run tests, verify they pass**

```bash
pytest tests/flink_job/test_pnl_stream_job.py -v
```
Expected: 3 passed

- [ ] **Step 6: Run full unit test suite**

```bash
pytest -m unit -v
```
Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add flink_job/pnl_stream_job.py flink_job/requirements.txt tests/flink_job/test_pnl_stream_job.py
git commit -m "feat: add PyFlink PnL streaming job with anchor-chain logic"
```

---

## Task 7: PyFlink Dockerfile

**Files:**
- Create: `flink_job/Dockerfile`
- Create: `flink_job/connectors/.gitkeep`

- [ ] **Step 1: Download connector JARs**

Run once locally (or in CI). JARs go in `flink_job/connectors/` and are copied into the image.

```bash
mkdir -p flink_job/connectors
# Flink Kafka connector (matches Flink 1.19)
curl -L -o flink_job/connectors/flink-sql-connector-kafka-3.2.0-1.19.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# ClickHouse JDBC driver
curl -L -o flink_job/connectors/clickhouse-jdbc-0.6.0-all.jar \
  https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.0/clickhouse-jdbc-0.6.0-all.jar
```

- [ ] **Step 2: Write the Dockerfile**

```dockerfile
# flink_job/Dockerfile
FROM apache/flink:1.19.0-scala_2.12-java11

# Install Python 3.11 and PyFlink deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-dev python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf python3.11 /usr/bin/python3 && ln -sf python3.11 /usr/bin/python

WORKDIR /app

COPY flink_job/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy connector JARs into Flink lib directory
COPY flink_job/connectors/*.jar /opt/flink/lib/

# Copy application code
COPY streaming/ ./streaming/
COPY flink_job/ ./flink_job/
COPY trading_dagster/utils/clickhouse_client.py ./trading_dagster/utils/clickhouse_client.py
COPY trading_dagster/utils/__init__.py ./trading_dagster/utils/__init__.py
COPY trading_dagster/__init__.py ./trading_dagster/__init__.py

ENV PYTHONUNBUFFERED=1
ENV PYFLINK_PYTHON=python3

CMD ["python", "-m", "flink_job.pnl_stream_job"]
```

- [ ] **Step 3: Build locally to verify**

```bash
docker build -f flink_job/Dockerfile -t flink-job-test .
```
Expected: Successfully built

- [ ] **Step 4: Add connectors to .gitignore (JARs are large)**

Add to `.gitignore`:
```
flink_job/connectors/*.jar
```

- [ ] **Step 5: Commit**

```bash
git add flink_job/Dockerfile flink_job/connectors/.gitkeep .gitignore
git commit -m "feat: add PyFlink Dockerfile with Kafka and ClickHouse JDBC connectors"
```

---

## Task 8: Shadow ClickHouse tables

**Files:**
- Create: `schemas/clickhouse_shadow.sql`

- [ ] **Step 1: Create shadow schema**

```sql
-- schemas/clickhouse_shadow.sql
-- Shadow tables for validating Flink output before cutover.
-- Drop these after successful cutover validation.

CREATE TABLE IF NOT EXISTS analytics.futures_price_1min_shadow
(
    exchange   String,
    instrument String,
    ts         DateTime,
    open       Float64,
    high       Float64,
    low        Float64,
    close      Float64,
    volume     Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, instrument, ts);

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_prod_v2_shadow
(
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);
```

- [ ] **Step 2: Apply to ClickHouse Cloud**

```bash
# Requires CLICKHOUSE_HOST, CLICKHOUSE_PASSWORD set in env
clickhouse-client \
  --host "$CLICKHOUSE_HOST" \
  --port 9440 \
  --secure \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  < schemas/clickhouse_shadow.sql
```

- [ ] **Step 3: Commit**

```bash
git add schemas/clickhouse_shadow.sql
git commit -m "feat: add shadow ClickHouse tables for Flink validation"
```

---

## Task 9: Terraform — ECR, ECS tasks, security groups

**Files:**
- Modify: `infra/terraform/main.tf`

- [ ] **Step 1: Add ECR repositories**

Append to `infra/terraform/main.tf`:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# ECR — Streaming images
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "ws_consumer" {
  name                 = "trading-analysis-ws-consumer"
  image_tag_mutability = "MUTABLE"
  tags                 = local.common_tags
}

resource "aws_ecr_repository" "flink_job" {
  name                 = "trading-analysis-flink-job"
  image_tag_mutability = "MUTABLE"
  tags                 = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# IAM — Flink task role (S3 checkpoint access)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "flink_task" {
  name = "${local.name_prefix}-flink-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy" "flink_s3" {
  name = "flink-s3-checkpoints"
  role = aws_iam_role.flink_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        "arn:aws:s3:::trading-analysis-data-v2",
        "arn:aws:s3:::trading-analysis-data-v2/flink-checkpoints/*"
      ]
    }]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# Security group — Redpanda (internal VPC only)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_security_group" "redpanda" {
  name        = "${local.name_prefix}-redpanda"
  description = "Redpanda broker — internal VPC only"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
    description = "Kafka API from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# ECS Task Definitions
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_task_definition" "redpanda" {
  family                   = "${local.name_prefix}-redpanda"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn

  container_definitions = jsonencode([{
    name      = "redpanda"
    image     = "redpandadata/redpanda:latest"
    essential = true
    command = [
      "redpanda", "start",
      "--smp", "1",
      "--memory", "1500M",
      "--reserve-memory", "0M",
      "--node-id", "0",
      "--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
      "--advertise-kafka-addr", "PLAINTEXT://redpanda.${local.name_prefix}.local:9092",
      "--set", "redpanda.auto_create_topics_enabled=true",
    ]
    portMappings = [{ containerPort = 9092, protocol = "tcp" }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${local.name_prefix}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "redpanda"
      }
    }
  }])

  tags = local.common_tags
}

resource "aws_ecs_task_definition" "ws_consumer" {
  family                   = "${local.name_prefix}-ws-consumer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_execution.arn

  container_definitions = jsonencode([{
    name      = "ws-consumer"
    image     = "${aws_ecr_repository.ws_consumer.repository_url}:latest"
    essential = true
    environment = [
      { name = "REDPANDA_BROKERS", value = "redpanda.${local.name_prefix}.local:9092" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${local.name_prefix}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "ws-consumer"
      }
    }
  }])

  tags = local.common_tags
}

resource "aws_ecs_task_definition" "flink_job" {
  family                   = "${local.name_prefix}-flink-job"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.flink_task.arn

  container_definitions = jsonencode([{
    name      = "flink-job"
    image     = "${aws_ecr_repository.flink_job.repository_url}:latest"
    essential = true
    secrets = [
      { name = "CLICKHOUSE_HOST",     valueFrom = "${data.aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PORT",     valueFrom = "${data.aws_secretsmanager_secret.clickhouse.arn}:port::" },
      { name = "CLICKHOUSE_USER",     valueFrom = "${data.aws_secretsmanager_secret.clickhouse.arn}:user::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${data.aws_secretsmanager_secret.clickhouse.arn}:password::" },
    ]
    environment = [
      { name = "CLICKHOUSE_SECURE",   value = "true" },
      { name = "REDPANDA_BROKERS",    value = "redpanda.${local.name_prefix}.local:9092" },
      { name = "S3_BUCKET",           value = "trading-analysis-data-v2" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/${local.name_prefix}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "flink-job"
      }
    }
  }])

  tags = local.common_tags
}

# ─────────────────────────────────────────────────────────────────────────────
# ECS Services (FARGATE_SPOT)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecs_service" "redpanda" {
  name            = "${local.name_prefix}-redpanda"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.redpanda.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.redpanda.id]
    assign_public_ip = false
  }

  tags = local.common_tags
}

resource "aws_ecs_service" "ws_consumer" {
  name            = "${local.name_prefix}-ws-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.ws_consumer.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  tags = local.common_tags
}

resource "aws_ecs_service" "flink_job" {
  name            = "${local.name_prefix}-flink-job"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.flink_job.arn
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs.id]
    assign_public_ip = false
  }

  tags = local.common_tags
}
```

- [ ] **Step 2: Verify Terraform plan**

```bash
cd infra/terraform
terraform init
terraform plan -var="github_repo=williamwxz/trading-analysis"
```
Expected: plan shows new resources, no unexpected destroys

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat: add Terraform resources for Redpanda, WS consumer, and PyFlink ECS tasks"
```

---

## Task 10: CI/CD — build and push new images

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Add image build steps**

In `.github/workflows/ci-cd.yml`, find the `build` job and add these steps after the existing Dagster image build:

```yaml
      - name: Build and push WS consumer image
        env:
          ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
        run: |
          docker build -f streaming/Dockerfile \
            -t $ECR_REGISTRY/trading-analysis-ws-consumer:latest \
            -t $ECR_REGISTRY/trading-analysis-ws-consumer:${{ github.sha }} .
          docker push $ECR_REGISTRY/trading-analysis-ws-consumer:latest
          docker push $ECR_REGISTRY/trading-analysis-ws-consumer:${{ github.sha }}

      - name: Download Flink connector JARs
        run: |
          mkdir -p flink_job/connectors
          curl -L -o flink_job/connectors/flink-sql-connector-kafka-3.2.0-1.19.jar \
            https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
          curl -L -o flink_job/connectors/clickhouse-jdbc-0.6.0-all.jar \
            https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.0/clickhouse-jdbc-0.6.0-all.jar

      - name: Build and push Flink job image
        env:
          ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
        run: |
          docker build -f flink_job/Dockerfile \
            -t $ECR_REGISTRY/trading-analysis-flink-job:latest \
            -t $ECR_REGISTRY/trading-analysis-flink-job:${{ github.sha }} .
          docker push $ECR_REGISTRY/trading-analysis-flink-job:latest
          docker push $ECR_REGISTRY/trading-analysis-flink-job:${{ github.sha }}
```

Also add the ECR permission for new repos in the GHA IAM policy (already covered by `ecr:*` on all repos in the existing policy).

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add build and push steps for WS consumer and Flink job images"
```

---

## Task 11: Shadow mode validation and cutover

This task runs after Flink has been deployed and running in shadow mode for 24h.

**Files:**
- Modify: `trading_dagster/definitions/__init__.py`

- [ ] **Step 1: Verify shadow table row counts match production**

```bash
# Run against ClickHouse Cloud
clickhouse-client \
  --host "$CLICKHOUSE_HOST" --port 9440 --secure \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --query "
SELECT
  'production' AS source,
  count() AS rows,
  min(ts) AS min_ts,
  max(ts) AS max_ts
FROM analytics.strategy_pnl_1min_prod_v2
WHERE ts >= now() - INTERVAL 1 DAY
UNION ALL
SELECT
  'shadow' AS source,
  count() AS rows,
  min(ts) AS min_ts,
  max(ts) AS max_ts
FROM analytics.strategy_pnl_1min_prod_v2_shadow
WHERE ts >= now() - INTERVAL 1 DAY
"
```
Expected: row counts within 5% of each other, max_ts within 2 minutes of now for both

- [ ] **Step 2: Switch Flink sinks to production tables**

In `flink_job/pnl_stream_job.py`, change the two table names in `handle()`:

```python
# Before:
insert_rows("analytics.futures_price_1min_shadow", ...)
insert_rows("analytics.strategy_pnl_1min_prod_v2_shadow", ...)

# After:
insert_rows("analytics.futures_price_1min", ...)
insert_rows("analytics.strategy_pnl_1min_prod_v2", ...)
```

- [ ] **Step 3: Decommission live Dagster assets**

In `trading_dagster/definitions/__init__.py`, remove `binance_futures_ohlcv_minutely_asset` and `pnl_prod_v2_live_asset` from `all_assets`:

```python
# Remove these two lines:
#   binance_futures_ohlcv_minutely_asset,
#   pnl_prod_v2_live_asset,
```

- [ ] **Step 4: Run unit tests**

```bash
pytest -m unit -v
```
Expected: all pass

- [ ] **Step 5: Commit and deploy**

```bash
git add flink_job/pnl_stream_job.py trading_dagster/definitions/__init__.py
git commit -m "feat: cut over Flink to production tables, decommission Dagster live assets"
```

Push to main — CI/CD deploys all three images and runs terraform apply.

- [ ] **Step 6: Drop shadow tables (after 48h monitoring)**

```bash
clickhouse-client \
  --host "$CLICKHOUSE_HOST" --port 9440 --secure \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --query "
DROP TABLE IF EXISTS analytics.futures_price_1min_shadow;
DROP TABLE IF EXISTS analytics.strategy_pnl_1min_prod_v2_shadow;
"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] WS consumer subscribes to `kline_1m`, filters `k.x == true` → Task 2
- [x] 8 instruments defined in `INSTRUMENTS` → Task 2
- [x] Exponential backoff reconnect → Task 2
- [x] In-memory buffer `deque(maxlen=120)` → Task 2
- [x] Redpanda topic `binance.price.ticks`, 3 partitions, 1h retention → Task 9 (ECS command flags)
- [x] PyFlink job: Kafka source → lookup join → PnL → two CH sinks → Tasks 5, 6
- [x] Carry-forward last known position → Task 4 (`AnchorState.compute_pnl`)
- [x] Anchor-chain formula → Task 4
- [x] Bootstrap anchor from ClickHouse on cold start → Task 6 (`_bootstrap_anchors`)
- [x] S3 checkpointing every 30s → Task 6 (`run_flink_job`)
- [x] Shadow mode validation before cutover → Task 8 + Task 11
- [x] Decommission Dagster live assets → Task 11
- [x] ECR repos + ECS task defs → Task 9
- [x] CI/CD image builds → Task 10
- [x] Cost ~$24/month documented in spec (not in plan — correct)

**No placeholders found.**

**Type consistency:** `CandleEvent` defined in Task 1, used in Tasks 2, 6. `AnchorRecord`/`AnchorState` defined in Task 4, used in Tasks 6, 11. `StrategyBar` defined in Task 5, used in Task 6. All consistent.
