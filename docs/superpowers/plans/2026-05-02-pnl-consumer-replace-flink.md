# Replace PyFlink Job with Plain Python PnL Consumer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the PyFlink ECS job with a plain Python `confluent-kafka` consumer loop, rename everything `flink` → `pnl-consumer` throughout the repo.

**Architecture:** A single `confluent-kafka` consumer loop in `pnl_consumer/pnl_consumer.py` replaces the PyFlink runner. All business logic files (`anchor_state.py`, `ch_lookup.py`, `process_candle()`, `_bootstrap_anchors()`) move verbatim from `flink_job/` to `pnl_consumer/`. The Dockerfile swaps `apache/flink` base for `python:3.11-slim`. Terraform, CI/CD, and tests are updated to use the new name.

**Tech Stack:** Python 3.11, confluent-kafka>=2.4, clickhouse-connect>=0.8, python:3.11-slim Docker base, AWS ECS Fargate, Terraform, GitHub Actions.

---

## File Map

| Action | Path |
|--------|------|
| Create | `pnl_consumer/__init__.py` |
| Create | `pnl_consumer/anchor_state.py` (moved from `flink_job/anchor_state.py`) |
| Create | `pnl_consumer/ch_lookup.py` (moved from `flink_job/ch_lookup.py`) |
| Create | `pnl_consumer/pnl_consumer.py` (replaces `flink_job/pnl_stream_job.py`) |
| Create | `pnl_consumer/Dockerfile` |
| Create | `pnl_consumer/requirements.txt` |
| Create | `tests/pnl_consumer/__init__.py` |
| Create | `tests/pnl_consumer/test_anchor_state.py` (moved, imports updated) |
| Create | `tests/pnl_consumer/test_ch_lookup.py` (moved, imports updated) |
| Create | `tests/pnl_consumer/test_pnl_consumer.py` (moved from `test_pnl_stream_job.py`, imports updated) |
| Delete | `flink_job/` (entire directory) |
| Delete | `tests/flink_job/` (entire directory) |
| Modify | `infra/terraform/main.tf` |
| Modify | `.github/workflows/ci-cd.yml` |

---

## Task 1: Create `pnl_consumer/` package with moved logic files

**Files:**
- Create: `pnl_consumer/__init__.py`
- Create: `pnl_consumer/anchor_state.py`
- Create: `pnl_consumer/ch_lookup.py`

- [ ] **Step 1: Create the package directory and `__init__.py`**

```bash
mkdir pnl_consumer
touch pnl_consumer/__init__.py
```

- [ ] **Step 2: Copy `anchor_state.py` verbatim**

Create `pnl_consumer/anchor_state.py` with this exact content:

```python
from dataclasses import dataclass


@dataclass
class AnchorRecord:
    anchor_pnl: float = 0.0
    anchor_price: float = 0.0
    anchor_position: float = 0.0


class AnchorState:
    """Dict-backed anchor store."""

    def __init__(self) -> None:
        self._store: dict[str, AnchorRecord] = {}

    def get(self, strategy_table_name: str) -> AnchorRecord:
        return self._store.get(strategy_table_name, AnchorRecord())

    def update(self, strategy_table_name: str, record: AnchorRecord) -> None:
        self._store[strategy_table_name] = record

    def __len__(self) -> int:
        return len(self._store)

    def compute_pnl(
        self,
        strategy_table_name: str,
        close_price: float,
        position: float,
    ) -> float:
        """Apply anchor-chain formula and update state. Returns new cumulative_pnl."""
        rec = self.get(strategy_table_name)
        if rec.anchor_price == 0.0:
            new_pnl = rec.anchor_pnl
        else:
            new_pnl = (
                rec.anchor_pnl
                + position * (close_price - rec.anchor_price) / rec.anchor_price
            )
        self.update(strategy_table_name, AnchorRecord(
            anchor_pnl=new_pnl,
            anchor_price=close_price,
            anchor_position=position,
        ))
        return new_pnl
```

- [ ] **Step 3: Copy `ch_lookup.py` with updated import**

Create `pnl_consumer/ch_lookup.py` — identical to `flink_job/ch_lookup.py` except the module docstring reference to "PyFlink" is removed:

```python
import json
from dataclasses import dataclass
from datetime import datetime

from trading_dagster.utils.clickhouse_client import query_dicts

_LOOKBACK = "1 DAY"


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
    max(ts) AS latest_ts,
    argMin(row_json, revision_ts) AS row_json
FROM analytics.strategy_output_history_v2
WHERE underlying = '{instrument}'
  AND ts <= '{ts_str}'
  AND ts >= '{ts_str}'::DateTime - INTERVAL {_LOOKBACK}
GROUP BY
    strategy_table_name, strategy_id, strategy_name,
    underlying, config_timeframe, weighting
ORDER BY strategy_table_name, latest_ts DESC
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

- [ ] **Step 4: Commit**

```bash
git add pnl_consumer/
git commit -m "feat: add pnl_consumer package with anchor_state and ch_lookup"
```

---

## Task 2: Migrate tests to `tests/pnl_consumer/`

**Files:**
- Create: `tests/pnl_consumer/__init__.py`
- Create: `tests/pnl_consumer/test_anchor_state.py`
- Create: `tests/pnl_consumer/test_ch_lookup.py`

- [ ] **Step 1: Create test package**

```bash
mkdir tests/pnl_consumer
touch tests/pnl_consumer/__init__.py
```

- [ ] **Step 2: Create `tests/pnl_consumer/test_anchor_state.py`**

Same tests as `tests/flink_job/test_anchor_state.py`, import path updated:

```python
import pytest
from pnl_consumer.anchor_state import AnchorState, AnchorRecord


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
    state = AnchorState()
    pnl = state.compute_pnl("new_strat", close_price=93000.0, position=1.0)
    assert pnl == 0.0
    rec = state.get("new_strat")
    assert rec.anchor_price == 93000.0
    assert rec.anchor_position == 1.0


@pytest.mark.unit
def test_anchor_state_pnl_formula():
    state = AnchorState()
    state.update("strat_B", AnchorRecord(anchor_pnl=5.0, anchor_price=100.0, anchor_position=2.0))
    pnl = state.compute_pnl("strat_B", close_price=110.0, position=2.0)
    # 5.0 + 2.0 * (110 - 100) / 100 = 5.2
    assert pnl == pytest.approx(5.2)
    rec = state.get("strat_B")
    assert rec.anchor_pnl == pytest.approx(5.2)
    assert rec.anchor_price == 110.0
    assert rec.anchor_position == 2.0
```

- [ ] **Step 3: Run anchor_state tests**

```bash
pytest tests/pnl_consumer/test_anchor_state.py -v -m unit
```

Expected: 4 tests PASS.

- [ ] **Step 4: Create `tests/pnl_consumer/test_ch_lookup.py`**

Same tests as `tests/flink_job/test_ch_lookup.py`, import path updated:

```python
from datetime import datetime
from unittest.mock import patch

import pytest

from pnl_consumer.ch_lookup import fetch_strategies_for_candle


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
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=mock_rows):
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
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=[]):
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
    with patch("pnl_consumer.ch_lookup.query_dicts", return_value=mock_rows):
        bars = fetch_strategies_for_candle(
            instrument="ETHUSDT",
            candle_ts=datetime(2026, 4, 26, 0, 0, 0),
        )
    assert bars[0].position == -1.0
    assert bars[0].final_signal == -1.0
    assert bars[0].benchmark == 0.01
```

- [ ] **Step 5: Run ch_lookup tests**

```bash
pytest tests/pnl_consumer/test_ch_lookup.py -v -m unit
```

Expected: 3 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/pnl_consumer/
git commit -m "test: migrate anchor_state and ch_lookup tests to pnl_consumer"
```

---

## Task 3: Write `pnl_consumer/pnl_consumer.py` and its tests

**Files:**
- Create: `pnl_consumer/pnl_consumer.py`
- Create: `tests/pnl_consumer/test_pnl_consumer.py`

- [ ] **Step 1: Write the failing test first**

Create `tests/pnl_consumer/test_pnl_consumer.py`:

```python
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import StrategyBar
from pnl_consumer.pnl_consumer import process_candle
from streaming.models import CandleEvent


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
```

- [ ] **Step 2: Run tests — verify they FAIL**

```bash
pytest tests/pnl_consumer/test_pnl_consumer.py -v -m unit
```

Expected: ImportError — `pnl_consumer.pnl_consumer` does not exist yet.

- [ ] **Step 3: Create `pnl_consumer/pnl_consumer.py`**

```python
import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import fetch_strategies_for_candle
from streaming.models import CandleEvent
from trading_dagster.utils.clickhouse_client import insert_rows
from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

logger = logging.getLogger(__name__)

FLUSH_EVERY = 50
TOPIC = "binance.price.ticks"
GROUP_ID = "flink-pnl-consumer"

PRICE_COLUMNS = ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"]


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
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []

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
        state.update(
            row["strategy_table_name"],
            AnchorRecord(
                anchor_pnl=row["anchor_pnl"],
                anchor_price=row["anchor_price"],
                anchor_position=row["anchor_position"],
            ),
        )
    logger.info("Bootstrapped %d anchor(s) from ClickHouse", len(state))


def _flush(
    consumer: Consumer,
    price_batch: list[list],
    pnl_batch: list[list],
) -> None:
    """Insert batches to ClickHouse, then commit offsets. Raises on failure."""
    if price_batch:
        insert_rows("analytics.futures_price_1min", PRICE_COLUMNS, price_batch)
        price_batch.clear()
    if pnl_batch:
        insert_rows("analytics.strategy_pnl_1min_prod_v2", PROD_INSERT_COLUMNS, pnl_batch)
        pnl_batch.clear()
    consumer.commit(asynchronous=False)


def run() -> None:
    logging.basicConfig(level=logging.INFO)

    state = AnchorState()
    _bootstrap_anchors(state)

    consumer = Consumer({
        "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    logger.info("Subscribed to %s as group %s", TOPIC, GROUP_ID)

    price_batch: list[list] = []
    pnl_batch: list[list] = []

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, flushing and closing")
        try:
            _flush(consumer, price_batch, pnl_batch)
        except Exception:
            logger.exception("Error during shutdown flush")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                logger.warning("Kafka error: %s", msg.error())
                continue

            data = json.loads(msg.value().decode())
            candle = CandleEvent(
                exchange=data["exchange"],
                instrument=data["instrument"],
                ts=datetime.fromisoformat(data["ts"]),
                open=data["open"],
                high=data["high"],
                low=data["low"],
                close=data["close"],
                volume=data["volume"],
            )

            for row in process_candle(candle, state):
                if row["_sink"] == "price":
                    price_batch.append([
                        row["exchange"], row["instrument"], row["ts"],
                        row["open"], row["high"], row["low"],
                        row["close"], row["volume"],
                    ])
                elif row["_sink"] == "pnl":
                    pnl_batch.append([row[c] for c in PROD_INSERT_COLUMNS])

            if len(price_batch) >= FLUSH_EVERY or len(pnl_batch) >= FLUSH_EVERY:
                n_price, n_pnl = len(price_batch), len(pnl_batch)
                _flush(consumer, price_batch, pnl_batch)
                logger.info("Flushed %d price + %d pnl rows", n_price, n_pnl)

    except Exception:
        logger.exception("Fatal error in consumer loop")
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    run()
```

- [ ] **Step 4: Run tests — verify they PASS**

```bash
pytest tests/pnl_consumer/test_pnl_consumer.py -v -m unit
```

Expected: 3 tests PASS.

- [ ] **Step 5: Run all pnl_consumer tests together**

```bash
pytest tests/pnl_consumer/ -v -m unit
```

Expected: 10 tests PASS (4 anchor + 3 ch_lookup + 3 pnl_consumer).

- [ ] **Step 6: Commit**

```bash
git add pnl_consumer/pnl_consumer.py tests/pnl_consumer/test_pnl_consumer.py
git commit -m "feat: add pnl_consumer loop and tests"
```

---

## Task 4: Add Dockerfile and requirements.txt

**Files:**
- Create: `pnl_consumer/Dockerfile`
- Create: `pnl_consumer/requirements.txt`

- [ ] **Step 1: Create `pnl_consumer/requirements.txt`**

```
confluent-kafka>=2.4
clickhouse-connect>=0.8
```

- [ ] **Step 2: Create `pnl_consumer/Dockerfile`**

```dockerfile
# pnl_consumer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY pnl_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY trading_dagster/utils/clickhouse_client.py ./trading_dagster/utils/clickhouse_client.py
COPY trading_dagster/utils/__init__.py ./trading_dagster/utils/__init__.py
COPY trading_dagster/utils/pnl_compute.py ./trading_dagster/utils/pnl_compute.py
RUN echo "" > ./trading_dagster/__init__.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "pnl_consumer.pnl_consumer"]
```

- [ ] **Step 3: Build image locally to verify it works**

```bash
docker build --platform linux/amd64 -f pnl_consumer/Dockerfile -t pnl-consumer-test .
```

Expected: build succeeds, no errors.

- [ ] **Step 4: Smoke-test the image starts correctly**

```bash
docker run --rm --platform linux/amd64 \
  -e REDPANDA_BROKERS=localhost:9092 \
  -e CLICKHOUSE_HOST=fake \
  -e CLICKHOUSE_PASSWORD=fake \
  -e CLICKHOUSE_PORT=8443 \
  -e CLICKHOUSE_USER=dev_ro3 \
  -e CLICKHOUSE_SECURE=true \
  pnl-consumer-test 2>&1 | head -5
```

Expected: Python starts, logs `Bootstrapped ... anchor(s)` or a ClickHouse connection error (no JVM startup noise, no `sed: can't read /config.yaml`).

- [ ] **Step 5: Commit**

```bash
git add pnl_consumer/Dockerfile pnl_consumer/requirements.txt
git commit -m "feat: add pnl_consumer Dockerfile and requirements"
```

---

## Task 5: Delete `flink_job/` and `tests/flink_job/`

- [ ] **Step 1: Delete the old directories**

```bash
git rm -r flink_job/
git rm -r tests/flink_job/
```

- [ ] **Step 2: Verify remaining tests still pass**

```bash
pytest tests/pnl_consumer/ -v -m unit
```

Expected: 10 tests PASS.

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: remove flink_job directory and tests"
```

---

## Task 6: Update Terraform

**Files:**
- Modify: `infra/terraform/main.tf`

All changes are in the block from line ~843 to ~1100.

- [ ] **Step 1: Rename ECR repository resource and name**

Find:
```hcl
resource "aws_ecr_repository" "flink_job" {
  name                 = "trading-analysis-flink-job"
```

Replace with:
```hcl
resource "aws_ecr_repository" "pnl_consumer" {
  name                 = "trading-analysis-pnl-consumer"
```

- [ ] **Step 2: Rename IAM role, remove S3 policy**

Find and replace the entire IAM section (lines ~849–880):
```hcl
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
```

Replace with:
```hcl
# ─────────────────────────────────────────────────────────────────────────────
# IAM — pnl-consumer task role
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "pnl_consumer_task" {
  name = "${local.name_prefix}-pnl-consumer-task"
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
```

- [ ] **Step 3: Update ECS task definition**

Find and replace the entire `aws_ecs_task_definition.flink_job` resource (lines ~971–1006):

```hcl
resource "aws_ecs_task_definition" "pnl_consumer" {
  family                   = "${local.name_prefix}-pnl-consumer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.pnl_consumer_task.arn

  container_definitions = jsonencode([{
    name      = "pnl-consumer"
    image     = "${aws_ecr_repository.pnl_consumer.repository_url}:latest"
    essential = true
    secrets = [
      { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
    ]
    environment = [
      { name = "CLICKHOUSE_PORT",   value = "8443" },
      { name = "CLICKHOUSE_USER",   value = "dev_ro3" },
      { name = "CLICKHOUSE_SECURE", value = "true" },
      { name = "REDPANDA_BROKERS",  value = "redpanda.${local.name_prefix}.local:9092" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.streaming.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "pnl-consumer"
      }
    }
  }])

  tags = local.common_tags
}
```

- [ ] **Step 4: Update ECS service**

Find:
```hcl
resource "aws_ecs_service" "flink_job" {
  name            = "${local.name_prefix}-flink-job"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.flink_job.arn
```

Replace with:
```hcl
resource "aws_ecs_service" "pnl_consumer" {
  name            = "${local.name_prefix}-pnl-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.pnl_consumer.arn
```

- [ ] **Step 5: Verify no remaining `flink` references in main.tf**

```bash
grep -n "flink" infra/terraform/main.tf
```

Expected: no output (zero matches).

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat(terraform): rename flink-job to pnl-consumer, remove S3 checkpoint IAM policy"
```

---

## Task 7: Update CI/CD workflow

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Update `detect-changes` outputs and filters (lines 14–37)**

Find:
```yaml
      flink:     ${{ steps.filter.outputs.flink     == 'true' || steps.filter.outputs.terraform == 'true' }}
```
Replace with:
```yaml
      pnl-consumer: ${{ steps.filter.outputs.pnl-consumer == 'true' || steps.filter.outputs.terraform == 'true' }}
```

Find:
```yaml
            flink:
              - 'flink_job/**'
```
Replace with:
```yaml
            pnl-consumer:
              - 'pnl_consumer/**'
```

- [ ] **Step 2: Replace the `build-flink` job with `build-pnl-consumer`**

Find the entire `build-flink` job (lines ~171–227) and replace:

```yaml
  build-pnl-consumer:
    runs-on: ubuntu-latest
    needs: [test, terraform, detect-changes]
    if: |
      always() &&
      needs.test.result == 'success' &&
      (needs.terraform.result == 'success' || needs.terraform.result == 'skipped') &&
      needs.detect-changes.outputs.pnl-consumer == 'true'
    permissions:
      id-token: write
      contents: read
    outputs:
      image_tag: ${{ steps.build.outputs.image_tag }}
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Build and push pnl-consumer image
        id: build
        env:
          REGISTRY: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com
        run: |
          [ -n "$REGISTRY" ] || { echo "ERROR: REGISTRY is empty"; exit 1; }
          IMAGE_URI=$REGISTRY/trading-analysis-pnl-consumer:${{ github.sha }}
          IMAGE_LATEST=$REGISTRY/trading-analysis-pnl-consumer:latest
          IMAGE_CACHE=$REGISTRY/trading-analysis-pnl-consumer:cache
          docker buildx build \
            -f pnl_consumer/Dockerfile \
            --cache-from type=registry,ref=$IMAGE_CACHE \
            --cache-to   type=registry,ref=$IMAGE_CACHE,mode=max \
            --provenance=false \
            -t $IMAGE_URI -t $IMAGE_LATEST \
            --push .
          echo "image_tag=${{ github.sha }}" >> $GITHUB_OUTPUT
```

- [ ] **Step 3: Replace the `deploy-flink` job with `deploy-pnl-consumer`**

Find the entire `deploy-flink` job (lines ~405–486) and replace:

```yaml
  deploy-pnl-consumer:
    runs-on: ubuntu-latest
    needs: [build-pnl-consumer, detect-changes]
    if: |
      always() &&
      needs.build-pnl-consumer.result == 'success' &&
      needs.detect-changes.outputs.pnl-consumer == 'true'
    permissions:
      id-token: write
      contents: read
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Deploy pnl-consumer to ECS
        env:
          IMAGE_TAG: ${{ needs.build-pnl-consumer.outputs.image_tag }}
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
          REGISTRY: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com
        run: |
          set -eu
          IMAGE_URI=$REGISTRY/trading-analysis-pnl-consumer:$IMAGE_TAG
          TASK_DEF=$(aws ecs describe-task-definition \
            --task-definition trading-analysis-pnl-consumer \
            --region $DEPLOY_REGION --query taskDefinition --output json)
          NEW_TASK=$(echo "$TASK_DEF" | python3 -c "
          import sys, json
          img = sys.argv[1]
          td = json.load(sys.stdin)
          for c in td['containerDefinitions']:
              if c['name'] == 'pnl-consumer':
                  c['image'] = img
          for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy'):
              td.pop(k, None)
          print(json.dumps(td))
          " "$IMAGE_URI")
          NEW_ARN=$(aws ecs register-task-definition \
            --cli-input-json "$NEW_TASK" \
            --region $DEPLOY_REGION \
            --query taskDefinition.taskDefinitionArn --output text)
          [ -n "$NEW_ARN" ] || { echo "ERROR: register-task-definition returned empty ARN"; exit 1; }
          echo "Registered: $NEW_ARN"
          aws ecs update-service \
            --cluster $ECS_CLUSTER \
            --service trading-analysis-pnl-consumer \
            --task-definition "$NEW_ARN" \
            --force-new-deployment \
            --region $DEPLOY_REGION \
            --output json > /dev/null

      - name: Wait for pnl-consumer stability
        env:
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
        run: |
          echo "Waiting up to 20 min for trading-analysis-pnl-consumer to stabilise..."
          for i in $(seq 1 80); do
            STATUS=$(aws ecs describe-services \
              --cluster $ECS_CLUSTER \
              --services trading-analysis-pnl-consumer \
              --region $DEPLOY_REGION \
              --query 'services[0].deployments' --output json)
            PRIMARIES=$(echo "$STATUS" | python3 -c "
          import sys,json
          d=json.load(sys.stdin)
          print(sum(1 for x in d if x['status']=='PRIMARY' and x['runningCount']>=x['desiredCount'] and x['failedTasks']==0))")
            TOTAL=$(echo "$STATUS" | python3 -c "
          import sys,json; d=json.load(sys.stdin); print(len(d))")
            echo "Poll $i/80: deployments=$TOTAL primary_stable=$PRIMARIES"
            if [ "$PRIMARIES" = "1" ] && [ "$TOTAL" = "1" ]; then
              echo "pnl-consumer deploy complete."
              break
            fi
            if [ "$i" = "80" ]; then
              echo "ERROR: timed out waiting for pnl-consumer stability"; exit 1
            fi
            sleep 15
          done
```

- [ ] **Step 4: Verify no remaining `flink` references in ci-cd.yml**

```bash
grep -n "flink" .github/workflows/ci-cd.yml
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: rename flink build/deploy jobs to pnl-consumer, remove JAR download step"
```

---

## Task 8: Final verification

- [ ] **Step 1: Run the full unit test suite**

```bash
pytest -m unit -v
```

Expected: all unit tests pass, no references to `flink_job` in output.

- [ ] **Step 2: Verify no stray `flink_job` references remain in source**

```bash
grep -r "flink_job\|flink-job\|build-flink\|deploy-flink" \
  --include="*.py" --include="*.tf" --include="*.yml" --include="*.toml" \
  . 2>/dev/null | grep -v ".pyc" | grep -v "docs/"
```

Expected: no output.

- [ ] **Step 3: Verify Docker image builds cleanly**

```bash
docker build --platform linux/amd64 -f pnl_consumer/Dockerfile -t pnl-consumer-final .
```

Expected: build succeeds.

- [ ] **Step 4: Final commit if anything was missed**

If step 2 reveals any remaining references, fix them, then:

```bash
git add -p
git commit -m "chore: clean up remaining flink references"
```
