# PyFlink PnL Job on ECS Fargate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a PyFlink DataStream job (`flink_pnl/`) that consumes `binance.price.ticks` from Redpanda, computes PnL for prod/bt/real_trade modes using shared `libs/computation/` logic, and sinks to ClickHouse — running alongside `pnl_consumer/` for evaluation.

**Architecture:** Single ECS Fargate task running PyFlink in embedded mini-cluster mode (JobManager + TaskManager in-process, parallelism=1). Anchor state is `dict[underlying, dict[strategy_table_name, AnchorRecord]]`, bootstrapped from ClickHouse on cold start. Flink checkpoints every 60s to S3. All sink modes (price/prod/bt/real_trade) are env-var controlled and default to `false`.

**Tech Stack:** Python 3.11, PyFlink 2.2.1 (apache-flink==2.2.1), apache/flink:2.2.1-java21 Docker base, confluent-kafka>=2.4, clickhouse-connect>=0.8, boto3>=1.34, AWS ECS Fargate, S3 checkpoints, Terraform, GitHub Actions.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `flink_pnl/__init__.py` | Package marker |
| Create | `flink_pnl/sink_config.py` | `SinkConfig` dataclass — reads ENABLE_* env vars |
| Create | `flink_pnl/state.py` | `StateMap` type alias + `build_state_from_bootstrap()` — converts bootstrap result into `underlying → stn → AnchorRecord` |
| Create | `flink_pnl/process_candle.py` | Pure function `process_candle()` — all per-candle PnL logic for all three modes, no I/O |
| Create | `flink_pnl/clickhouse_sink.py` | `ClickHouseSinkFunction(SinkFunction)` — buffers rows, flushes on `snapshotState()` |
| Create | `flink_pnl/pnl_job.py` | `PnlProcessFunction(ProcessFunction)` — Flink operator wiring `open()` + `process_element()` |
| Create | `flink_pnl/entrypoint.py` | Flink `StreamExecutionEnvironment` setup, checkpoint config, job launch |
| Create | `flink_pnl/Dockerfile` | `apache/flink:2.2.1-java21` base + Python + PyFlink install |
| Create | `flink_pnl/requirements.txt` | Pinned dependencies |
| Create | `tests/flink_pnl/__init__.py` | Test package marker |
| Create | `tests/flink_pnl/test_sink_config.py` | Unit tests for `SinkConfig` |
| Create | `tests/flink_pnl/test_state.py` | Unit tests for `build_state_from_bootstrap()` |
| Create | `tests/flink_pnl/test_process_candle.py` | Unit tests for `process_candle()` — all modes, carry-forward, revision guard |
| Modify | `infra/terraform/main.tf` | Add ECR repo, IAM role + S3 policy, ECS task definition + service (additive) |
| Modify | `.github/workflows/ci-cd.yml` | Add `build-flink-pnl` and `deploy-flink-pnl` jobs |

---

## Background: Key Existing APIs

These are the functions from `libs/computation/` that `flink_pnl/` imports. Read the actual source at the paths below — do not guess signatures.

- `libs/computation/anchor_state.py`: `AnchorRecord` (dataclass with `pnl`, `price`, `position`, `bar_ts`, `revision_ts`, `strategy_id`, `strategy_name`, `underlying`, `config_timeframe`, `weighting`, `strategy_instance_id`, `final_signal`, `benchmark`), `AnchorState` (has `.get(stn)`, `.set(stn, rec)`, `.has(stn)`, `.keys()`, `.compute_pnl(stn, price, position, bar_ts, revision_ts, meta)`, `.should_apply_revision(stn, bar_ts, revision_ts)`)
- `libs/computation/candle_lookup.py`: `fetch_strategies_for_candle(instrument, candle_ts) -> list[StrategyBar]`, `fetch_bt_strategies_for_candle(instrument, candle_ts) -> list[StrategyBar]`, `fetch_real_trade_for_candle(instrument, candle_ts) -> list[StrategyRevision]`. Note: these functions take `instrument` as `"BTCUSDT"` (full instrument) and internally strip `"USDT"` — do NOT pre-strip before passing.
- `libs/computation/pnl_formula.py`: `build_pnl_row(strategy_table_name, bar_dict, price, cumulative_pnl, source_label, ts, now) -> list`, `build_carry_forward_row(strategy_table_name, rec, price, cumulative_pnl, source_label, ts, now) -> list`, `INSERT_COLUMNS` (list of 16 column names in insert order)
- `pnl_consumer/pnl_consumer.py`: `_bootstrap_state(mode, reference_ts) -> AnchorState` and `peek_reference_ts(brokers, group_id, topic, timeout) -> datetime | None` — import and reuse these directly

**Critical invariants:**
- `price` in all output rows = `candle.open` from Redpanda — never from ClickHouse
- `position` always from `strategy_output_history_*` — never from the PnL table
- `underlying` in anchor map = `candle.instrument.removesuffix("USDT")` — e.g. `"BTCUSDT"` → `"BTC"`
- `fetch_strategies_for_candle` takes the full instrument string `"BTCUSDT"` — it strips internally
- Three modes have different source tables, revision logic, and bar gates — see spec for full table

---

## Task 1: Package skeleton + `SinkConfig`

**Files:**
- Create: `flink_pnl/__init__.py`
- Create: `flink_pnl/sink_config.py`
- Create: `flink_pnl/requirements.txt`
- Create: `tests/flink_pnl/__init__.py`
- Create: `tests/flink_pnl/test_sink_config.py`

- [ ] **Step 1: Create package files**

```bash
mkdir -p flink_pnl tests/flink_pnl
touch flink_pnl/__init__.py tests/flink_pnl/__init__.py
```

- [ ] **Step 2: Create `flink_pnl/requirements.txt`**

```
apache-flink==2.2.1
clickhouse-connect>=0.8
confluent-kafka>=2.4
boto3>=1.34
```

- [ ] **Step 3: Write the failing test first**

Create `tests/flink_pnl/test_sink_config.py`:

```python
import pytest
from flink_pnl.sink_config import SinkConfig


@pytest.mark.unit
def test_all_false_by_default():
    cfg = SinkConfig.from_env({})
    assert cfg.price is False
    assert cfg.prod is False
    assert cfg.bt is False
    assert cfg.real_trade is False


@pytest.mark.unit
def test_enable_price_only():
    cfg = SinkConfig.from_env({"ENABLE_PRICE_SINK": "true"})
    assert cfg.price is True
    assert cfg.prod is False
    assert cfg.bt is False
    assert cfg.real_trade is False


@pytest.mark.unit
def test_enable_all():
    cfg = SinkConfig.from_env({
        "ENABLE_PRICE_SINK": "true",
        "ENABLE_PROD_SINK": "true",
        "ENABLE_BT_SINK": "true",
        "ENABLE_REAL_TRADE_SINK": "true",
    })
    assert cfg.price is True
    assert cfg.prod is True
    assert cfg.bt is True
    assert cfg.real_trade is True


@pytest.mark.unit
def test_case_insensitive():
    cfg = SinkConfig.from_env({"ENABLE_PROD_SINK": "TRUE"})
    assert cfg.prod is True


@pytest.mark.unit
def test_false_string():
    cfg = SinkConfig.from_env({"ENABLE_PROD_SINK": "false"})
    assert cfg.prod is False
```

- [ ] **Step 4: Run — verify FAIL**

```bash
pytest tests/flink_pnl/test_sink_config.py -v -m unit
```

Expected: `ImportError: cannot import name 'SinkConfig'`

- [ ] **Step 5: Create `flink_pnl/sink_config.py`**

```python
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class SinkConfig:
    price: bool
    prod: bool
    bt: bool
    real_trade: bool

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> SinkConfig:
        if env is None:
            env = os.environ  # type: ignore[assignment]

        def _flag(key: str) -> bool:
            return env.get(key, "false").lower() == "true"

        return cls(
            price=_flag("ENABLE_PRICE_SINK"),
            prod=_flag("ENABLE_PROD_SINK"),
            bt=_flag("ENABLE_BT_SINK"),
            real_trade=_flag("ENABLE_REAL_TRADE_SINK"),
        )
```

- [ ] **Step 6: Run — verify PASS**

```bash
pytest tests/flink_pnl/test_sink_config.py -v -m unit
```

Expected: 5 tests PASS.

- [ ] **Step 7: Commit**

```bash
git add flink_pnl/ tests/flink_pnl/
git commit -m "feat(flink-pnl): add package skeleton and SinkConfig"
```

---

## Task 2: State map builder

**Files:**
- Create: `flink_pnl/state.py`
- Create: `tests/flink_pnl/test_state.py`

The bootstrap produces an `AnchorState` (flat `stn → AnchorRecord`). The Flink job needs `underlying → stn → AnchorRecord` for O(1) per-candle lookups. `build_state_from_bootstrap()` does this conversion.

- [ ] **Step 1: Write the failing test**

Create `tests/flink_pnl/test_state.py`:

```python
from datetime import datetime

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from flink_pnl.state import StateMap, build_state_from_bootstrap


@pytest.mark.unit
def test_groups_by_underlying():
    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=1.0, price=50000.0, position=1.0,
        underlying="BTC", strategy_table_name="strat_btc_5m",
        strategy_instance_id="sid1",
    ))
    anchor.set("strat_eth_1h", AnchorRecord(
        pnl=0.5, price=3000.0, position=-1.0,
        underlying="ETH", strategy_table_name="strat_eth_1h",
        strategy_instance_id="sid2",
    ))

    result: StateMap = build_state_from_bootstrap(anchor)

    assert "BTC" in result
    assert "ETH" in result
    assert "strat_btc_5m" in result["BTC"]
    assert "strat_eth_1h" in result["ETH"]
    assert result["BTC"]["strat_btc_5m"].pnl == 1.0
    assert result["ETH"]["strat_eth_1h"].position == -1.0


@pytest.mark.unit
def test_empty_anchor_state_returns_empty_map():
    result = build_state_from_bootstrap(AnchorState())
    assert result == {}


@pytest.mark.unit
def test_multiple_strategies_same_underlying():
    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(pnl=1.0, price=50000.0, position=1.0, underlying="BTC", strategy_instance_id="s1"))
    anchor.set("strat_btc_1h", AnchorRecord(pnl=2.0, price=50000.0, position=-1.0, underlying="BTC", strategy_instance_id="s2"))

    result = build_state_from_bootstrap(anchor)

    assert len(result["BTC"]) == 2
    assert "strat_btc_5m" in result["BTC"]
    assert "strat_btc_1h" in result["BTC"]
```

- [ ] **Step 2: Run — verify FAIL**

```bash
pytest tests/flink_pnl/test_state.py -v -m unit
```

Expected: `ImportError: cannot import name 'StateMap'`

- [ ] **Step 3: Create `flink_pnl/state.py`**

```python
from __future__ import annotations

from libs.computation.anchor_state import AnchorRecord, AnchorState

# underlying ("BTC") → strategy_table_name → AnchorRecord
StateMap = dict[str, dict[str, AnchorRecord]]


def build_state_from_bootstrap(anchor: AnchorState) -> StateMap:
    """Convert a flat AnchorState into a nested StateMap keyed by underlying.

    The bootstrap produces AnchorState keyed by strategy_table_name. This
    converts it to underlying → stn → AnchorRecord so the process function
    can look up all strategies for a given underlying in O(1).

    Strategies whose AnchorRecord.underlying is empty are skipped — they
    have no metadata yet and cannot be carry-forwarded meaningfully.
    """
    result: StateMap = {}
    for stn in anchor.keys():
        rec = anchor.get(stn)
        if not rec.underlying:
            continue
        if rec.underlying not in result:
            result[rec.underlying] = {}
        result[rec.underlying][stn] = rec
    return result
```

- [ ] **Step 4: Run — verify PASS**

```bash
pytest tests/flink_pnl/test_state.py -v -m unit
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add flink_pnl/state.py tests/flink_pnl/test_state.py
git commit -m "feat(flink-pnl): add StateMap and build_state_from_bootstrap"
```

---

## Task 3: Pure `process_candle()` function

**Files:**
- Create: `flink_pnl/process_candle.py`
- Create: `tests/flink_pnl/test_process_candle.py`

This is the most critical task. `process_candle()` is a pure function (no I/O) that takes a candle + three state maps + sink config and returns a list of ClickHouse-ready row lists. All three mode differences live here.

- [ ] **Step 1: Write the failing tests**

Create `tests/flink_pnl/test_process_candle.py`:

```python
from datetime import datetime
from unittest.mock import patch

import pytest

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.candle_lookup import StrategyBar, StrategyRevision
from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap, build_state_from_bootstrap
from flink_pnl.process_candle import process_candle
from streaming.models import CandleEvent


def _candle(instrument: str = "BTCUSDT", open_price: float = 50000.0) -> CandleEvent:
    return CandleEvent(
        exchange="binance",
        instrument=instrument,
        ts=datetime(2026, 5, 15, 10, 0, 0),
        open=open_price,
        high=open_price + 100,
        low=open_price - 100,
        close=open_price + 50,
        volume=1.0,
    )


def _bar(stn: str = "strat_btc_5m", underlying: str = "BTC", position: float = 1.0) -> StrategyBar:
    return StrategyBar(
        strategy_table_name=stn,
        strategy_instance_id="sid1",
        strategy_id=1,
        strategy_name="momentum",
        underlying=underlying,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=1.0,
        benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),
    )


def _revision(stn: str = "strat_btc_rt", underlying: str = "BTC", position: float = 0.5) -> StrategyRevision:
    return StrategyRevision(
        strategy_table_name=stn,
        strategy_instance_id="sid_rt",
        strategy_id=2,
        strategy_name="real_trade_mom",
        underlying=underlying,
        config_timeframe="5m",
        weighting=1.0,
        position=position,
        final_signal=0.5,
        benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),
        revision_ts=datetime(2026, 5, 15, 9, 59, 0),
    )


@pytest.mark.unit
def test_price_sink_emits_price_row():
    cfg = SinkConfig(price=True, prod=False, bt=False, real_trade=False)
    candle = _candle()
    state_prod: StateMap = {}
    state_bt: StateMap = {}
    state_rt: StateMap = {}

    rows = process_candle(candle, state_prod, state_bt, state_rt, cfg)

    price_rows = [r for r in rows if r["_sink"] == "price"]
    assert len(price_rows) == 1
    assert price_rows[0]["instrument"] == "BTCUSDT"
    assert price_rows[0]["open"] == 50000.0


@pytest.mark.unit
def test_all_sinks_false_returns_empty():
    cfg = SinkConfig(price=False, prod=False, bt=False, real_trade=False)
    rows = process_candle(_candle(), {}, {}, {}, cfg)
    assert rows == []


@pytest.mark.unit
def test_prod_new_bar_emits_pnl_row():
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(open_price=50000.0)

    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=0.0, price=49000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
        strategy_id=1, strategy_name="momentum",
        config_timeframe="5m", weighting=1.0,
        final_signal=1.0, benchmark=0.0,
    ))
    state_prod = build_state_from_bootstrap(anchor)

    bar = _bar(position=1.0)
    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[bar]):
        rows = process_candle(candle, state_prod, {}, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1
    row_data = pnl_rows[0]["_row"]
    # price must be candle.open = 50000.0, not from ClickHouse
    assert row_data[11] == 50000.0  # price is index 11 in INSERT_COLUMNS
    # cumulative_pnl = 0.0 + 1.0 * (50000 - 49000) / 49000
    expected_pnl = 0.0 + 1.0 * (50000.0 - 49000.0) / 49000.0
    assert abs(row_data[8] - expected_pnl) < 1e-9  # cumulative_pnl is index 8


@pytest.mark.unit
def test_prod_carry_forward_when_no_new_bar():
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(instrument="BTCUSDT", open_price=50000.0)

    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=1.0, price=49000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
        strategy_id=1, strategy_name="momentum",
        config_timeframe="5m", weighting=1.0,
        final_signal=1.0, benchmark=0.0,
    ))
    state_prod = build_state_from_bootstrap(anchor)

    # No new bar returned for BTCUSDT
    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state_prod, {}, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert len(pnl_rows) == 1  # carry-forward row emitted
    row_data = pnl_rows[0]["_row"]
    assert row_data[11] == 50000.0  # price updated to candle.open
    assert row_data[10] == 1.0      # position unchanged (index 10)


@pytest.mark.unit
def test_carry_forward_only_fires_for_matching_underlying():
    """A SOL candle must not carry-forward BTC strategies."""
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(instrument="SOLUSDT", open_price=150.0)

    anchor = AnchorState()
    anchor.set("strat_btc_5m", AnchorRecord(
        pnl=1.0, price=49000.0, position=1.0,
        underlying="BTC", strategy_instance_id="sid1",
    ))
    state_prod = build_state_from_bootstrap(anchor)

    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[]):
        rows = process_candle(candle, state_prod, {}, {}, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_prod"]
    assert pnl_rows == []  # BTC strategy must NOT be carry-forwarded on SOL candle


@pytest.mark.unit
def test_real_trade_revision_guard_blocks_stale_revision():
    cfg = SinkConfig(price=False, prod=False, bt=False, real_trade=True)
    candle = _candle()

    anchor = AnchorState()
    anchor.set("strat_btc_rt", AnchorRecord(
        pnl=1.0, price=49000.0, position=0.5,
        underlying="BTC", strategy_instance_id="sid_rt",
        strategy_id=2, strategy_name="rt",
        config_timeframe="5m", weighting=1.0,
        final_signal=0.5, benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 10, 0, 0),      # newer bar already active
        revision_ts=datetime(2026, 5, 15, 10, 0, 30),
    ))
    state_rt = build_state_from_bootstrap(anchor)

    # Revision with older bar_ts — should be rejected by guard
    stale_rev = _revision(position=0.0)
    stale_rev = StrategyRevision(
        strategy_table_name="strat_btc_rt",
        strategy_instance_id="sid_rt",
        strategy_id=2,
        strategy_name="rt",
        underlying="BTC",
        config_timeframe="5m",
        weighting=1.0,
        position=0.0,
        final_signal=0.0,
        benchmark=0.0,
        bar_ts=datetime(2026, 5, 15, 9, 55, 0),   # older bar
        revision_ts=datetime(2026, 5, 15, 9, 59, 0),
    )

    with patch("flink_pnl.process_candle.fetch_real_trade_for_candle", return_value=[stale_rev]):
        rows = process_candle(candle, {}, {}, state_rt, cfg)

    pnl_rows = [r for r in rows if r["_sink"] == "pnl_real_trade"]
    assert len(pnl_rows) == 1  # carry-forward emitted (guard blocked new position)
    row_data = pnl_rows[0]["_row"]
    assert row_data[10] == 0.5  # position from anchor unchanged (index 10)


@pytest.mark.unit
def test_fetch_strategies_receives_full_instrument_not_underlying():
    """fetch_strategies_for_candle must receive 'BTCUSDT', not 'BTC'."""
    cfg = SinkConfig(price=False, prod=True, bt=False, real_trade=False)
    candle = _candle(instrument="BTCUSDT")

    with patch("flink_pnl.process_candle.fetch_strategies_for_candle", return_value=[]) as mock_fetch:
        process_candle(candle, {}, {}, {}, cfg)

    mock_fetch.assert_called_once_with("BTCUSDT", candle.ts)
```

- [ ] **Step 2: Run — verify FAIL**

```bash
pytest tests/flink_pnl/test_process_candle.py -v -m unit
```

Expected: `ImportError: cannot import name 'process_candle'`

- [ ] **Step 3: Create `flink_pnl/process_candle.py`**

```python
"""Pure per-candle PnL computation for all three modes.

No I/O. Takes pre-populated state maps and a candle, returns a list of row
dicts ready for ClickHouseSinkFunction. Each dict has '_sink' key:
  'price'         → futures_price_1min row (dict with raw column values)
  'pnl_prod'      → strategy_pnl_1min_prod_v2 row (dict with '_row' list)
  'pnl_bt'        → strategy_pnl_1min_bt_v2 row
  'pnl_real_trade'→ strategy_pnl_1min_real_trade_v2 row

Price invariant: price in all rows = candle.open — never from ClickHouse.
Position invariant: position always from strategy_output_history — never PnL table.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from libs.computation.anchor_state import AnchorRecord, AnchorState
from libs.computation.candle_lookup import (
    StrategyBar,
    StrategyRevision,
    fetch_bt_strategies_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
)
from libs.computation.pnl_formula import build_carry_forward_row, build_pnl_row
from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap
from streaming.models import CandleEvent


def _meta_from_bar(bar: StrategyBar) -> AnchorRecord:
    return AnchorRecord(
        strategy_id=bar.strategy_id,
        strategy_name=bar.strategy_name,
        underlying=bar.underlying,
        config_timeframe=bar.config_timeframe,
        weighting=bar.weighting,
        strategy_instance_id=bar.strategy_instance_id,
        final_signal=bar.final_signal,
        benchmark=bar.benchmark,
    )


def _meta_from_revision(rev: StrategyRevision) -> AnchorRecord:
    return AnchorRecord(
        strategy_id=rev.strategy_id,
        strategy_name=rev.strategy_name,
        underlying=rev.underlying,
        config_timeframe=rev.config_timeframe,
        weighting=rev.weighting,
        strategy_instance_id=rev.strategy_instance_id,
        final_signal=rev.final_signal,
        benchmark=rev.benchmark,
    )


def _compute_and_emit(
    state_map: StateMap,
    underlying: str,
    stn: str,
    candle: CandleEvent,
    position: float,
    source_label: str,
    sink_key: str,
    now: datetime,
    bar_ts: datetime | None = None,
    revision_ts: datetime | None = None,
    meta: AnchorRecord | None = None,
) -> dict:
    """Advance the anchor chain for one strategy and return a sink row dict."""
    if underlying not in state_map:
        state_map[underlying] = {}
    if stn not in state_map[underlying]:
        # Lazy-seed: new strategy seen for the first time — start from zero.
        state_map[underlying][stn] = AnchorRecord(
            pnl=0.0, price=candle.open, position=0.0,
        )

    # Wrap the underlying dict in a temporary AnchorState for compute_pnl/carry-forward.
    inner = state_map[underlying]
    tmp = AnchorState()
    for k, v in inner.items():
        tmp.set(k, v)

    from datetime import datetime as dt
    pnl = tmp.compute_pnl(
        stn,
        candle.open,
        position,
        bar_ts=bar_ts or dt.min,
        revision_ts=revision_ts or dt.min,
        meta=meta,
    )
    # Write updated record back into the mutable state map.
    state_map[underlying][stn] = tmp.get(stn)

    bar_dict = {
        "strategy_id": state_map[underlying][stn].strategy_id,
        "strategy_name": state_map[underlying][stn].strategy_name,
        "underlying": state_map[underlying][stn].underlying,
        "config_timeframe": state_map[underlying][stn].config_timeframe,
        "weighting": state_map[underlying][stn].weighting,
        "strategy_instance_id": state_map[underlying][stn].strategy_instance_id,
        "final_signal": state_map[underlying][stn].final_signal,
        "bar_benchmark": state_map[underlying][stn].benchmark,
        "position": position,
    }
    return {
        "_sink": sink_key,
        "_row": build_pnl_row(stn, bar_dict, candle.open, pnl, source_label, candle.ts, now),
    }


def _carry_forward(
    state_map: StateMap,
    underlying: str,
    stn: str,
    candle: CandleEvent,
    source_label: str,
    sink_key: str,
    now: datetime,
) -> dict | None:
    """Emit a carry-forward row using last known position and new candle price."""
    rec = state_map.get(underlying, {}).get(stn)
    if rec is None or not rec.strategy_instance_id:
        return None

    tmp = AnchorState()
    tmp.set(stn, rec)
    pnl = tmp.compute_pnl(stn, candle.open, rec.position,
                           bar_ts=rec.bar_ts, revision_ts=rec.revision_ts)
    state_map[underlying][stn] = tmp.get(stn)

    return {
        "_sink": sink_key,
        "_row": build_carry_forward_row(stn, rec, candle.open, pnl, source_label, candle.ts, now),
    }


def process_candle(
    candle: CandleEvent,
    state_prod: StateMap,
    state_bt: StateMap,
    state_rt: StateMap,
    cfg: SinkConfig,
) -> list[dict]:
    """Compute all output rows for one candle across all enabled modes.

    Mutates state_prod, state_bt, state_rt in-place (advances anchor chain).
    Returns list of row dicts with '_sink' key for routing in ClickHouseSinkFunction.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []
    # underlying is "BTC" — stripped from candle.instrument "BTCUSDT"
    underlying = candle.instrument.removesuffix("USDT")

    # --- Price sink ---
    if cfg.price:
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

    # --- Prod sink ---
    # fetch_strategies_for_candle takes full instrument ("BTCUSDT"); strips internally.
    if cfg.prod:
        prod_bars = fetch_strategies_for_candle(candle.instrument, candle.ts)
        prod_seen: set[str] = set()
        for bar in prod_bars:
            prod_seen.add(bar.strategy_table_name)
            rows.append(_compute_and_emit(
                state_prod, underlying, bar.strategy_table_name,
                candle, bar.position, "production", "pnl_prod", now,
                meta=_meta_from_bar(bar),
            ))
        # Carry-forward: all strategies for this underlying with no new bar.
        for stn in list(state_prod.get(underlying, {}).keys()):
            if stn not in prod_seen:
                row = _carry_forward(state_prod, underlying, stn, candle, "production", "pnl_prod", now)
                if row is not None:
                    rows.append(row)

    # --- Backtest sink ---
    if cfg.bt:
        bt_bars = fetch_bt_strategies_for_candle(candle.instrument, candle.ts)
        bt_seen: set[str] = set()
        for bar in bt_bars:
            bt_seen.add(bar.strategy_table_name)
            rows.append(_compute_and_emit(
                state_bt, underlying, bar.strategy_table_name,
                candle, bar.position, "backtest", "pnl_bt", now,
                meta=_meta_from_bar(bar),
            ))
        for stn in list(state_bt.get(underlying, {}).keys()):
            if stn not in bt_seen:
                row = _carry_forward(state_bt, underlying, stn, candle, "backtest", "pnl_bt", now)
                if row is not None:
                    rows.append(row)

    # --- Real-trade sink ---
    # Revision guard: apply only if (bar_ts, revision_ts) > anchor's (bar_ts, revision_ts).
    if cfg.real_trade:
        rt_revs = fetch_real_trade_for_candle(candle.instrument, candle.ts)
        rt_seen: set[str] = set()
        for rev in rt_revs:
            rt_seen.add(rev.strategy_table_name)
            rec = state_rt.get(underlying, {}).get(rev.strategy_table_name)
            if rec is not None:
                tmp = AnchorState()
                tmp.set(rev.strategy_table_name, rec)
                guard_pass = tmp.should_apply_revision(
                    rev.strategy_table_name, rev.bar_ts, rev.revision_ts
                )
            else:
                guard_pass = True  # No existing anchor — first revision always applies.
            if not guard_pass:
                row = _carry_forward(state_rt, underlying, rev.strategy_table_name,
                                     candle, "real_trade", "pnl_real_trade", now)
                if row is not None:
                    rows.append(row)
                continue
            rows.append(_compute_and_emit(
                state_rt, underlying, rev.strategy_table_name,
                candle, rev.position, "real_trade", "pnl_real_trade", now,
                bar_ts=rev.bar_ts, revision_ts=rev.revision_ts,
                meta=_meta_from_revision(rev),
            ))
        for stn in list(state_rt.get(underlying, {}).keys()):
            if stn not in rt_seen:
                row = _carry_forward(state_rt, underlying, stn, candle, "real_trade", "pnl_real_trade", now)
                if row is not None:
                    rows.append(row)

    return rows
```

- [ ] **Step 4: Run — verify PASS**

```bash
pytest tests/flink_pnl/test_process_candle.py -v -m unit
```

Expected: 7 tests PASS.

- [ ] **Step 5: Run all flink_pnl tests together**

```bash
pytest tests/flink_pnl/ -v -m unit
```

Expected: 15 tests PASS (5 + 3 + 7).

- [ ] **Step 6: Commit**

```bash
git add flink_pnl/process_candle.py tests/flink_pnl/test_process_candle.py
git commit -m "feat(flink-pnl): add process_candle with all three modes and carry-forward"
```

---

## Task 4: ClickHouse sink function

**Files:**
- Create: `flink_pnl/clickhouse_sink.py`

No unit tests here — the sink just buffers and flushes. Integration is verified via smoke test in Task 7.

- [ ] **Step 1: Create `flink_pnl/clickhouse_sink.py`**

```python
"""Flink SinkFunction that buffers rows and flushes to ClickHouse on checkpoint.

Rows arrive as dicts with '_sink' key routing them to the correct table.
On snapshotState() (Flink checkpoint), all buffered rows are flushed to
ClickHouse before Flink snapshots the Kafka offset — ensuring at-least-once
delivery. ReplacingMergeTree handles any duplicates from replayed candles.
"""

from __future__ import annotations

import logging
from typing import Any

from pyflink.datastream import SinkFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common.typeinfo import Types

from libs.clickhouse_client import insert_rows
from libs.computation.pnl_formula import INSERT_COLUMNS

logger = logging.getLogger(__name__)

PRICE_COLUMNS = ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"]

_TABLE_MAP = {
    "pnl_prod":       "analytics.strategy_pnl_1min_prod_v2",
    "pnl_bt":         "analytics.strategy_pnl_1min_bt_v2",
    "pnl_real_trade": "analytics.strategy_pnl_1min_real_trade_v2",
}


class ClickHouseSinkFunction(SinkFunction):
    """Buffers output rows in-memory; flushes to ClickHouse on Flink checkpoint.

    invoke() is called per row. snapshotState() is called by Flink's checkpoint
    coordinator before snapshotting Kafka offsets — so a flush failure aborts the
    checkpoint and offsets are not advanced.
    """

    def __init__(self) -> None:
        super().__init__()
        self._price_buf: list[list] = []
        self._pnl_prod_buf: list[list] = []
        self._pnl_bt_buf: list[list] = []
        self._pnl_rt_buf: list[list] = []

    def open(self, runtime_context: RuntimeContext) -> None:
        logger.info("ClickHouseSinkFunction opened")

    def invoke(self, value: dict, context: Any) -> None:
        sink = value["_sink"]
        if sink == "price":
            row = value
            self._price_buf.append([
                row["exchange"], row["instrument"], row["ts"],
                row["open"], row["high"], row["low"], row["close"], row["volume"],
            ])
        elif sink == "pnl_prod":
            self._pnl_prod_buf.append(value["_row"])
        elif sink == "pnl_bt":
            self._pnl_bt_buf.append(value["_row"])
        elif sink == "pnl_real_trade":
            self._pnl_rt_buf.append(value["_row"])

    def snapshot_state(self, context: Any) -> None:
        """Flush all buffers to ClickHouse. Called by Flink on each checkpoint.

        If any insert fails, the exception propagates — Flink aborts the checkpoint
        and retries at the next interval. Kafka offsets are NOT advanced.
        """
        if self._price_buf:
            insert_rows("analytics.futures_price_1min", PRICE_COLUMNS, self._price_buf)
            logger.info("Flushed %d price rows", len(self._price_buf))
            self._price_buf.clear()

        if self._pnl_prod_buf:
            insert_rows("analytics.strategy_pnl_1min_prod_v2", INSERT_COLUMNS, self._pnl_prod_buf)
            logger.info("Flushed %d prod pnl rows", len(self._pnl_prod_buf))
            self._pnl_prod_buf.clear()

        if self._pnl_bt_buf:
            insert_rows("analytics.strategy_pnl_1min_bt_v2", INSERT_COLUMNS, self._pnl_bt_buf)
            logger.info("Flushed %d bt pnl rows", len(self._pnl_bt_buf))
            self._pnl_bt_buf.clear()

        if self._pnl_rt_buf:
            insert_rows("analytics.strategy_pnl_1min_real_trade_v2", INSERT_COLUMNS, self._pnl_rt_buf)
            logger.info("Flushed %d real_trade pnl rows", len(self._pnl_rt_buf))
            self._pnl_rt_buf.clear()
```

- [ ] **Step 2: Commit**

```bash
git add flink_pnl/clickhouse_sink.py
git commit -m "feat(flink-pnl): add ClickHouseSinkFunction"
```

---

## Task 5: Flink operator and entrypoint

**Files:**
- Create: `flink_pnl/pnl_job.py`
- Create: `flink_pnl/entrypoint.py`

- [ ] **Step 1: Create `flink_pnl/pnl_job.py`**

```python
"""PnlProcessFunction: Flink ProcessFunction wrapping the per-candle logic.

open():  bootstrap AnchorState from ClickHouse for each enabled mode,
         convert to StateMap (underlying → stn → AnchorRecord).
process_element(): call process_candle(), collect output rows.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from pyflink.datastream import ProcessFunction, RuntimeContext
from pyflink.datastream.timerservice import TimerService

from libs.computation.anchor_state import AnchorState
from flink_pnl.process_candle import process_candle
from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap, build_state_from_bootstrap
from streaming.models import CandleEvent

logger = logging.getLogger(__name__)


class PnlProcessFunction(ProcessFunction):
    """Stateful Flink operator: bootstraps anchor state on open(), processes candles."""

    def __init__(self) -> None:
        super().__init__()
        self._cfg: SinkConfig | None = None
        self._state_prod: StateMap = {}
        self._state_bt: StateMap = {}
        self._state_rt: StateMap = {}

    def open(self, runtime_context: RuntimeContext) -> None:
        self._cfg = SinkConfig.from_env()
        logger.info(
            "PnlProcessFunction.open() — price=%s prod=%s bt=%s real_trade=%s",
            self._cfg.price, self._cfg.prod, self._cfg.bt, self._cfg.real_trade,
        )

        if not (self._cfg.prod or self._cfg.bt or self._cfg.real_trade):
            logger.info("No PnL sinks enabled — skipping bootstrap")
            return

        brokers = os.environ["REDPANDA_BROKERS"]
        group_id = os.environ.get("KAFKA_GROUP_ID", "flink-pnl-consumer-v2")

        # Import bootstrap from pnl_consumer — same logic, no duplication.
        from pnl_consumer.pnl_consumer import _bootstrap_state, peek_reference_ts

        reference_ts = peek_reference_ts(brokers, group_id)
        if reference_ts is not None:
            logger.info("Bootstrap reference_ts from committed offset: %s", reference_ts)
        else:
            logger.info("No committed offset — using now() as reference_ts")

        if self._cfg.prod:
            logger.info("Bootstrapping prod state...")
            anchor = _bootstrap_state("prod", reference_ts)
            self._state_prod = build_state_from_bootstrap(anchor)
            logger.info("Prod bootstrap complete: %d underlyings", len(self._state_prod))

        if self._cfg.bt:
            logger.info("Bootstrapping bt state...")
            anchor = _bootstrap_state("bt", reference_ts)
            self._state_bt = build_state_from_bootstrap(anchor)
            logger.info("BT bootstrap complete: %d underlyings", len(self._state_bt))

        if self._cfg.real_trade:
            logger.info("Bootstrapping real_trade state...")
            anchor = _bootstrap_state("real_trade", reference_ts)
            self._state_rt = build_state_from_bootstrap(anchor)
            logger.info("Real-trade bootstrap complete: %d underlyings", len(self._state_rt))

    def process_element(self, value: str, ctx: ProcessFunction.Context, out: any) -> None:
        assert self._cfg is not None
        try:
            data = json.loads(value)
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
        except Exception:
            logger.exception("Failed to parse candle message: %.200s", value)
            return

        logger.info("Candle %s open=%.2f ts=%s", candle.instrument, candle.open, candle.ts)

        rows = process_candle(
            candle,
            self._state_prod,
            self._state_bt,
            self._state_rt,
            self._cfg,
        )
        for row in rows:
            out.collect(row)
```

- [ ] **Step 2: Create `flink_pnl/entrypoint.py`**

```python
"""Flink job entrypoint: sets up StreamExecutionEnvironment and launches the job.

Runs in embedded mini-cluster mode (JobManager + TaskManager in-process).
Parallelism=1 — all state in a single operator, no keying needed.
Checkpoints every 60s to S3 for durable offset storage.
"""

import logging
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

from flink_pnl.clickhouse_sink import ClickHouseSinkFunction
from flink_pnl.pnl_job import PnlProcessFunction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Checkpoint every 60s to S3. On restart, Flink restores from last checkpoint
    # and replays Kafka from the checkpointed offset — bootstrap skipped.
    checkpoint_interval_ms = int(os.environ.get("FLINK_CHECKPOINT_INTERVAL_MS", "60000"))
    env.enable_checkpointing(checkpoint_interval_ms)

    s3_checkpoint_path = os.environ.get(
        "FLINK_CHECKPOINT_DIR",
        "s3://trading-analysis-data-v2/flink-pnl-checkpoints/",
    )
    env.get_checkpoint_config().set_checkpoint_storage_uri(s3_checkpoint_path)
    logger.info("Checkpointing every %dms to %s", checkpoint_interval_ms, s3_checkpoint_path)

    brokers = os.environ["REDPANDA_BROKERS"]
    group_id = os.environ.get("KAFKA_GROUP_ID", "flink-pnl-consumer-v2")
    topic = "binance.price.ticks"

    kafka_props = {
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }

    source = FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )

    stream = env.add_source(source, source_name="redpanda-price-ticks")
    processed = stream.process(PnlProcessFunction())
    processed.add_sink(ClickHouseSinkFunction())

    logger.info("Starting Flink job 'flink-pnl-job'")
    env.execute("flink-pnl-job")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Commit**

```bash
git add flink_pnl/pnl_job.py flink_pnl/entrypoint.py
git commit -m "feat(flink-pnl): add PnlProcessFunction and entrypoint"
```

---

## Task 6: Dockerfile

**Files:**
- Create: `flink_pnl/Dockerfile`

- [ ] **Step 1: Create `flink_pnl/Dockerfile`**

```dockerfile
FROM apache/flink:2.2.1-java21

# The official flink image no longer ships a python3 variant tag.
# Install Python + PyFlink pip package. Versions must match exactly —
# JVM 2.2.1 + apache-flink==2.2.1. Update both together deliberately.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY flink_pnl/requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/flink/usrlib
COPY libs/ ./libs/
COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY flink_pnl/ ./flink_pnl/

ENV PYTHONPATH=/opt/flink/usrlib
ENV PYTHONUNBUFFERED=1

CMD ["python3", "-m", "flink_pnl.entrypoint"]
```

Note: `pnl_consumer/` is copied because `pnl_job.py` imports `_bootstrap_state` and `peek_reference_ts` from `pnl_consumer.pnl_consumer` — avoids duplicating bootstrap logic.

- [ ] **Step 2: Build image locally**

```bash
docker build --platform linux/amd64 -f flink_pnl/Dockerfile -t flink-pnl-test .
```

Expected: build succeeds. First build takes ~5 min (JVM base + pip). Layer cache makes subsequent builds fast.

- [ ] **Step 3: Smoke test — verify Python starts, Flink initialises, no crash on missing env**

```bash
docker run --rm --platform linux/amd64 \
  -e ENABLE_PRICE_SINK=false \
  -e ENABLE_PROD_SINK=false \
  -e ENABLE_BT_SINK=false \
  -e ENABLE_REAL_TRADE_SINK=false \
  -e REDPANDA_BROKERS=localhost:9092 \
  -e CLICKHOUSE_HOST=fake \
  -e CLICKHOUSE_PASSWORD=fake \
  -e CLICKHOUSE_PORT=8443 \
  -e CLICKHOUSE_USER=dev_ro3 \
  -e CLICKHOUSE_SECURE=true \
  flink-pnl-test 2>&1 | head -30
```

Expected output includes:
- Flink version log line (e.g. `Flink 2.2.1`)
- `PnlProcessFunction.open()` log
- `No PnL sinks enabled — skipping bootstrap`
- Then a Kafka connection error (no broker at localhost:9092) — this is expected and correct

What you must NOT see:
- `sed: can't read /config.yaml` (old JVM Flink noise)
- `ImportError` or `ModuleNotFoundError`
- Python traceback before the Kafka error

- [ ] **Step 4: Commit**

```bash
git add flink_pnl/Dockerfile
git commit -m "feat(flink-pnl): add Dockerfile based on apache/flink:2.2.1-java21"
```

---

## Task 7: Terraform — additive infra

**Files:**
- Modify: `infra/terraform/main.tf`

Add ECR repo, IAM role with S3 + CloudWatch policies, ECS task definition, and ECS service. All changes are additive — existing resources are untouched.

- [ ] **Step 1: Add ECR repository**

In `infra/terraform/main.tf`, after the `aws_ecr_repository.pnl_consumer` block (around line 1045), add:

```hcl
resource "aws_ecr_repository" "flink_pnl" {
  name                 = "trading-analysis-flink-pnl"
  image_tag_mutability = "MUTABLE"
  tags                 = local.common_tags
}
```

- [ ] **Step 2: Add IAM role and policies**

After the `aws_iam_role_policy.pnl_consumer_cloudwatch` block (around line 1076), add:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# IAM — flink-pnl task role (S3 checkpoints + CloudWatch)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "flink_pnl_task" {
  name = "${local.name_prefix}-flink-pnl-task"
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

resource "aws_iam_role_policy" "flink_pnl_s3" {
  name = "flink-pnl-s3-checkpoints"
  role = aws_iam_role.flink_pnl_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        "arn:aws:s3:::trading-analysis-data-v2",
        "arn:aws:s3:::trading-analysis-data-v2/flink-pnl-checkpoints/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy" "flink_pnl_cloudwatch" {
  name = "flink-pnl-cloudwatch"
  role = aws_iam_role.flink_pnl_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["cloudwatch:PutMetricData"]
      Resource = "*"
    }]
  })
}
```

- [ ] **Step 3: Add ECS task definition**

After the `aws_ecs_task_definition.pnl_consumer` for_each block (around line 1270), add:

```hcl
resource "aws_ecs_task_definition" "flink_pnl" {
  family                   = "${local.name_prefix}-flink-pnl"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 3072
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.flink_pnl_task.arn

  container_definitions = jsonencode([{
    name      = "flink-pnl"
    image     = "${aws_ecr_repository.flink_pnl.repository_url}:latest"
    essential = true
    secrets = [
      { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
      { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
    ]
    environment = [
      { name = "CLICKHOUSE_PORT",         value = "8443" },
      { name = "CLICKHOUSE_USER",         value = "dev_ro3" },
      { name = "CLICKHOUSE_SECURE",       value = "true" },
      { name = "REDPANDA_BROKERS",        value = "redpanda.${local.name_prefix}.local:9092" },
      { name = "KAFKA_GROUP_ID",          value = "flink-pnl-consumer-v2" },
      { name = "ENABLE_PRICE_SINK",       value = "false" },
      { name = "ENABLE_PROD_SINK",        value = "false" },
      { name = "ENABLE_REAL_TRADE_SINK",  value = "false" },
      { name = "ENABLE_BT_SINK",          value = "false" },
      { name = "FLINK_CHECKPOINT_DIR",    value = "s3://trading-analysis-data-v2/flink-pnl-checkpoints/" },
      { name = "AWS_REGION",              value = var.aws_region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.streaming.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "flink-pnl"
      }
    }
  }])

  tags = local.common_tags
}
```

- [ ] **Step 4: Add ECS service**

After the `aws_ecs_service.pnl_consumer` for_each block (around line 1380), add:

```hcl
resource "aws_ecs_service" "flink_pnl" {
  name            = "${local.name_prefix}-flink-pnl"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.flink_pnl.arn
  desired_count   = 0
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  lifecycle {
    ignore_changes = [task_definition]
  }

  tags = local.common_tags
}
```

`desired_count = 0` — service is provisioned but not running. Set to 1 manually or via CI/CD once the image is validated.

- [ ] **Step 5: Verify no syntax errors**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat(terraform): add flink-pnl ECR, IAM, ECS task and service (desired_count=0)"
```

---

## Task 8: CI/CD — build and deploy jobs

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Add path filter output in `detect-changes` job**

In the `detect-changes` job outputs section (around line 14), add after the `pnl-consumer` output line:

```yaml
      flink-pnl: ${{ steps.filter.outputs.flink-pnl == 'true' || steps.filter.outputs.terraform == 'true' || steps.force.outputs.flink-pnl == 'true' }}
```

In the `dorny/paths-filter` step filters (around line 36), add after the `pnl-consumer` filter:

```yaml
            flink-pnl:
              - 'flink_pnl/**'
```

In the `workflow_dispatch` force-outputs step (around line 48), add:

```yaml
          echo "flink-pnl=true" >> $GITHUB_OUTPUT
```

- [ ] **Step 2: Add `build-flink-pnl` job**

After the `build-pnl-consumer` job (around line 228), add:

```yaml
  build-flink-pnl:
    runs-on: ubuntu-latest
    needs: [test, terraform, detect-changes]
    if: |
      always() &&
      needs.test.result == 'success' &&
      (needs.terraform.result == 'success' || needs.terraform.result == 'skipped') &&
      needs.detect-changes.outputs.flink-pnl == 'true'
    permissions:
      id-token: write
      contents: read
    outputs:
      image_tag: ${{ steps.build.outputs.image_tag }}
    steps:
      - uses: actions/checkout@v6.0.2

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v4

      - name: Build and push flink-pnl image
        id: build
        env:
          REGISTRY: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com
        run: |
          [ -n "$REGISTRY" ] || { echo "ERROR: REGISTRY is empty"; exit 1; }
          IMAGE_URI=$REGISTRY/trading-analysis-flink-pnl:${{ github.sha }}
          IMAGE_CACHE=$REGISTRY/trading-analysis-flink-pnl:cache
          docker buildx build \
            -f flink_pnl/Dockerfile \
            --cache-from type=registry,ref=$IMAGE_CACHE \
            --cache-to   type=registry,ref=$IMAGE_CACHE,mode=max \
            --provenance=false \
            -t $IMAGE_URI \
            --push .
          echo "image_tag=${{ github.sha }}" >> $GITHUB_OUTPUT
```

- [ ] **Step 3: Add `deploy-flink-pnl` job**

After the `deploy-pnl-consumer` job (around line 500), add:

```yaml
  deploy-flink-pnl:
    runs-on: ubuntu-latest
    needs: [build-flink-pnl, detect-changes]
    if: |
      always() &&
      needs.build-flink-pnl.result == 'success' &&
      needs.detect-changes.outputs.flink-pnl == 'true'
    permissions:
      id-token: write
      contents: read
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Deploy flink-pnl to ECS
        env:
          IMAGE_TAG: ${{ needs.build-flink-pnl.outputs.image_tag }}
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
          REGISTRY: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com
        run: |
          set -eu
          [ -n "$IMAGE_TAG" ] || { echo "ERROR: IMAGE_TAG is empty — build job output missing"; exit 1; }
          IMAGE_URI=$REGISTRY/trading-analysis-flink-pnl:$IMAGE_TAG
          TASK_DEF=$(aws ecs describe-task-definition \
            --task-definition trading-analysis-flink-pnl \
            --region $DEPLOY_REGION --query taskDefinition --output json)
          NEW_TASK=$(echo "$TASK_DEF" | python3 -c "
          import sys, json
          img = sys.argv[1]
          td = json.load(sys.stdin)
          for c in td['containerDefinitions']:
              if c['name'] == 'flink-pnl':
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
            --service trading-analysis-flink-pnl \
            --task-definition "$NEW_ARN" \
            --force-new-deployment \
            --region $DEPLOY_REGION \
            --output json > /dev/null
          echo "flink-pnl task definition updated. Service desired_count=0 — start manually to validate."
```

Note: No stability wait — `desired_count=0` means the service is not running after deploy. Start manually via ECS console or `aws ecs update-service --desired-count 1` once ready to validate.

- [ ] **Step 4: Verify YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci-cd.yml'))" && echo "YAML valid"
```

Expected: `YAML valid`

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add build-flink-pnl and deploy-flink-pnl jobs"
```

---

## Task 9: Final verification

- [ ] **Step 1: Run all unit tests**

```bash
pytest -m unit -v
```

Expected: all tests pass including the 15 new `flink_pnl` tests. No import errors.

- [ ] **Step 2: Verify no stray references**

```bash
grep -rn "flink_pnl" \
  --include="*.py" --include="*.tf" --include="*.yml" \
  . | grep -v ".pyc" | grep -v "docs/"
```

Expected: only `flink_pnl/` source files, `tests/flink_pnl/`, `infra/terraform/main.tf`, `.github/workflows/ci-cd.yml`.

- [ ] **Step 3: Re-build Docker image cleanly**

```bash
docker build --platform linux/amd64 -f flink_pnl/Dockerfile -t flink-pnl-final . 2>&1 | tail -5
```

Expected: `Successfully built ...` with no errors.

- [ ] **Step 4: Confirm pnl_consumer is untouched**

```bash
git diff HEAD~9 -- pnl_consumer/ libs/ streaming/
```

Expected: no output — these directories are unchanged.

- [ ] **Step 5: Final commit if anything was missed**

```bash
git status
```

If any files are untracked or modified, commit them:

```bash
git add <files>
git commit -m "chore(flink-pnl): final cleanup"
```
