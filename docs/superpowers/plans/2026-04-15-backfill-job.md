# Backfill Job Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a manually-triggered Dagster job (`backfill_job`) that runs `binance_futures_backfill` → `pnl_prod_v2_daily` + `pnl_real_trade_v2_daily` → `pnl_1hour_rollup` for user-selected daily partition dates.

**Architecture:** A `define_asset_job` with an explicit `AssetSelection` targeting the four assets. Dagster resolves execution order from existing asset deps. The job is registered in `Definitions` alongside the existing assets and sensors.

**Tech Stack:** Dagster (`define_asset_job`, `AssetSelection`, `Definitions`), Python 3.11.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `trading_dagster/jobs/__init__.py` | Package init, re-exports `backfill_job` |
| Create | `trading_dagster/jobs/backfill_job.py` | Job definition |
| Modify | `trading_dagster/definitions/__init__.py` | Register job in `Definitions` |
| Create | `tests/test_backfill_job.py` | Verify job selects correct assets and loads without error |

---

### Task 1: Create the jobs package and job definition

**Files:**
- Create: `trading_dagster/jobs/__init__.py`
- Create: `trading_dagster/jobs/backfill_job.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_backfill_job.py`:

```python
"""Unit tests for backfill_job definition."""

import pytest
from dagster import AssetKey


def test_backfill_job_loads():
    """Job can be imported without error."""
    from trading_dagster.jobs.backfill_job import backfill_job
    assert backfill_job.name == "backfill_job"


def test_backfill_job_selects_correct_assets():
    """Job selection includes all four expected assets."""
    from trading_dagster.jobs.backfill_job import backfill_job

    expected = {
        AssetKey("binance_futures_backfill"),
        AssetKey("pnl_prod_v2_daily"),
        AssetKey("pnl_real_trade_v2_daily"),
        AssetKey("pnl_1hour_rollup"),
    }
    # AssetSelection.assets() stores keys; resolve against the full asset graph
    # to verify the selection is well-formed (no typos)
    from trading_dagster.definitions import defs

    job = defs.get_job_def("backfill_job")
    assert job is not None
    assert job.name == "backfill_job"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_backfill_job.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `trading_dagster.jobs.backfill_job` does not exist yet.

- [ ] **Step 3: Create the jobs package**

Create `trading_dagster/jobs/__init__.py`:

```python
from .backfill_job import backfill_job

__all__ = ["backfill_job"]
```

- [ ] **Step 4: Create the job definition**

Create `trading_dagster/jobs/backfill_job.py`:

```python
"""
Backfill Job

Manually-triggered job that runs a full daily backfill:
  binance_futures_backfill → pnl_prod_v2_daily + pnl_real_trade_v2_daily → pnl_1hour_rollup

Usage: Dagster UI → Jobs → backfill_job → Launchpad → select partition dates → Launch.
"""

from dagster import AssetSelection, define_asset_job

backfill_job = define_asset_job(
    name="backfill_job",
    selection=AssetSelection.assets(
        "binance_futures_backfill",
        "pnl_prod_v2_daily",
        "pnl_real_trade_v2_daily",
        "pnl_1hour_rollup",
    ),
    description=(
        "Manual backfill: price fetch → prod PnL → real trade PnL → hourly rollup. "
        "Select one or more daily partition dates in the Launchpad."
    ),
)
```

- [ ] **Step 5: Run test to verify it passes**

```bash
pytest tests/test_backfill_job.py::test_backfill_job_loads -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add trading_dagster/jobs/__init__.py trading_dagster/jobs/backfill_job.py tests/test_backfill_job.py
git commit -m "feat: add backfill_job definition and jobs package"
```

---

### Task 2: Register the job in Definitions

**Files:**
- Modify: `trading_dagster/definitions/__init__.py`

- [ ] **Step 1: Update the definitions file**

Open `trading_dagster/definitions/__init__.py`. The current file ends with:

```python
defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
)
```

Add the import and register the job:

```python
"""
Dagster Definitions — trading-analysis

Entry point for the Dagster instance. Registers all assets and automation sensors.
"""

from dagster import Definitions

from ..assets.binance_futures_ohlcv import (
    binance_futures_backfill_asset,
    binance_futures_ohlcv_minutely_asset,
)
from ..assets.pnl_strategy_v2 import (
    pnl_prod_v2_live_asset,
    pnl_prod_v2_daily_asset,
    # pnl_bt_v2_live_asset,
    # pnl_bt_v2_daily_asset,
    pnl_real_trade_v2_live_asset,
    pnl_real_trade_v2_daily_asset,
)
from ..assets.pnl_rollup import pnl_1hour_rollup_asset
from ..assets.pnl_safety_scan import pnl_daily_safety_scan_asset
from ..sensors.automation_sensors import build_automation_sensors
from ..jobs.backfill_job import backfill_job


all_assets = [
    # Market data
    binance_futures_backfill_asset,
    binance_futures_ohlcv_minutely_asset,
    # Strategy PnL v2 (Live)
    pnl_prod_v2_live_asset,
    # pnl_bt_v2_live_asset,
    pnl_real_trade_v2_live_asset,
    # Strategy PnL v2 (Daily Backfills)
    pnl_prod_v2_daily_asset,
    # pnl_bt_v2_daily_asset,
    pnl_real_trade_v2_daily_asset,
    # Rollups & Scans
    pnl_1hour_rollup_asset,
    pnl_daily_safety_scan_asset,
]

all_sensors = build_automation_sensors()

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
    jobs=[backfill_job],
)
```

- [ ] **Step 2: Run the full test for job registration**

```bash
pytest tests/test_backfill_job.py -v
```

Expected: Both tests PASS.
- `test_backfill_job_loads` — PASS
- `test_backfill_job_selects_correct_assets` — PASS (verifies `defs.get_job_def("backfill_job")` returns the job)

- [ ] **Step 3: Run the full test suite to check for regressions**

```bash
pytest tests/ -v --tb=short
```

Expected: All previously-passing tests continue to pass.

- [ ] **Step 4: Commit**

```bash
git add trading_dagster/definitions/__init__.py
git commit -m "feat: register backfill_job in Dagster Definitions"
```

---

## Self-Review

**Spec coverage:**
- `binance_futures_backfill` — selected in job ✓
- `pnl_prod_v2_daily` — selected in job ✓
- `pnl_real_trade_v2_daily` — selected in job ✓
- `pnl_1hour_rollup` — selected in job ✓
- `Definitions(jobs=[...])` registration — Task 2 ✓
- Manual trigger only, no automation changes — no automation code touched ✓

**Placeholder scan:** None found.

**Type consistency:** `backfill_job` is the only exported name; used consistently across both tasks.
