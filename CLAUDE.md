# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mini trading analytics pipeline — a simplified fork of falcon-lakehouse. Processes strategy PnL data with Binance API polling, ClickHouse Cloud analytics, Dagster orchestration, and Grafana dashboards. Deployed on AWS ECS Fargate.

**Key simplifications from falcon-lakehouse:**
- No EKS → ECS Fargate; no StarRocks → ClickHouse Cloud only
- No Amber Data / Redpanda → Binance REST API polling via Dagster
- V2 PnL pipeline only (no v1 SQL-based anchor CTE approach)
- No bronze/silver/gold layers → direct analytics tables

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e .[dev]

# Code quality
black .
ruff check --fix .
mypy .

# Tests (all unit, no ClickHouse required)
pytest
pytest tests/test_pnl_compute.py -v        # specific file
pytest -m unit                              # unit tests only
pytest -m integration                       # requires ClickHouse/S3

# Local Dagster (needs Postgres for metadata)
docker compose up -d postgres
dagster dev                                 # http://localhost:3000

# Full local stack
docker compose up -d
# Dagster: http://localhost:3000, Grafana: http://localhost:3001 (admin/admin)
```

Line length: 88. Target Python 3.11.

## Architecture

### Data Pipeline

```
Binance Futures REST API (every 2 min)
    → analytics.futures_price_1min

External strategy service (push)
    → analytics.strategy_output_history_v2 / _bt_v2

PnL refresh assets (every 5/10 min, Python anchor-chaining)
    → analytics.strategy_pnl_1min_{prod,bt,real_trade}_v2

Rollup asset (hourly, argMax aggregation)
    → analytics.strategy_pnl_1hour_{prod,bt,real_trade}_v2
```

### Dagster Entry Point

`pyproject.toml` sets `[tool.dagster] module_name = "trading_dagster"`. Dagster discovers assets via `trading_dagster/definitions/__init__.py`, which imports all `*_asset` variables from the assets package and registers them with two `AutomationConditionSensorDefinition` sensors.

### Automation Sensors

| Sensor | Asset Groups | Interval |
|--------|-------------|----------|
| `market_data_automation_sensor` | `market_data` | 60s |
| `strategy_pnl_automation_sensor` | `strategy_pnl` | 60s |

Assets use `AutomationCondition.on_cron("...") & ~AutomationCondition.in_progress()`.

### Incremental Refresh Pattern (PnL assets)

Each PnL refresh asset follows this pattern:
1. **Watermark check**: reads `analytics.pnl_refresh_watermarks` (or falls back to `max(updated_at) - 2h` from target) to find `since`
2. **Skip if up to date**: compare source `max(revision_ts)` against watermark
3. **Python PnL computation**: `fetch_new_bars_*` → `fetch_anchors` → `fetch_prices` → `compute_*_pnl` → `insert_rows`
4. **Write watermark**: update `analytics.pnl_refresh_watermarks` after successful insert
5. **Tail fill**: pure SQL fills gap from last bar timestamp to `now()` using current position and live prices

The **tail fill** runs after the Python refresh to keep PnL current between bar boundaries. It's pure SQL because it only needs one anchor row (no chaining required).

### Why Python for PnL (not SQL)

V1 used a SQL anchor CTE. It fails when multiple bars arrive in a single INSERT batch: SQL can't chain bar N's end-price into bar N+1's start-price within the same query. V2 uses Python to iterate bars in order, passing `anchor_pnl` and `anchor_price` forward explicitly.

### Real Trade vs Prod/Bt

`real_trade` bars have multiple revisions per bar (live updates). `fetch_new_bars_real_trade` uses a window function to select the latest revision per execution group, identified by `execution_ts = toStartOfMinute(revision_ts + 59s)`. Prod/bt use `argMin(row_json, revision_ts)` (first revision wins).

### ClickHouse Patterns

- **All queries through** `trading_dagster/utils/clickhouse_client.py` — `get_client()`, `query_rows()`, `query_dicts()`, `query_scalar()`, `execute()`, `insert_rows()`
- **No connection pooling**: each call to `query_*`/`execute`/`insert_rows` creates a new connection unless a `client=` is passed explicitly. Pass a shared `client` when doing multiple operations in one asset run (see `binance_futures_ohlcv.py`)
- **ReplacingMergeTree deduplication**: use `FINAL` in SELECT queries on PnL tables to get deduplicated results (e.g., in rollup watermark queries)
- **Idempotent upserts**: tables use `ReplacingMergeTree(updated_at)` — re-inserting rows with newer `updated_at` replaces older ones after background merges

### ClickHouse Cloud Connection

Required env vars: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` (8443), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE` (true).

Schema: `schemas/clickhouse_cloud.sql` (apply once). No `Replicated*` prefix needed — Cloud handles replication.

## Deployment

```bash
# Build and push Docker image
docker build -t trading-analysis .
aws ecr get-login-password | docker login --username AWS --password-stdin <ecr-url>
docker tag trading-analysis:latest <ecr-url>:latest
docker push <ecr-url>:latest

# Apply Terraform (ECS Fargate + S3 + Grafana)
cd infra/terraform && terraform init && terraform apply

# Apply ClickHouse schema (one-time)
clickhouse-client --host <cloud-host> --secure --password <pw> < schemas/clickhouse_cloud.sql
```
