# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mini trading analytics pipeline. Processes strategy PnL data with Binance API polling, ClickHouse Cloud analytics, Dagster orchestration, and Grafana dashboards. Deployed on AWS ECS Fargate in `ap-northeast-1` (Tokyo).

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Code quality (line length: 88, target Python 3.11)
black .
ruff check --fix .
mypy .

# Tests (unit tests require no external services)
pytest
pytest tests/test_pnl_compute.py -v
pytest -m unit        # fast, no external deps
pytest -m integration # requires live ClickHouse/S3

# Local Dagster (requires Postgres for metadata)
docker compose up -d postgres
dagster dev           # http://localhost:3000

# Full local stack (Dagster + Grafana)
docker compose up -d
# Dagster: http://localhost:3000, Grafana: http://localhost:3001 (admin/admin)
```

## Architecture

### Data Flow

```
Binance Futures REST API (every 5 min, CCXT)
    → analytics.futures_price_1min

External strategy service (push)
    → analytics.strategy_output_history_v2 / _bt_v2

PnL refresh assets (live: every ~5 min, daily: partitioned backfill)
    → analytics.strategy_pnl_1min_{prod,bt,real_trade}_v2

Rollup asset (hourly, argMax aggregation)
    → analytics.strategy_pnl_1hour_{prod,bt,real_trade}_v2
```

### Dagster Entry Point

`pyproject.toml` sets `[tool.dagster] module_name = "trading_dagster"`. Dagster discovers assets via `trading_dagster/definitions/__init__.py`, which imports all `*_asset` variables and registers them with a single `AutomationConditionSensorDefinition` sensor (30s interval):

| Sensor | Asset Groups |
|--------|-------------|
| `trading_analysis_automation_sensor` | all assets (market_data + strategy_pnl) |

**Active assets** (registered in `definitions/__init__.py`):
- `binance_futures_backfill` — daily partitioned market data
- `binance_futures_ohlcv_minutely` — live 5-min market data
- `pnl_prod_v2_live` / `pnl_prod_v2_daily` — production PnL, live + backfill
- `pnl_real_trade_v2_live` / `pnl_real_trade_v2_daily` — real trade PnL, live + backfill
- `pnl_1hour_rollup` — hourly aggregation (runs 15 min past each hour)
- `pnl_daily_safety_scan` — row-count validation (02:00 UTC daily)

**Commented out** (defined in `pnl_strategy_v2.py` but not registered): `pnl_bt_v2_live_asset`, `pnl_bt_v2_daily_asset` — backtest PnL is not currently active.

### Dual Asset Strategy

Every data source has two complementary assets:
- **Partitioned (backfill)**: `DailyPartitionsDefinition`, idempotent DELETE+INSERT, triggered manually or via backfill UI
- **Unpartitioned (live)**: `AutomationCondition.on_cron(...)`, uses watermark to fetch only new data

Example: `binance_futures_backfill` (daily partitions from 2024-01-01) + `binance_futures_ohlcv_minutely` (every 5 min, auto-incremental).

### Incremental Refresh Pattern (PnL Assets)

1. **Watermark check**: read `analytics.pnl_refresh_watermarks` (or fall back to `max(updated_at) - 2h` from target)
2. **Skip if up to date**: compare source `max(revision_ts)` against watermark
3. **Python PnL computation**: `fetch_new_bars_*` → `fetch_anchors` → `fetch_prices` → `compute_*_pnl` → `insert_rows`
4. **Write watermark**: update `analytics.pnl_refresh_watermarks` after successful insert

`_CHUNK_DAYS = 7` in `pnl_strategy_v2.py` controls how many days of bars are processed per ClickHouse INSERT to cap memory usage. Each chunk fetches only that window's prices and chains anchors forward at the end.

### Why Python for PnL (not SQL)

V1 used a SQL anchor CTE which fails when multiple bars arrive in a single INSERT batch — SQL can't chain bar N's end-price into bar N+1's start-price within the same query. V2 uses Python to iterate bars in order, explicitly passing `anchor_pnl` and `anchor_price` forward.

PnL formula: `cumulative_pnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price`

### Real Trade vs Prod/Bt

`real_trade` bars have multiple revisions per bar (live updates). `fetch_new_bars_real_trade` uses a ClickHouse window function (`lagInFrame`/`leadInFrame`) to group revisions by `execution_ts = toStartOfMinute(revision_ts + 59s)` and keeps only the latest revision per group. `compute_real_trade_pnl` advances `active_position` as each `execution_ts` is reached while iterating 1-min intervals. Prod/bt use `argMin(row_json, revision_ts)` (first revision wins) and a simpler `compute_prod_pnl`.

**Note**: `_refresh_pnl_real_trade` in `pnl_strategy_v2.py` is currently a stub — it returns an empty `MaterializeResult` without running the actual computation. The computation logic exists in `pnl_compute.py` (`fetch_new_bars_real_trade`, `compute_real_trade_pnl`) but is not wired up yet.

### ClickHouse Patterns

- **All queries through** `trading_dagster/utils/clickhouse_client.py`: `get_client()`, `query_rows()`, `query_dicts()`, `query_scalar()`, `execute()`, `insert_rows()`
- **No connection pooling**: each call creates a new connection unless a `client=` is passed explicitly. Pass a shared client when doing multiple operations in one asset run to avoid redundant connections.
- **ReplacingMergeTree(updated_at)**: re-inserting rows replaces older ones after background merge. Use `FINAL` in SELECT to get deduplicated results immediately, but avoid `FINAL` on large frequently-queried tables (it forces in-memory merge). Prefer `LIMIT 1 BY key ORDER BY key, updated_at DESC` to read the latest row per key using the sort index instead.
- **Idempotent upserts**: partitioned assets DELETE the day+instrument partition before inserting; unpartitioned assets rely on ReplacingMergeTree.
- **Batch insert**: `insert_rows()` defaults to 200k rows per batch.
- **Datetime types**: ClickHouse-connect requires Python `datetime` objects for DateTime columns. `_prepare_rows_for_clickhouse()` in `pnl_strategy_v2.py` handles string→datetime conversion for PnL rows (ts at index 7, updated_at at index 14 of PROD_INSERT_COLUMNS).

### ClickHouse Cloud Connection

Required env vars: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` (8443), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE` (true).

Schema lives in `schemas/clickhouse_cloud.sql` (apply once). No `Replicated*` prefix needed — Cloud handles replication.

## CI/CD

AWS CodePipeline triggers automatically on push to `main`. Stages:
1. **Test** (`buildspec/test.yml`): `pytest tests/ -v --tb=short`
2. **Build** (`buildspec/build.yml`): builds two Docker images (main + Grafana), pushes to ECR with both commit-SHA and `:latest` tags
3. **Deploy** (`buildspec/deploy.yml`): `terraform apply`, updates ECS task definitions

VPC NAT Gateway provides a static outbound IP (`18.182.133.240`) — needed for ClickHouse Cloud IP allowlisting.

## Deployment

```bash
# Build and push Docker image
docker build -t trading-analysis .
aws ecr get-login-password | docker login --username AWS --password-stdin <ecr-url>
docker push <ecr-url>:latest

# Apply Terraform (ECS Fargate + RDS + Grafana)
cd infra/terraform && terraform init
terraform apply -var="github_repo=williamwxz/trading-analysis"

# Apply ClickHouse schema (one-time)
clickhouse-client --host <host> --secure --password <pw> < schemas/clickhouse_cloud.sql
```

ECR repos: `trading-analysis-dagster`, `trading-analysis-grafana`  
S3 buckets: `trading-analysis-data-v2`, `trading-analysis-pipeline-v2`
