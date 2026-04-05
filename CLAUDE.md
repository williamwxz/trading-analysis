# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

Mini trading analytics pipeline — a simplified fork of falcon-lakehouse. Processes strategy PnL data with Binance API polling, ClickHouse Cloud analytics, Dagster orchestration, and Grafana dashboards. Deployed on AWS ECS Fargate.

**Key simplifications from falcon-lakehouse:**
- No EKS → ECS Fargate
- No StarRocks → ClickHouse Cloud only
- No Amber Data / Redpanda → Binance REST API polling via Dagster
- V2 PnL pipeline only (no v1)
- No bronze/silver/gold layers → direct analytics tables

## Repository Structure

```
trading-analysis/
├── trading_dagster/              # Core Python Dagster package
│   ├── assets/                   # Dagster pipeline assets
│   │   ├── binance_futures_ohlcv.py    # Binance 1min OHLCV polling
│   │   ├── pnl_prod_v2_refresh.py      # Production PnL refresh
│   │   ├── pnl_bt_v2_refresh.py        # Backtest PnL refresh
│   │   ├── pnl_real_trade_v2_refresh.py # Real trade PnL refresh
│   │   ├── pnl_rollup.py              # Hourly rollup
│   │   └── pnl_safety_scan.py         # Daily safety scan
│   ├── definitions/
│   │   └── __init__.py           # Dagster entry point (Definitions)
│   ├── sensors/
│   │   └── automation_sensors.py # 2 automation sensors
│   └── utils/
│       ├── clickhouse_client.py  # ClickHouse Cloud client (clickhouse-connect)
│       └── pnl_compute.py       # Anchor-chained PnL computation engine
├── schemas/
│   └── clickhouse_cloud.sql      # ClickHouse Cloud DDL (apply once)
├── infra/
│   ├── terraform/main.tf         # ECS Fargate + S3 + MSK + Grafana
│   └── grafana/
│       ├── dashboards/           # 5 strategy PnL dashboards (L1-L5)
│       └── provisioning/         # Datasource + dashboard provisioning
├── tests/
│   └── test_pnl_compute.py      # Unit tests for PnL computation
├── pyproject.toml                # Python package config
├── dagster.yaml                  # Dagster instance config
├── Dockerfile                    # Production container
├── docker-compose.yml            # Local dev stack
└── .env.example                  # Environment variables template
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Dagster 1.11+ |
| Analytics DB | ClickHouse Cloud (HTTPS, port 8443) |
| Data Source | Binance Futures REST API |
| Streaming | Amazon MSK Serverless (reserved for future use) |
| Data Lake | AWS S3 |
| Infrastructure | AWS ECS Fargate, Terraform |
| Monitoring | Grafana + ClickHouse datasource |
| Python | 3.11+ |

## Data Pipeline

```
Binance Futures API (REST polling, every 2 min)
    ↓
analytics.futures_price_1min (ClickHouse Cloud)

Strategy execution service (external push)
    ↓
analytics.strategy_output_history_v2 / _bt_v2

PnL refresh assets (every 5/10 min, Python anchor-chaining)
    ↓
analytics.strategy_pnl_1min_{prod,bt,real_trade}_v2

Rollup asset (hourly, argMax aggregation)
    ↓
analytics.strategy_pnl_1hour_{prod,bt,real_trade}_v2
```

## Automation

| Sensor | Asset Groups | Interval |
|--------|-------------|----------|
| `market_data_automation_sensor` | market_data | 60s |
| `strategy_pnl_automation_sensor` | strategy_pnl | 60s |

Assets use `AutomationCondition.on_cron()` with `~in_progress()` guard.

## Local Development

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e .[dev]

# Set environment variables
cp .env.example .env
# Edit .env with your ClickHouse Cloud credentials

# Run Dagster locally (needs Postgres for metadata)
docker compose up -d postgres
dagster dev  # http://localhost:3000

# Or run full stack
docker compose up -d
# Dagster: http://localhost:3000
# Grafana: http://localhost:3001 (admin/admin)
```

## Code Quality

```bash
black .
ruff check --fix .
mypy .
pytest
pytest tests/test_pnl_compute.py -v  # run specific test
```

Line length: 88. Target Python: 3.11.

## ClickHouse Cloud

Uses `clickhouse-connect` library (native HTTPS protocol). All queries go through `trading_dagster/utils/clickhouse_client.py`.

Required env vars: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` (8443), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE` (true).

Schema: Apply `schemas/clickhouse_cloud.sql` to initialize. Uses `ReplacingMergeTree(updated_at)` for idempotent upserts — no `Replicated*` prefixes needed (Cloud handles replication).

## Key Design Decisions

- **V2-only**: Dropped v1 SQL-based PnL (anchor CTE can't chain between bars within a batch). V2 uses Python for correct bar-to-bar cumulative PnL.
- **No streaming layer**: Binance API polling replaces Amber Data → Redpanda → Kafka MV. Simpler, fewer moving parts, same 1-min granularity.
- **ECS Fargate**: No Kubernetes complexity. Dagster webserver + daemon in one task definition. Grafana in a separate task.
- **ClickHouse Cloud**: No operator, no replicas to manage. HTTPS endpoint, pay-per-query.

## Deployment

```bash
# Build and push Docker image
docker build -t trading-analysis .
aws ecr get-login-password | docker login --username AWS --password-stdin <ecr-url>
docker tag trading-analysis:latest <ecr-url>:latest
docker push <ecr-url>:latest

# Apply Terraform
cd infra/terraform
terraform init
terraform plan
terraform apply

# Apply ClickHouse schema (one-time)
clickhouse-client --host <cloud-host> --secure --password <pw> < schemas/clickhouse_cloud.sql
```
