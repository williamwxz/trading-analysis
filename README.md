# Trading Analysis Platform

A simplified, automated trading analytics pipeline: **Binance API → streaming (Kafka/Redpanda) → pnl_consumer → ClickHouse Cloud → Grafana**.

## Core Stack
- **Streaming**: Binance WebSocket producer → Redpanda topic `binance.price.ticks` (ECS Fargate)
- **Real-time PnL**: `pnl_consumer` services (prod / bt / real-trade / price) → ClickHouse (ECS Fargate)
- **Batch recompute / repair**: `scripts/audit_pnl.py` (standalone window delete+recompute & audit)
- **Market-data backfill**: `backfill_prices` daily AWS Lambda (ccxt gap-fill of `futures_price_1min`)
- **Database**: ClickHouse Cloud (Analytics); 1hour/1day rollups via Materialized Views
- **Visualization**: Grafana Cloud (L1-L5 PnL Dashboards)
- **Cloud**: AWS Tokyo (`ap-northeast-1`)
- **CI/CD**: GitHub Actions

## Key Features
- **Real-time PnL**: Sub-minute PnL from streamed candles with anchor-chained continuity.
- **Window Repair**: `audit_pnl.py --fix-window` does idempotent delete+recompute and rebuilds rollups.
- **MV Rollups**: 1hour/1day tables maintained by ClickHouse Materialized Views (no orchestrator).
- **Infrastructure as Code**: 100% managed via Terraform.

## Project Structure
- `services/streaming/`: Binance WebSocket → Kafka/Redpanda producer.
- `services/pnl_consumer/`: Kafka consumer → real-time PnL → ClickHouse (per-mode ECS services).
- `services/backfill_prices/`: Daily Lambda for `futures_price_1min` historical gap-fill.
- `libs/computation/`: Shared PnL computation library (imported by the consumer and audit_pnl.py).
- `scripts/audit_pnl.py`: Batch PnL recompute/repair and audit.
- `infra/`: Terraform modules and Grafana provisioning.
- `infra/schemas/`: Optimized ClickHouse Cloud schema.

## Getting Started
### Local Development
1. `python -m venv venv`
2. `source venv/bin/activate`
3. `pip install -e ".[dev]"`
4. `docker compose up -d` (brings up Grafana; Grafana is Grafana Cloud in prod)

### Deployment
Pushes to the `main` branch automatically trigger the GitHub Actions CI/CD pipeline in Tokyo.

## Access Links
- **Grafana**: Grafana Cloud (L1-L5 PnL Dashboards)
