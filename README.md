# trading-analysis

Mini trading analytics pipeline — strategy PnL processing with Binance API polling, ClickHouse Cloud, Dagster, and Grafana. Deployed on AWS ECS Fargate (Tokyo).

Simplified fork of falcon-lakehouse: no EKS, no StarRocks, no Amber Data.

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  External Strategy Execution Service                                 │
│  (pushes signals via direct INSERT)                                  │
└──────────────────┬──────────────────────────────────────────────────┘
                   │
         ┌─────────▼──────────┐         ┌──────────────────────┐
         │ strategy_output_   │         │    Binance Futures    │
         │ history_v2         │         │    REST API           │
         │ strategy_output_   │         │  /fapi/v1/klines      │
         │ history_bt_v2      │         │  (1-min OHLCV)        │
         └─────────┬──────────┘         └──────────┬───────────┘
                   │                               │  every 2 min
                   │                               ▼
                   │                  ┌────────────────────────┐
                   │                  │  futures_price_1min    │
                   │                  │  (ClickHouse Cloud)    │
                   │                  └────────────┬───────────┘
                   │                               │
         ──────────▼───────────────────────────────▼──────────────
                                 Dagster
                         (ECS Fargate, ap-northeast-1)
         ───────────────────────────────────────────────────────────
                   │
       ┌───────────┼───────────────┐
       │           │               │
       ▼           ▼               ▼
  pnl_prod    pnl_bt_v2    pnl_real_trade
  _v2_refresh _refresh     _v2_refresh
  (every 5m)  (every 10m)  (every 5m)
       │
       │  Python anchor-chained PnL computation
       │  Expands bars → 1-min intervals
       │  Anchor: last committed (cumulative_pnl, price)
       │
       ▼
  ┌──────────────────────────────────────┐
  │         ClickHouse Cloud             │
  │  analytics.strategy_pnl_1min_*_v2   │
  │  (prod / bt / real_trade)            │
  └───────────────────┬──────────────────┘
                      │  every hour (15 min past)
                      ▼
  ┌──────────────────────────────────────┐
  │  analytics.strategy_pnl_1hour_*_v2  │
  │  argMax per hour bucket              │
  └───────────────────┬──────────────────┘
                      │
                      ▼
  ┌──────────────────────────────────────┐
  │            Grafana                   │
  │  (ECS Fargate, ClickHouse datasource)│
  │  L1 Instance / L2 Sid+Underlying     │
  │  L3 Strategy / L4 Underlying         │
  │  L5 Portfolio                        │
  └──────────────────────────────────────┘
```

---

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Dagster 1.11+ |
| Analytics DB | ClickHouse Cloud (HTTPS, port 8443) |
| Market data | Binance Futures REST API |
| Infrastructure | AWS ECS Fargate (ap-northeast-1) |
| Data lake | AWS S3 |
| Streaming (reserved) | Amazon MSK Serverless |
| Dashboards | Grafana 11 + grafana-clickhouse-datasource |
| IaC | Terraform 1.5 |
| CI/CD | AWS CodePipeline (us-east-1) → ECS (ap-northeast-1) |

---

## ClickHouse Tables

| Table | Written by | Purpose |
|---|---|---|
| `analytics.strategy_output_history_v2` | External service | Production bar revisions |
| `analytics.strategy_output_history_bt_v2` | External service | Backtest bar revisions |
| `analytics.futures_price_1min` | Dagster (Binance API) | 1-min OHLCV prices |
| `analytics.strategy_pnl_1min_prod_v2` | Dagster | Anchor-chained production PnL |
| `analytics.strategy_pnl_1min_bt_v2` | Dagster | Anchor-chained backtest PnL |
| `analytics.strategy_pnl_1min_real_trade_v2` | Dagster | Real-execution PnL |
| `analytics.strategy_pnl_1hour_*_v2` | Dagster (rollup) | Hourly aggregations |
| `analytics.pnl_refresh_watermarks` | Dagster | Incremental progress tracking |

---

## Dagster Assets

| Asset | Group | Cron | Description |
|---|---|---|---|
| `binance_futures_ohlcv_1min` | market_data | `*/2 * * * *` | Polls Binance 1-min klines |
| `pnl_prod_v2_refresh` | strategy_pnl | `*/5 * * * *` | Production PnL refresh |
| `pnl_bt_v2_refresh` | strategy_pnl | `*/10 * * * *` | Backtest PnL refresh |
| `pnl_real_trade_v2_refresh` | strategy_pnl | `*/5 * * * *` | Real-trade PnL refresh |
| `pnl_1hour_rollup` | strategy_pnl | `15 * * * *` | Hourly rollup |
| `pnl_daily_safety_scan` | strategy_pnl | `0 2 * * *` | 7-day lookback validation |

---

## Local Development

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

cp .env.example .env
# Add your ClickHouse Cloud credentials to .env

docker compose up -d postgres
dagster dev            # http://localhost:3000

# Full stack (Dagster + Grafana)
docker compose up -d   # Grafana at http://localhost:3001
```

---

## CI/CD: AWS CodePipeline Setup

The pipeline is fully AWS-native using **AWS CodePipeline + CodeConnections** (no GitHub secrets or OIDC needed). The pipeline lives in `us-east-1` (where the GitHub connection was created) and deploys to `ap-northeast-1` (Tokyo).

```
GitHub (main branch)
    → CodeConnections (us-east-1)
    → CodePipeline: Test → Build → Deploy
                                      └→ Terraform apply (ap-northeast-1)
                                      └→ ECS task update (ap-northeast-1)
                                      └→ ClickHouse schema (if changed)
```

### One-time setup

**1 — Activate the GitHub connection**

The CodeConnections connector is already created:
```
arn:aws:codeconnections:us-east-1:339163283253:connection/31f9d517-f760-4d90-b7d8-b87cde5be67f
```

If it shows `PENDING`, activate it in the AWS Console:
[AWS Console → CodePipeline → Settings → Connections](https://us-east-1.console.aws.amazon.com/codesuite/settings/connections) → select the connection → **Update pending connection** → authorize with GitHub.

**2 — Bootstrap the Terraform state bucket (Tokyo)**

```bash
aws s3 mb s3://trading-analysis-tfstate --region ap-northeast-1
aws s3api put-bucket-versioning \
  --bucket trading-analysis-tfstate \
  --versioning-configuration Status=Enabled
```

**3 — Seed secrets in AWS Secrets Manager**

```bash
# ClickHouse Cloud credentials
aws secretsmanager create-secret \
  --name trading-analysis-production/clickhouse \
  --region ap-northeast-1 \
  --secret-string '{"host":"<ch-host>","password":"<ch-password>"}'

# Dagster Postgres (RDS or Aurora Serverless)
aws secretsmanager create-secret \
  --name trading-analysis-production/dagster-pg \
  --region ap-northeast-1 \
  --secret-string '{"host":"<pg-host>","password":"<pg-password>"}'
```

**4 — Apply Terraform to create the pipeline**

```bash
cd infra/terraform
terraform init
terraform apply \
  -var="aws_region=ap-northeast-1" \
  -var="environment=production" \
  -var="github_repo=williamwxz/trading-analysis"
```

This creates the CodePipeline, three CodeBuild projects (test/build/deploy), IAM roles, S3 artifact bucket, and CloudWatch log groups — all in `us-east-1`, targeting `ap-northeast-1` for the actual workloads.

**5 — Push to main**

```bash
git push origin main
```

The pipeline auto-triggers on every push to `main` and runs: **Test → Build (Docker → ECR) → Deploy (Terraform + ECS + schema if changed)**.

View pipeline status: [CodePipeline Console](https://us-east-1.console.aws.amazon.com/codesuite/codepipeline/pipelines/trading-analysis-production/view)

---

## Deployment

```bash
# One-time: apply ClickHouse schema
clickhouse-client --host <host> --port 9440 --secure \
  --password <pw> --multiquery < schemas/clickhouse_cloud.sql

# Build & push image manually
docker build -t trading-analysis .
aws ecr get-login-password --region ap-northeast-1 | \
  docker login --username AWS --password-stdin <account>.dkr.ecr.ap-northeast-1.amazonaws.com
docker tag trading-analysis:latest <account>.dkr.ecr.ap-northeast-1.amazonaws.com/trading-analysis-dagster:latest
docker push <account>.dkr.ecr.ap-northeast-1.amazonaws.com/trading-analysis-dagster:latest

# Terraform
cd infra/terraform && terraform init && terraform apply
```
