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
| CI/CD | GitHub Actions → OIDC → AWS |

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

## GitHub → AWS Deployment Setup

This repo uses **GitHub Actions OIDC** (no long-lived AWS keys). One-time setup:

### 1 — Create the OIDC provider in AWS

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1 \
  --region ap-northeast-1
```

### 2 — Create the IAM role

Create `infra/terraform/iam_github_actions.tf` or apply this policy manually. The trust policy allows only your repo:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_GITHUB_ORG/trading-analysis:*"
        },
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
```

Attach these AWS managed policies to the role:
- `AmazonECS_FullAccess`
- `AmazonEC2ContainerRegistryFullAccess`
- `AmazonS3FullAccess`
- Custom inline policy for Terraform state (S3 + DynamoDB if using locking)

### 3 — Add GitHub Secrets

In your repo → **Settings → Secrets and variables → Actions**, add:

| Secret | Value |
|---|---|
| `AWS_ACCOUNT_ID` | Your 12-digit AWS account ID |
| `AWS_ROLE_TO_ASSUME` | `arn:aws:iam::<ACCOUNT_ID>:role/trading-analysis-github-actions` |
| `CLICKHOUSE_HOST` | Your ClickHouse Cloud hostname |
| `CLICKHOUSE_PASSWORD` | Your ClickHouse Cloud password |

### 4 — Create a GitHub Environment

Go to **Settings → Environments → New environment**, name it `production`. This gates the deployment jobs behind any protection rules you want (e.g. required reviewers).

### 5 — Bootstrap Terraform state bucket

```bash
aws s3 mb s3://trading-analysis-tfstate --region ap-northeast-1
aws s3api put-bucket-versioning \
  --bucket trading-analysis-tfstate \
  --versioning-configuration Status=Enabled
```

### 6 — Push to main

```bash
git push origin main
```

The workflow runs: **test → terraform plan → terraform apply → docker build → push ECR → apply schema (if changed) → deploy ECS**.

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
