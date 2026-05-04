# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mini trading analytics pipeline. Processes strategy PnL data with Binance API polling, ClickHouse Cloud analytics, Dagster orchestration, and Grafana Cloud dashboards. Dagster deployed on AWS ECS Fargate in `ap-northeast-1` (Tokyo). Grafana is Grafana Cloud (not self-hosted).

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
pytest -m unit                    # fast, no external deps
pytest -m integration             # requires live ClickHouse/S3
pytest -m streaming_integration   # requires Docker + Binance network

# Local Dagster (requires Postgres for metadata)
docker compose up -d postgres
dagster dev           # http://localhost:3000

# Full local stack (Dagster only — Grafana is Grafana Cloud, not local)
docker compose up -d
# Dagster: http://localhost:3000

# Access production Dagster UI (ALB — no tooling required)
# http://trading-analysis-dagster-944050731.ap-northeast-1.elb.amazonaws.com
```

## Architecture

### Services

Three independent ECS Fargate services, each with its own Dockerfile:

| Service | Directory | Role |
|---------|-----------|------|
| `trading-analysis-dagster` | `trading_dagster/` | Dagster orchestration — batch PnL refresh, market data backfill |
| `trading-analysis-streaming` | `streaming/` | Binance WebSocket → Kafka/Redpanda topic `binance.price.ticks` |
| `trading-analysis-pnl-consumer` | `pnl_consumer/` | Kafka consumer → real-time PnL → ClickHouse |

Adding new instruments requires updating `INSTRUMENTS` in both `streaming/binance_ws_consumer.py` and the Dagster market data assets.

### Data Flow

**Real-time path** (sub-minute latency):
```
Binance WebSocket (1m closed candles)
    → streaming/binance_ws_consumer.py
    → Kafka topic: binance.price.ticks  (CandleEvent JSON)
    → pnl_consumer/pnl_consumer.py  (GROUP_ID: flink-pnl-consumer, flush every 10 candles)
    → analytics.futures_price_1min
    → analytics.strategy_pnl_1min_prod_v2
    → analytics.strategy_pnl_1hour_prod_v2  (hourly snapshot upsert on each flush)
```

**Batch path** (Dagster, every ~5 min or daily):
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
- `pnl_prod_v2_daily` — production PnL daily backfill
- `pnl_real_trade_v2_daily` — real trade PnL daily backfill
- `pnl_bt_v2_daily` — backtest PnL daily backfill
- `pnl_1hour_prod_rollup` — hourly rollup prod 1min → 1hour, daily partition, auto-triggered by `pnl_prod_v2_daily`
- `pnl_1hour_real_trade_rollup` — hourly rollup real_trade 1min → 1hour, daily partition, auto-triggered by `pnl_real_trade_v2_daily`
- `pnl_1hour_bt_rollup` — hourly rollup bt 1min → 1hour, daily partition, auto-triggered by `pnl_bt_v2_daily`
- `clickhouse_connectivity_check` — infra health check
- `postgres_cleanup` — Dagster metadata DB cleanup

**Not registered** (defined in `pnl_strategy_v2.py` but inactive): `pnl_bt_v2_live_asset`, `pnl_prod_v2_live_asset`, `pnl_real_trade_v2_live_asset` — live paths not currently active.

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

`real_trade` bars have multiple revisions per bar (live updates). `fetch_new_bars_real_trade` fetches all revisions that arrive before bar close (`revision_ts < ts + tf_minutes`), computing `execution_ts = toStartOfMinute(revision_ts + 59s)` per revision. All revisions are returned ordered by `ts, revision_ts`. `compute_real_trade_pnl` iterates by `execution_ts` and advances `active_position` as each revision is reached. Prod/bt use `argMin(row_json, revision_ts)` (first revision wins) and a simpler `compute_prod_pnl`.

`_refresh_pnl_real_trade` in `pnl_strategy_v2.py` is fully implemented for both live and daily paths, using `fetch_new_bars_real_trade` and `compute_real_trade_pnl` from `pnl_compute.py`.

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

GitHub Actions drives all CI/CD. Workflows live in `.github/workflows/`.

| Workflow | Trigger | Stages |
|----------|---------|--------|
| `ci-cd.yml` | Push to `main` | test → build + terraform (parallel) → deploy-dagster + deploy-grafana-cloud (parallel, terraform must pass first) |

Each service rebuilds only if its paths changed (via `dorny/paths-filter`). `workflow_dispatch` forces rebuild of all services regardless. Path filters:
- `dagster`: `trading_dagster/**`, `Dockerfile`, `pyproject.toml`
- `streaming`: `streaming/**`
- `pnl-consumer`: `pnl_consumer/**`
- `terraform`: `infra/terraform/**`
- `grafana`: `infra/grafana/**`

AWS auth uses GitHub OIDC — no static credentials stored in GitHub. Each job that needs AWS access declares `permissions: id-token: write` at the job level and assumes the `trading-analysis-github-actions` IAM role via `aws-actions/configure-aws-credentials@v4`.

### Grafana Cloud Deployment

Dashboard JSON files in `infra/grafana/dashboards/` are automatically pushed to Grafana Cloud on every push to `main` via the `deploy-grafana-cloud` job. It uses:
- `GRAFANA_CLOUD_TOKEN` — service account token (GitHub secret)
- `GRAFANA_CLOUD_URL` — Grafana Cloud stack URL, e.g. `https://yourstack.grafana.net` (GitHub secret)

The job POSTs each `*.json` file to `$GRAFANA_CLOUD_URL/api/dashboards/db` with `"overwrite": true`. To add or update a dashboard, commit the JSON to `infra/grafana/dashboards/` and push to `main`.

### IAM for Grafana CloudWatch Datasource

The IAM user `trading-analysis-grafana-cloudwatch` (with `cloudwatch:GetMetricStatistics`, `cloudwatch:ListMetrics`, `cloudwatch:GetMetricData` permissions, scoped to `AWS/Billing` namespace) is managed by Terraform. Credentials are stored in Secrets Manager at `trading-analysis/grafana-cloudwatch` (region: `ap-northeast-1`). After `terraform apply`, retrieve them:

```bash
aws secretsmanager get-secret-value \
  --secret-id trading-analysis/grafana-cloudwatch \
  --region ap-northeast-1 \
  --query SecretString --output text
```

Configure in Grafana Cloud → Connections → CloudWatch datasource (default region: `us-east-1`, where AWS billing metrics are published).

### One-Time AWS Setup

Run these once when setting up a new environment. Requires AWS CLI and `gh` CLI.

**Step 1: Create GitHub OIDC Identity Provider**

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1 \
  --region ap-northeast-1
```

**Step 2: Create IAM role `trading-analysis-github-actions`**

```bash
cat > /tmp/gha-trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::068704208855:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:williamwxz/trading-analysis:*"
        }
      }
    }
  ]
}
EOF

aws iam create-role \
  --role-name trading-analysis-github-actions \
  --assume-role-policy-document file:///tmp/gha-trust-policy.json \
  --region ap-northeast-1

cat > /tmp/gha-permissions.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ECR",
      "Effect": "Allow",
      "Action": ["ecr:GetAuthorizationToken"],
      "Resource": "*"
    },
    {
      "Sid": "ECRRepos",
      "Effect": "Allow",
      "Action": ["ecr:*"],
      "Resource": [
        "arn:aws:ecr:ap-northeast-1:068704208855:repository/trading-analysis-dagster"
      ]
    },
    {
      "Sid": "ECS",
      "Effect": "Allow",
      "Action": [
        "ecs:DescribeTaskDefinition",
        "ecs:RegisterTaskDefinition",
        "ecs:UpdateService",
        "ecs:DescribeServices"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SecretsManager",
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": [
        "arn:aws:secretsmanager:ap-northeast-1:068704208855:secret:trading-analysis/clickhouse*"
      ]
    },
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Sid": "TerraformState",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketVersioning"],
      "Resource": [
        "arn:aws:s3:::trading-analysis-tfstate",
        "arn:aws:s3:::trading-analysis-tfstate/*"
      ]
    },
    {
      "Sid": "TerraformProvision",
      "Effect": "Allow",
      "Action": [
        "ec2:*", "rds:*", "elasticloadbalancing:*",
        "iam:*", "secretsmanager:*", "ecs:*",
        "s3:*", "ecr:*", "logs:*", "elasticfilesystem:*"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name trading-analysis-github-actions \
  --policy-name github-actions-policy \
  --policy-document file:///tmp/gha-permissions.json
```

**Step 3: Add GitHub repository secrets**

```bash
gh secret set AWS_ACCOUNT_ID --body "068704208855" --repo williamwxz/trading-analysis
gh secret set AWS_REGION --body "ap-northeast-1" --repo williamwxz/trading-analysis
gh secret set GHA_ROLE_ARN --body "arn:aws:iam::068704208855:role/trading-analysis-github-actions" --repo williamwxz/trading-analysis
```

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

ECR repo: `trading-analysis-dagster`  
S3 buckets: `trading-analysis-data-v2`, `trading-analysis-pipeline-v2`
