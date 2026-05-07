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
- `clickhouse_connectivity_check` — infra health check
- `postgres_cleanup` — Dagster metadata DB cleanup

**Not registered** (defined in `pnl_strategy_v2.py` but inactive): `pnl_bt_v2_live_asset`, `pnl_prod_v2_live_asset`, `pnl_real_trade_v2_live_asset` — live paths not currently active.

### Dual Asset Strategy

Every data source has two complementary assets:
- **Partitioned (backfill)**: `DailyPartitionsDefinition`, idempotent DELETE+INSERT, triggered manually or via backfill UI
- **Unpartitioned (full recompute)**: `pnl_*_v2_full` assets, process all underlyings from start date to now in `_CHUNK_DAYS=7` day chunks

Example: `binance_futures_backfill` (daily partitions from 2024-01-01).

### PnL Refresh Pattern (Daily Partitioned Assets)

1. **Delete partition**: idempotent DELETE WHERE ts in [start, end) AND source=label
2. **Fetch anchors**: `fetch_anchors()` reads the last committed row per strategy from the target table (previous day's tail)
3. **Fetch bars**: `fetch_new_bars_*()` queries `strategy_output_history_v2` for bars in the partition window
4. **Fetch prices**: `fetch_prices_multi()` reads `analytics.futures_price_1min` — prices ALWAYS come from here, never from `row_json`
5. **Compute PnL**: `compute_*_pnl()` expands each bar into 1-min rows, chaining anchors forward
6. **Insert rows**: `insert_rows()` writes to the target table

`_CHUNK_DAYS = 7` in `pnl_strategy_v2.py` controls how many days the full-recompute path processes per chunk. The daily partitioned path processes exactly one day per run.

### Why Python for PnL (not SQL)

V1 used a SQL anchor CTE which fails when multiple bars arrive in a single INSERT batch — SQL can't chain bar N's end-price into bar N+1's start-price within the same query. V2 uses Python to iterate bars in order, explicitly passing `anchor_pnl` and `anchor_price` forward.

PnL formula: `cumulative_pnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price`

**Critical invariants:**
- `price` in all PnL output tables is always the 1-min open from `analytics.futures_price_1min` — never the `price` field from `row_json`/`strategy_output_history_*`
- `cumulative_pnl` is always recomputed from scratch using our own anchor chain — the `cumulative_pnl` field in `row_json` is only used as a cold-start seed for BT (where we have no prior anchor), never for prod or real_trade
- `traded` column in `strategy_pnl_1min_real_trade_v2` is always `False` — it was intended for future use and is never set; do not use it for any logic or reporting
- `pnl_refresh_watermarks` table does not exist — the live refresh path is not active; no watermark table or watermark logic should be referenced

### 1-Min Expansion Window

Each bar's position is held from its `closing_ts` (= `ts + tf_minutes`) until the next bar's `closing_ts`. The last bar in a partition holds for one extra `tf_minutes` past its own `closing_ts` — this covers the gap until the next partition's first bar arrives. For daily bars (`1d`, `tf_minutes=1440`) this means rows extend into the next calendar day, which is correct and expected. Do not confuse future-dated `ts` values from `1d`-bar expansion with data errors.

### Real Trade vs Prod/Bt

`real_trade` bars have multiple revisions per bar (position updates arriving after bar open). `fetch_new_bars_real_trade` fetches all revisions for bars in the window, computing:
- `execution_ts = toStartOfMinute(revision_ts + 59s)` — the minute when the new position becomes active
- `closing_ts = ts + tf_minutes` — the bar's own close time
- `next_bar_closing_ts` — the close time of the following bar (via SQL `leadInFrame`); used to filter stale revisions

`compute_real_trade_pnl` acceptance rule: a revision is accepted if `revision_ts < next_bar_closing_ts`. When no next bar exists, the sentinel `next_bar_closing_ts == closing_ts` always accepts. Accepted revisions are expanded 1-min from their `execution_ts` until the next accepted revision's `execution_ts`. The last accepted revision holds for `tf_minutes` past its own `closing_ts`.

`execution_ts` in `strategy_pnl_1min_real_trade_v2` is `toStartOfMinute(revision_ts + 59s)` — the minute the position took effect. It is NOT the bar's closing time. `closing_ts` is the bar closing time.

Prod/bt use `argMin(row_json, revision_ts)` (first revision only) and `iter_compute_prod_pnl` / `compute_bt_pnl`. BT additionally extracts `cumulative_pnl` from `row_json` as a cold-start seed when no prior anchor exists in the target table.

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
