# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Mini trading analytics pipeline. Streams strategy PnL data: Binance API → streaming producer (Kafka/Redpanda) → pnl_consumer → ClickHouse Cloud analytics → Grafana Cloud dashboards. Batch recompute/repair is handled by `scripts/audit_pnl.py`; market-data gap-fill is a daily `backfill_prices` Lambda. Services run on AWS ECS Fargate in `ap-northeast-1` (Tokyo). Grafana is Grafana Cloud (not self-hosted).

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
pytest -m unit                    # fast, no external deps
pytest -m integration             # requires live ClickHouse/S3
pytest -m streaming_integration   # requires Docker + Binance network

# Local stack (brings up Grafana only — Grafana is Grafana Cloud in prod)
docker compose up -d

# Batch PnL recompute / repair of a window (prod | real_trade | bt)
python scripts/audit_pnl.py --type prod --fix-window "START" "END"
# Plain audit (no --fix): coverage/position/hour-sync checks
python scripts/audit_pnl.py --type prod
```

## Architecture

### Services

Independent ECS Fargate services, each with its own Dockerfile:

| Service | Directory | Role |
|---------|-----------|------|
| `trading-analysis-ws-consumer` | `services/streaming/` | Binance WebSocket → Kafka/Redpanda topic `binance.price.ticks` |
| `trading-analysis-pnl-consumer-{prod,bt,real-trade,price}` | `services/pnl_consumer/` | Kafka consumer → real-time PnL → ClickHouse (one ECS service per mode; the `price` sink writes `futures_price_1min` from `candle.open`) |

A `services/flink_pnl/` service also exists in-tree (shares `libs/computation/`).

Batch PnL recompute/repair is the standalone script `scripts/audit_pnl.py` (not a service). Market-data historical gap-fill (`futures_price_1min`) is `services/backfill_prices/` — a daily AWS Lambda using ccxt.

Adding new instruments requires updating `INSTRUMENTS` in `services/streaming/streaming/binance_ws_consumer.py` and in the `backfill_prices` Lambda.

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

**Batch / repair path** (no orchestrator):
```
backfill_prices Lambda (daily, ccxt)  — fills futures_price_1min historical gaps only
    → analytics.futures_price_1min

External strategy service (push)
    → analytics.strategy_output_history_v2 / _bt_v2

scripts/audit_pnl.py --fix-window "START" "END"  (idempotent delete + recompute, prod | bt | real_trade)
    → analytics.strategy_pnl_1min_{prod,bt,real_trade}_v2
    → rebuilds analytics.strategy_pnl_1hour/1day_{prod,bt,real_trade}_v2 for the repaired window

1hour / 1day rollups (ClickHouse Materialized Views, fired on every 1min INSERT — no asset)
    → analytics.strategy_pnl_1hour_{prod,bt,real_trade}_v2
    → analytics.strategy_pnl_1day_{prod,bt,real_trade}_v2
```

The live `pnl_consumer` services write the 1min PnL tables continuously; `audit_pnl.py` is only for periodic verification and window repair. Real-time prices come from the pnl_consumer `price` sink (`candle.open` from Redpanda), not from ClickHouse.

### Batch Recompute / Repair (`scripts/audit_pnl.py`)

`scripts/audit_pnl.py` is the standalone batch tool for recomputing/repairing a PnL window (formerly the Dagster full-recompute assets). It pauses the relevant `pnl-consumer` ECS service for the mode being repaired, deletes and recomputes the window, rebuilds the 1hour/1day rollups, then resumes the service.

- `--type {prod|real_trade|bt}` selects the mode.
- `--fix-window "START" "END"` does an idempotent delete + recompute of `[START, END)`.
- Without `--fix`, it runs read-only audit checks (coverage / position / hour-sync).

### PnL Recompute Algorithm (`--fix-window`)

For a window `[start, end)` it recomputes PnL the same way the live consumer does:

1. **Fetch anchors**: `fetch_anchors()` reads the last committed row per strategy from the target table (previous day's tail)
2. **Delete window**: idempotent DELETE WHERE ts in [start, end) AND source=label
3. **Fetch bars**: `fetch_new_bars_*()` queries `strategy_output_history_v2` for bars in the window
4. **Fetch prices**: `fetch_prices_multi()` reads `analytics.futures_price_1min` — prices ALWAYS come from here, never from `row_json`
5. **Compute PnL**: `compute_*_pnl()` expands each bar into 1-min rows, chaining anchors forward
6. **Insert rows**: `insert_rows()` writes to the target table; the 1hour/1day rollups for the window are then rebuilt

### Why Python for PnL (not SQL)

V1 used a SQL anchor CTE which fails when multiple bars arrive in a single INSERT batch — SQL can't chain bar N's end-price into bar N+1's start-price within the same query. V2 uses Python to iterate bars in order, explicitly passing `anchor_pnl` and `anchor_price` forward.

PnL formula: `cumulative_pnl = anchor_pnl + position * (live_price - anchor_price) / anchor_price`

### Shared Computation Library (`libs/computation/`)

All PnL logic lives here — `pnl_consumer` (streaming), `flink_pnl`, and `scripts/audit_pnl.py` (batch) all import from this library. No service contains computation code directly.

| Module | Purpose |
|--------|---------|
| `anchor_state.py` | `AnchorRecord` + `AnchorState`: per-strategy running (pnl, price, position, bar_ts, revision_ts). `compute_pnl()` advances the chain; `should_apply_revision()` is the real_trade revision guard |
| `pnl_formula.py` | `INSERT_COLUMNS` (16 cols, ts=7, updated_at=14, strategy_instance_id=15), `TIMEFRAME_MAP`, `compute_prod_pnl`, `compute_bt_pnl`, `compute_real_trade_pnl`, `iter_compute_prod_pnl`, `extract_row_anchor` |
| `fetch_bars.py` | Batch recompute: `fetch_anchors`, `fetch_new_bars_prod`, `fetch_new_bars_bt`, `fetch_new_bars_real_trade` |
| `fetch_prices.py` | Batch recompute: `fetch_prices_multi` — reads `analytics.futures_price_1min` for multiple underlyings |
| `candle_lookup.py` | pnl_consumer live loop: `fetch_strategies_for_candle`, `fetch_bt_strategies_for_candle`, `fetch_real_trade_for_candle` — re-queried every candle |
| `bootstrap.py` | pnl_consumer cold-start: `fetch_bootstrap_seeds`, `fetch_walk_rows` |

`libs/clickhouse_client.py` is the standalone ClickHouse client used by all `libs/computation/` modules and `scripts/audit_pnl.py`.

### PnL Calculation Per Mode

All three modes share the same formula and anchor-chaining loop. Differences are in bar sourcing, execution timestamp, and cold-start seeding. Code lives in `libs/computation/pnl_formula.py`.

**Prod** (`source_label="production"`, `compute_prod_pnl` / `iter_compute_prod_pnl`):
- Source: `strategy_output_history_v2`, `argMin(row_json, revision_ts)` — only the *first* revision per bar is used.
- Execution starts at `closing_ts = ts + tf_minutes` (bar close). Position holds from closing_ts until next bar's closing_ts.
- Anchor seed: always from previous day's tail in `strategy_pnl_1min_prod_v2` via `fetch_anchors()`. Cold-start (zero anchor) only on a strategy's very first bar.
- Output columns: `INSERT_COLUMNS` (16 cols, ts at index 7, updated_at at index 14, strategy_instance_id at index 15).

**Backtest** (`source_label="backtest"`, `compute_bt_pnl`):
- Source: `strategy_output_history_bt_v2`, same `argMin` first-revision logic. Also extracts `cumulative_pnl` from `row_json`.
- Execution starts at `execution_ts = ts + tf_minutes` (same as prod closing_ts). Position holds until next bar's execution_ts.
- Anchor seed priority: (1) previous day's tail from `strategy_pnl_1min_bt_v2`; (2) `bar["cumulative_pnl"]` from source JSON **only** as cold-start when no prior anchor exists — never used once rows exist in the target table.
- Output columns: same `INSERT_COLUMNS` (16 cols).

**Real-trade** (`source_label="real_trade"`, `compute_real_trade_pnl`):
- Source: `strategy_output_history_v2`, ALL revisions per bar fetched and filtered.
- Revision acceptance rule: a revision is accepted if `revision_ts < next_bar_closing_ts`. When no next bar exists, the SQL sentinel `next_bar_closing_ts == closing_ts` always accepts. Discarded revisions are skipped entirely — the previous accepted position continues holding.
- Execution starts at `execution_ts = toStartOfMinute(revision_ts + 59s)` — the minute the position *actually* took effect (not bar close). `closing_ts` (= `ts + tf_minutes`) is stored separately on each output row.
- Accepted revisions expand 1-min from their `execution_ts` until the next accepted revision's `execution_ts`. The last accepted revision holds for `tf_minutes` past its `closing_ts`.
- Anchor seed: always from previous day's tail in `strategy_pnl_1min_real_trade_v2`. No cold-start seeding from source JSON.
- Output columns: same `INSERT_COLUMNS` (16 cols) — `closing_ts` and `execution_ts` are inputs to computation, not stored as separate output columns.

**Price fallback** (all modes): if `prices[ts_str]` is missing and an anchor price exists, the last known anchor price is reused. If no anchor price exists yet, the minute is skipped until the first price arrives.

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

### Streaming PnL Consumer Logic (pnl_consumer/)

Full detail in `docs/pnl_consumer_logic.md`. Shared query library in `libs/computation/`. Key points:

**Cold-start bootstrap (all three modes):**
1. `peek_reference_ts()` reads the Kafka committed offset → `reference_ts`
2. `start_ts = reference_ts - 3 days`
3. **Seed anchor**: per `strategy_instance_id`, load `(cumulative_pnl)` from the pnl table at latest minute `< start_ts`, `price` from `futures_price_1min` at that same minute, `position` from `strategy_output_history_*`
4. **Walk `[start_ts, reference_ts)`**: replay stored rows, recomputing PnL to verify consistency; crash if deviation `> 0.2%`; seed `AnchorState` from final walked values

**Live loop — prod and bt:**
- Re-query `strategy_output_history_v2` / `_bt_v2` every candle; latest bar whose `closing_ts <= candle_ts`, first revision only (`argMin`)
- Dedup: `(strategy_instance_id, bar_ts)` seen-set populated during walk, persists into live loop
- Lazy-seed: brand-new strategies seeded on first appearance

**Live loop — real_trade:**
- Re-query `strategy_output_history_v2` every candle; latest revision whose `revision_ts <= candle_ts` per `strategy_instance_id` (no `closing_ts` gate)
- **AnchorState revision guard**: apply revision only if `(bar_ts, revision_ts) > (anchor.bar_ts, anchor.revision_ts)` — prevents stale late revisions for an old bar overwriting a newer bar's active position
- `bar_ts` = `strategy_output_history_v2.ts` (bar open time, not closing_ts or revision_ts)
- Bootstrap walk resolves position as "latest revision with `revision_ts <= row.ts`" (same guard logic)

**Critical data-source rules:**
- `position` always from `strategy_output_history_*` — never from the PnL table
- `price` always from `futures_price_1min` (bootstrap/walk) or Redpanda candle `open` (live) — never from the PnL table's `price` column
- All bars arrive late — queries use `ts <= cutoff`, not `revision_ts <= cutoff`

### ClickHouse Patterns

- **All queries through** `libs/clickhouse_client.py`: `get_client()`, `query_rows()`, `query_dicts()`, `query_scalar()`, `execute()`, `insert_rows()`
- **No connection pooling**: each call creates a new connection unless a `client=` is passed explicitly. Pass a shared client when doing multiple operations in one run to avoid redundant connections.
- **ReplacingMergeTree(updated_at)**: re-inserting rows replaces older ones after background merge. Use `FINAL` in SELECT to get deduplicated results immediately, but avoid `FINAL` on large frequently-queried tables (it forces in-memory merge). Prefer `LIMIT 1 BY key ORDER BY key, updated_at DESC` to read the latest row per key using the sort index instead.
- **Idempotent upserts**: `audit_pnl.py --fix-window` DELETEs the window+instrument before inserting; live inserts rely on ReplacingMergeTree.
- **Batch insert**: `insert_rows()` defaults to 200k rows per batch.
- **Datetime types**: ClickHouse-connect requires Python `datetime` objects for DateTime columns. `_prepare_rows_for_clickhouse()` in `pnl_strategy_v2.py` handles string→datetime conversion for PnL rows (ts at index 7, updated_at at index 14 of PROD_INSERT_COLUMNS).

### ClickHouse Cloud Connection

Required env vars: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` (8443), `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_SECURE` (true).

Schema lives in `schemas/clickhouse_cloud.sql` (apply once). No `Replicated*` prefix needed — Cloud handles replication.

## CI/CD

GitHub Actions drives all CI/CD. Workflows live in `.github/workflows/`.

| Workflow | Trigger | Stages |
|----------|---------|--------|
| `ci-cd.yml` | Push to `main` | test → build + terraform (parallel) → deploy + deploy-grafana-cloud (parallel, terraform must pass first) |

Each service rebuilds only if its paths changed (via `dorny/paths-filter`). `workflow_dispatch` forces rebuild of all services regardless. Path filters:
- `streaming`: `services/streaming/**`
- `pnl-consumer`: `services/pnl_consumer/**`
- `backfill-prices`: `services/backfill_prices/**`
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
        "arn:aws:ecr:ap-northeast-1:068704208855:repository/trading-analysis-*"
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

S3 buckets: `trading-analysis-data-v2`, `trading-analysis-pipeline-v2`
