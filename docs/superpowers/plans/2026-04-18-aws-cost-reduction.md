# AWS Cost Reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce AWS monthly spend from ~$365/mo to ~$70/mo by removing the NAT Gateway, right-sizing the Dagster ECS task, replacing RDS Postgres with SQLite on EFS, and adding backfill skip logic for already-complete Binance partition days.

**Architecture:** ECS Fargate tasks move to public subnets with ephemeral public IPs (no NAT needed). Dagster metadata moves from RDS Postgres to SQLite on an EFS volume mounted at `/app`. The Dagster task shrinks from 8 vCPU/16 GB to 1 vCPU/2 GB. Backfill asset gains a pre-check that skips instruments with a full day of data already present.

**Tech Stack:** Terraform (AWS provider ~5.0), Python 3.11, Dagster, EFS, SQLite, pytest

---

## File Map

| File | Change |
|---|---|
| `infra/terraform/main.tf` | Remove NAT GW, EIP, private subnets, private route table, RDS, DB SG, DB subnet group, Secrets Manager PG secret. Add EFS file system, mount targets, access point, EFS SG. Update ECS task (cpu/memory, volumes, env vars). Update ECS services (public subnets, assign_public_ip). |
| `dagster.yaml` | Replace `storage.postgres` block with `storage.sqlite` block. Update `local_artifact_storage.base_dir`. |
| `trading_dagster/assets/binance_futures_ohlcv.py` | Add per-instrument row-count check before delete+fetch in `binance_futures_backfill_asset`. |
| `tests/test_binance_backfill_skip.py` | New file: unit tests for the skip logic. |

---

## Task 1: Add backfill skip logic + tests

This is pure Python with no external dependencies — do it first so it can be verified independently.

**Files:**
- Modify: `trading_dagster/assets/binance_futures_ohlcv.py`
- Create: `tests/test_binance_backfill_skip.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_binance_backfill_skip.py`:

```python
"""Unit tests for binance_futures_backfill_asset skip logic."""
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone, timedelta


def _make_context(partition_key: str):
    ctx = MagicMock()
    ctx.partition_key = partition_key
    ctx.log = MagicMock()
    return ctx


@pytest.mark.unit
def test_skips_instrument_when_full_day_present():
    """If query_scalar returns >= 1440 for an instrument, it is skipped (no DELETE, no fetch)."""
    from trading_dagster.assets.binance_futures_ohlcv import binance_futures_backfill_asset

    ctx = _make_context("2024-03-01")

    with patch("trading_dagster.assets.binance_futures_ohlcv.get_client") as mock_get_client, \
         patch("trading_dagster.assets.binance_futures_ohlcv.query_scalar", return_value=1440) as mock_qs, \
         patch("trading_dagster.assets.binance_futures_ohlcv.execute_query") as mock_exec, \
         patch("trading_dagster.assets.binance_futures_ohlcv.insert_rows") as mock_insert, \
         patch("trading_dagster.assets.binance_futures_ohlcv.ExchangePriceDataService") as mock_svc:

        result = binance_futures_backfill_asset(ctx)

    # query_scalar called once per instrument (10 instruments)
    assert mock_qs.call_count == 10
    # No DELETE and no Binance fetch when all instruments are full
    mock_exec.assert_not_called()
    mock_svc.fetch_ohlcv_times_series_df.assert_not_called()
    mock_insert.assert_not_called()


@pytest.mark.unit
def test_refetches_instrument_when_partial_day():
    """If query_scalar returns < 1440, the instrument is deleted and re-fetched."""
    import pandas as pd
    from trading_dagster.assets.binance_futures_ohlcv import binance_futures_backfill_asset, INSTRUMENTS

    ctx = _make_context("2024-03-01")

    # Only first instrument is partial; rest are full
    def scalar_side_effect(query, client):
        if INSTRUMENTS[0] in query:
            return 500  # partial
        return 1440  # full

    fake_df = pd.DataFrame({
        "timestamp": [datetime(2024, 3, 1, 0, i, tzinfo=timezone.utc) for i in range(10)],
        "open": [1.0] * 10,
        "high": [1.0] * 10,
        "low": [1.0] * 10,
        "close": [1.0] * 10,
        "volume": [1.0] * 10,
    })

    with patch("trading_dagster.assets.binance_futures_ohlcv.get_client"), \
         patch("trading_dagster.assets.binance_futures_ohlcv.query_scalar", side_effect=scalar_side_effect), \
         patch("trading_dagster.assets.binance_futures_ohlcv.execute_query") as mock_exec, \
         patch("trading_dagster.assets.binance_futures_ohlcv.insert_rows") as mock_insert, \
         patch("trading_dagster.assets.binance_futures_ohlcv.ExchangePriceDataService") as mock_svc:

        # Return data for first instrument, then empty to stop the loop
        mock_svc.fetch_ohlcv_times_series_df.side_effect = [fake_df, None]
        result = binance_futures_backfill_asset(ctx)

    # DELETE called exactly once (only for partial instrument)
    assert mock_exec.call_count == 1
    assert INSTRUMENTS[0] in mock_exec.call_args[0][0]
    # insert_rows called once for the partial instrument
    assert mock_insert.call_count == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_binance_backfill_skip.py -v -m unit
```

Expected: `FAILED` — `binance_futures_backfill_asset` has no skip logic yet.

- [ ] **Step 3: Add skip logic to backfill asset**

In `trading_dagster/assets/binance_futures_ohlcv.py`, replace the `for instrument in INSTRUMENTS:` loop body inside `binance_futures_backfill_asset` (starting at line 77). The new loop body:

```python
    for instrument in INSTRUMENTS:
        start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_dt_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        # Skip if full day already present
        existing = query_scalar(
            f"SELECT count() FROM {TARGET_TABLE} "
            f"WHERE exchange='binance' AND instrument='{instrument}' "
            f"AND ts >= '{start_dt_str}' AND ts < '{end_dt_str}'",
            client
        )
        if existing >= 1440:
            context.log.info(f"[{instrument}] Full day already present ({existing} rows), skipping.")
            continue

        # Idempotency: clear existing partial data for this day/instrument
        execute_query(
            f"DELETE FROM {TARGET_TABLE} WHERE exchange='binance' AND instrument='{instrument}' "
            f"AND ts >= '{start_dt_str}' AND ts < '{end_dt_str}'",
            client
        )

        current_start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        instrument_inserted = 0
        while True:
            df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
                symbol=_get_ccxt_symbol(instrument), exchange_name="binance_perp",
                timeframe="1m", date_since=current_start_iso, limit=1000
            )
            if df is None or df.empty: break

            # Ensure timestamp is datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            # Filter to partition window
            df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] < end_dt)]
            if df.empty: break

            insert_rows(TARGET_TABLE, INSERT_COLUMNS, _df_to_rows(instrument, df), client)
            total_inserted += len(df)
            instrument_inserted += len(df)

            if len(df) < 1000 or df.iloc[-1]['timestamp'] >= (end_dt - timedelta(minutes=1)): break
            current_start_iso = (df.iloc[-1]['timestamp'] + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            time.sleep(0.1)

        context.log.info(f"[{instrument}] Finished partition {partition_date_str}, inserted {instrument_inserted} rows.")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_binance_backfill_skip.py -v -m unit
```

Expected: both tests `PASSED`.

- [ ] **Step 5: Run full unit test suite to check for regressions**

```bash
pytest tests/ -v -m "not integration"
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add trading_dagster/assets/binance_futures_ohlcv.py tests/test_binance_backfill_skip.py
git commit -m "feat: skip binance backfill when full day already present"
```

---

## Task 2: Update dagster.yaml — switch to SQLite storage

**Files:**
- Modify: `dagster.yaml`

- [ ] **Step 1: Replace the storage block**

Replace the entire `storage:` section in `dagster.yaml`. The new file:

```yaml
# Dagster instance configuration — trading-analysis
# Runs on ECS Fargate with ClickHouse Cloud as analytics DB.
# Storage: SQLite on EFS mounted at /app

storage:
  sqlite:
    base_dir: /app/.dagster_storage

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

compute_logs:
  module: dagster.core.storage.noop_compute_log_manager
  class: NoOpComputeLogManager

local_artifact_storage:
  module: dagster.core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: /app/.dagster_storage/storage

run_queue:
  max_concurrent_runs: 25

concurrency:
  default_op_concurrency_limit: 5

retention:
  schedule:
    purge_after_days:
      skipped: 30
  sensor:
    purge_after_days:
      skipped: 30

python_logs:
  managed_python_loggers:
    - trading_dagster
```

- [ ] **Step 2: Verify Dagster still loads with SQLite config**

```bash
source .venv/bin/activate
python -c "from dagster import DagsterInstance; print('OK')"
```

Expected: prints `OK` with no errors.

- [ ] **Step 3: Commit**

```bash
git add dagster.yaml
git commit -m "chore: switch dagster storage from postgres to sqlite on efs"
```

---

## Task 3: Update Terraform — remove NAT Gateway and private subnets

**Files:**
- Modify: `infra/terraform/main.tf`

- [ ] **Step 1: Remove NAT Gateway, Elastic IP, and private networking resources**

In `infra/terraform/main.tf`, delete the following blocks entirely:

```hcl
# DELETE these 5 blocks:

resource "aws_eip" "nat" { ... }

resource "aws_nat_gateway" "main" { ... }

resource "aws_subnet" "private" { ... }

resource "aws_route_table" "private" { ... }

resource "aws_route_table_association" "private" { ... }
```

Also delete the `output "nat_static_ip"` block at the bottom.

- [ ] **Step 2: Update ECS services to use public subnets**

In `aws_ecs_service.dagster`, change `network_configuration`:

```hcl
  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
```

In `aws_ecs_service.grafana`, change `network_configuration`:

```hcl
  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
```

- [ ] **Step 3: Validate Terraform**

```bash
cd infra/terraform
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "infra: remove NAT gateway and move ECS tasks to public subnets"
```

---

## Task 4: Update Terraform — remove RDS and add EFS

**Files:**
- Modify: `infra/terraform/main.tf`

- [ ] **Step 1: Remove RDS-related resources**

Delete the following blocks from `infra/terraform/main.tf`:

```hcl
# DELETE these blocks:

resource "random_password" "db_password" { ... }

resource "aws_db_instance" "dagster" { ... }

resource "aws_db_subnet_group" "main" { ... }

resource "aws_security_group" "db" { ... }

resource "aws_secretsmanager_secret" "dagster_pg" { ... }

resource "aws_secretsmanager_secret_version" "dagster_pg" { ... }
```

- [ ] **Step 2: Add EFS file system, security group, mount targets, and access point**

Add these new resource blocks to `infra/terraform/main.tf`, after the S3 section and before the ECS section:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# EFS — SQLite storage for Dagster metadata
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_efs_file_system" "dagster" {
  encrypted = true

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dagster-efs" })
}

resource "aws_security_group" "efs" {
  name_prefix = "${local.name_prefix}-efs-"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_efs_mount_target" "dagster" {
  count           = 2
  file_system_id  = aws_efs_file_system.dagster.id
  subnet_id       = aws_subnet.public[count.index].id
  security_groups = [aws_security_group.efs.id]
}

resource "aws_efs_access_point" "dagster" {
  file_system_id = aws_efs_file_system.dagster.id

  posix_user {
    uid = 1000
    gid = 1000
  }

  root_directory {
    path = "/dagster-home"
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "755"
    }
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-dagster-ap" })
}
```

- [ ] **Step 3: Add EFS IAM permissions to ECS task role**

In `aws_iam_role_policy.ecs_task_policy`, add a new statement to the existing `Statement` array:

```hcl
      {
        Effect = "Allow"
        Action = [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:ClientRootAccess",
        ]
        Resource = aws_efs_file_system.dagster.arn
      },
```

- [ ] **Step 4: Validate Terraform**

```bash
cd infra/terraform
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "infra: replace RDS with EFS for dagster sqlite storage"
```

---

## Task 5: Update Terraform — right-size Dagster task + wire EFS volume

**Files:**
- Modify: `infra/terraform/main.tf`

- [ ] **Step 1: Right-size cpu and memory**

In `aws_ecs_task_definition.dagster`, change:

```hcl
  cpu    = 1024
  memory = 2048
```

- [ ] **Step 2: Add EFS volume to task definition**

In `aws_ecs_task_definition.dagster`, add a `volume` block (at the same level as `container_definitions`):

```hcl
  volume {
    name = "dagster-efs"

    efs_volume_configuration {
      file_system_id          = aws_efs_file_system.dagster.id
      transit_encryption      = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.dagster.id
        iam             = "ENABLED"
      }
    }
  }
```

- [ ] **Step 3: Add mountPoints and remove Postgres env vars from container definitions**

In the `dagster-webserver` container definition, add `mountPoints` and remove `DAGSTER_PG_*`:

```hcl
    {
      name      = "dagster-webserver"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "trading_dagster"]

      portMappings = [
        {
          containerPort = 3000
          protocol      = "tcp"
        }
      ]

      mountPoints = [
        {
          sourceVolume  = "dagster-efs"
          containerPath = "/app"
          readOnly      = false
        }
      ]

      environment = [
        { name = "DAGSTER_HOME",      value = "/app" },
        { name = "CLICKHOUSE_USER",   value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT",   value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.dagster.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "webserver"
        }
      }
    },
```

In the `dagster-daemon` container definition, add `mountPoints` and remove `DAGSTER_PG_*`:

```hcl
    {
      name      = "dagster-daemon"
      image     = "${aws_ecr_repository.dagster.repository_url}:latest"
      essential = true
      command   = ["dagster-daemon", "run", "-m", "trading_dagster"]

      mountPoints = [
        {
          sourceVolume  = "dagster-efs"
          containerPath = "/app"
          readOnly      = false
        }
      ]

      environment = [
        { name = "DAGSTER_HOME",      value = "/app" },
        { name = "CLICKHOUSE_USER",   value = "dev_ro3" },
        { name = "CLICKHOUSE_PORT",   value = "8443" },
        { name = "CLICKHOUSE_SECURE", value = "true" },
      ]

      secrets = [
        { name = "CLICKHOUSE_HOST",     valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:host::" },
        { name = "CLICKHOUSE_PASSWORD", valueFrom = "${aws_secretsmanager_secret.clickhouse.arn}:password::" },
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.dagster.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "daemon"
        }
      }
    }
```

- [ ] **Step 4: Narrow secrets IAM policy to remove dagster_pg reference**

In `aws_iam_role_policy.ecs_execution_secrets`, update the `Resource` list to only include the ClickHouse secret:

```hcl
        Resource = [
          aws_secretsmanager_secret.clickhouse.arn,
        ]
```

- [ ] **Step 5: Validate Terraform**

```bash
cd infra/terraform
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Run full unit test suite**

```bash
cd /path/to/repo && source .venv/bin/activate
pytest tests/ -v -m "not integration"
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "infra: right-size dagster task (1 vCPU/2 GB) and mount EFS volume"
```

---

## Task 6: Apply Terraform and verify deployment

This task requires AWS credentials and a live environment. Run from your local machine or via the manual `terraform.yml` workflow.

**Files:** None (operational steps only)

- [ ] **Step 1: Run terraform plan to review all changes**

```bash
cd infra/terraform
terraform plan -out=tfplan
```

Review the plan output. Expected removals:
- `aws_eip.nat`
- `aws_nat_gateway.main`
- `aws_subnet.private[0]`, `aws_subnet.private[1]`
- `aws_route_table.private`
- `aws_route_table_association.private[0]`, `[1]`
- `aws_db_instance.dagster`
- `aws_db_subnet_group.main`
- `aws_security_group.db`
- `aws_secretsmanager_secret.dagster_pg`
- `aws_secretsmanager_secret_version.dagster_pg`
- `random_password.db_password`

Expected additions:
- `aws_efs_file_system.dagster`
- `aws_security_group.efs`
- `aws_efs_mount_target.dagster[0]`, `[1]`
- `aws_efs_access_point.dagster`

Expected modifications:
- `aws_ecs_task_definition.dagster` (cpu, memory, volumes, env vars)
- `aws_ecs_service.dagster` (subnets, assign_public_ip)
- `aws_ecs_service.grafana` (subnets, assign_public_ip)
- `aws_iam_role_policy.ecs_execution_secrets` (resource list)
- `aws_iam_role_policy.ecs_task_policy` (added EFS actions)

- [ ] **Step 2: Apply Terraform**

```bash
terraform apply tfplan
```

Wait for apply to complete (~3-5 min for EFS + ECS updates).

- [ ] **Step 3: Verify ECS services are running**

```bash
aws ecs describe-services \
  --cluster trading-analysis \
  --services trading-analysis-dagster trading-analysis-grafana \
  --region ap-northeast-1 \
  --query 'services[*].{Name:serviceName,Status:status,Running:runningCount,Desired:desiredCount}' \
  --output table
```

Expected: both services show `runningCount = 1`, `status = ACTIVE`.

- [ ] **Step 4: Verify Dagster webserver is reachable**

```bash
# Get ALB DNS name from Terraform output
ALB=$(terraform output -raw alb_dns_name)
curl -s "$ALB/server_info" | python3 -m json.tool | grep version
```

Expected: JSON response with Dagster version info (HTTP 200).

- [ ] **Step 5: Verify Grafana is reachable**

```bash
ALB=$(terraform output -raw alb_dns_name)
curl -s "$ALB/grafana/api/health"
```

Expected: `{"commit":"...","database":"ok","version":"..."}` (HTTP 200).

- [ ] **Step 6: Check ECS task logs for SQLite init**

```bash
aws logs tail /ecs/trading-analysis-dagster \
  --region ap-northeast-1 \
  --since 5m \
  --format short
```

Expected: Dagster startup logs with no Postgres connection errors. Look for lines like `Starting Dagster services` and no `connection refused` errors.

- [ ] **Step 7: Commit final state**

```bash
git add -A
git status  # confirm only expected files changed
git commit -m "chore: apply cost reduction — NAT removed, EFS/SQLite, 1vCPU dagster"
```

---

## Self-Review Notes

**Spec coverage check:**
- Change 1 (NAT removal) → Task 3 ✓
- Change 2 (right-size Dagster) → Task 5 ✓
- Change 3 (RDS → SQLite on EFS) → Tasks 2, 4, 5 ✓
- Change 4 (backfill skip logic) → Task 1 ✓
- EFS IAM permissions → Task 4 ✓
- Narrow secrets IAM → Task 5 ✓
- Deployment verification → Task 6 ✓

**Order rationale:** Python/test changes first (Tasks 1-2, no AWS needed), then Terraform in logical dependency order (networking → storage → compute), then apply.
