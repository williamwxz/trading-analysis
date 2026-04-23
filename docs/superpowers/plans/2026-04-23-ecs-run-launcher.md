# EcsRunLauncher + Cost Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch Dagster to EcsRunLauncher so runs get isolated ECS tasks, right-size the always-on task to 0.5vCPU/1GB, fix the service to use FARGATE_SPOT, and remove unused EFS infrastructure.

**Architecture:** `dagster.yaml` is updated to use `EcsRunLauncher` from `dagster-aws`, which launches each Dagster run as its own short-lived FARGATE_SPOT ECS task inheriting cluster/network config from the always-on task. The always-on task (webserver + daemon + code-server) is right-sized since it no longer needs headroom for run compute. All changes are in two files: `dagster.yaml` and `infra/terraform/main.tf`.

**Tech Stack:** Dagster, dagster-aws (EcsRunLauncher), AWS ECS Fargate, Terraform ~> 5.0

---

## File Map

| File | Change |
|---|---|
| `dagster.yaml` | Replace `DefaultRunLauncher` with `EcsRunLauncher` |
| `infra/terraform/main.tf` | Right-size task, fix service to FARGATE_SPOT, add IAM permissions, remove EFS |

---

### Task 1: Switch dagster.yaml to EcsRunLauncher

**Files:**
- Modify: `dagster.yaml:16-18`

- [ ] **Step 1: Replace the run_launcher block**

Open `dagster.yaml`. Replace:

```yaml
run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher
```

With:

```yaml
run_launcher:
  module: dagster_aws.ecs
  class: EcsRunLauncher
  config:
    run_resources:
      cpu: "256"
      memory: "512"
    run_task_kwargs:
      capacityProviderStrategy:
        - capacityProvider: FARGATE_SPOT
          weight: 1
```

- [ ] **Step 2: Verify dagster-aws is a dependency**

```bash
grep -i dagster-aws pyproject.toml
```

Expected: a line like `dagster-aws>=1.0`. If missing, add it to `[project.dependencies]` in `pyproject.toml` and run `pip install -e ".[dev]"`.

- [ ] **Step 3: Validate YAML parses**

```bash
python3 -c "import yaml; yaml.safe_load(open('dagster.yaml'))" && echo "OK"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add dagster.yaml
git commit -m "feat: switch to EcsRunLauncher with FARGATE_SPOT run tasks"
```

---

### Task 2: Right-size always-on ECS task definition

**Files:**
- Modify: `infra/terraform/main.tf:389-506` (`aws_ecs_task_definition.dagster`)

- [ ] **Step 1: Update cpu and memory on the task definition**

In `infra/terraform/main.tf`, find the `aws_ecs_task_definition.dagster` resource (around line 389). Change:

```hcl
  cpu                      = 2048
  memory                   = 8192
```

To:

```hcl
  cpu                      = 512
  memory                   = 1024
```

- [ ] **Step 2: Verify no per-container limits conflict**

The three containers (webserver, daemon, code-server) all have `cpu = 0` and `memory = null` — they share the task pool. No per-container changes needed.

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat: right-size always-on ECS task to 0.5vCPU/1GB"
```

---

### Task 3: Fix ECS service to use FARGATE_SPOT

**Files:**
- Modify: `infra/terraform/main.tf:508-526` (`aws_ecs_service.dagster`)

- [ ] **Step 1: Remove launch_type, add capacity_provider_strategy**

Find the `aws_ecs_service.dagster` resource (around line 508). Replace:

```hcl
  desired_count   = 1
  launch_type     = "FARGATE"
```

With:

```hcl
  desired_count   = 1

  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }
```

- [ ] **Step 2: Verify terraform fmt**

```bash
cd infra/terraform && terraform fmt main.tf
```

Expected: `main.tf` (reformatted, or no output if already clean)

- [ ] **Step 3: Validate terraform config**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat: switch ECS service from on-demand FARGATE to FARGATE_SPOT"
```

---

### Task 4: Add EcsRunLauncher IAM permissions to task role

**Files:**
- Modify: `infra/terraform/main.tf:735-772` (`aws_iam_role_policy.ecs_task_policy`)

- [ ] **Step 1: Add ECS + IAM statements to ecs_task_policy**

Find `aws_iam_role_policy.ecs_task_policy` (around line 735). The current `Statement` array has three entries (S3, ecs:DescribeTaskDefinition, EFS). Replace the entire `policy` block's `Statement` with:

```hcl
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      },
      {
        # EcsRunLauncher: introspect task definition and launch/monitor/stop run tasks
        Effect = "Allow"
        Action = [
          "ecs:DescribeTaskDefinition",
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
          "ecs:ListTasks",
        ]
        Resource = "*"
      },
      {
        # EcsRunLauncher: pass execution + task roles to new run tasks
        Effect = "Allow"
        Action = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_execution.arn,
          aws_iam_role.ecs_task.arn,
        ]
      },
    ]
  })
```

Note: the EFS statement is intentionally removed here (cleaned up in Task 5).

- [ ] **Step 2: Validate terraform config**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "feat: add EcsRunLauncher IAM permissions to ECS task role"
```

---

### Task 5: Remove unused EFS resources from Terraform

**Files:**
- Modify: `infra/terraform/main.tf` — delete EFS section (lines 289–351)

- [ ] **Step 1: Delete the EFS resource block**

In `infra/terraform/main.tf`, delete the entire section between the EFS comment header and the ECS Cluster section. This removes:

- `aws_efs_file_system.dagster` 
- `aws_security_group.efs`
- `aws_efs_mount_target.dagster`
- `aws_efs_access_point.dagster`

The block to delete starts at:
```
# ─────────────────────────────────────────────────────────────────────────────
# EFS — SQLite storage for Dagster metadata
```
And ends just before:
```
# ─────────────────────────────────────────────────────────────────────────────
# ECS Cluster (Fargate)
```

- [ ] **Step 2: Verify no remaining EFS references**

```bash
grep -n "efs" infra/terraform/main.tf
```

Expected: zero matches (the EFS permission was already removed from the task policy in Task 4).

- [ ] **Step 3: Validate terraform config**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "chore: remove unused EFS resources (migrated to Supabase Postgres)"
```

---

### Task 6: Apply Terraform and verify

- [ ] **Step 1: Plan the changes**

```bash
cd infra/terraform && terraform plan \
  -var="github_repo=williamwxz/trading-analysis" \
  2>&1 | tee /tmp/tf-plan.txt
```

Expected output should show:
- `~ aws_ecs_task_definition.dagster` — cpu/memory change
- `~ aws_ecs_service.dagster` — launch_type removed, capacity_provider_strategy added
- `~ aws_iam_role_policy.ecs_task_policy` — EFS removed, ECS/IAM actions added
- `- aws_efs_file_system.dagster` — destroy
- `- aws_efs_mount_target.dagster` — destroy
- `- aws_efs_access_point.dagster` — destroy
- `- aws_security_group.efs` — destroy

Review the plan carefully. If `aws_ecs_service.dagster` shows a replacement (destroy+create) rather than in-place update, that is expected — ECS services require recreation when switching from launch_type to capacity_provider_strategy.

- [ ] **Step 2: Apply**

```bash
cd infra/terraform && terraform apply \
  -var="github_repo=williamwxz/trading-analysis"
```

Type `yes` when prompted.

- [ ] **Step 3: Verify the service is running on FARGATE_SPOT**

```bash
aws ecs describe-services \
  --cluster trading-analysis \
  --services trading-analysis-dagster \
  --region ap-northeast-1 \
  --profile AdministratorAccess-339163283253 \
  --query 'services[0].{status:status,runningCount:runningCount,capacityProviderStrategy:capacityProviderStrategy,taskDefinition:taskDefinition}'
```

Expected: `capacityProviderStrategy` shows `FARGATE_SPOT`, `runningCount: 1`, `status: ACTIVE`.

- [ ] **Step 4: Verify task is right-sized**

```bash
aws ecs describe-task-definition \
  --task-definition trading-analysis-dagster \
  --region ap-northeast-1 \
  --profile AdministratorAccess-339163283253 \
  --query 'taskDefinition.{cpu:cpu,memory:memory}'
```

Expected: `{"cpu": "512", "memory": "1024"}`

- [ ] **Step 5: Verify Dagster UI is reachable**

Open `http://<nat_eip>:3000` (get EIP from `terraform output dagster_url`). The UI should load and show the asset graph. Check that the daemon is healthy (top-right status indicator should be green).

- [ ] **Step 6: Trigger a test run and verify it launches as a separate ECS task**

In the Dagster UI, manually materialize `binance_futures_ohlcv_minutely` (or any live asset). Then:

```bash
aws ecs list-tasks \
  --cluster trading-analysis \
  --region ap-northeast-1 \
  --profile AdministratorAccess-339163283253
```

Expected: 2 tasks — the always-on task + a short-lived run task. The run task should disappear after the run completes.

- [ ] **Step 7: Check run task capacity provider**

While a run is in flight, describe the run task ARN (from list-tasks output):

```bash
aws ecs describe-tasks \
  --cluster trading-analysis \
  --tasks <run-task-arn> \
  --region ap-northeast-1 \
  --profile AdministratorAccess-339163283253 \
  --query 'tasks[0].{cpu:cpu,memory:memory,capacityProviderName:capacityProviderName}'
```

Expected: `cpu: 256`, `memory: 512`, `capacityProviderName: FARGATE_SPOT`
