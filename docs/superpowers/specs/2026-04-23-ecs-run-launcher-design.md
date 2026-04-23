# EcsRunLauncher + Cost Optimization Design

**Date:** 2026-04-23  
**Status:** Approved

## Summary

Switch Dagster from `DefaultRunLauncher` (runs inside the always-on task) to `EcsRunLauncher` (each run gets its own ECS task). Right-size the always-on task from 2vCPU/8GB to 0.5vCPU/1GB. Fix the service to use FARGATE_SPOT. Clean up unused EFS resources. Expected cost: ~$4–6/month down from ~$58/month.

## Motivation

- Always-on task runs on plain on-demand Fargate (capacityProviderStrategy is null despite cluster default being FARGATE_SPOT) — billing bug
- 2vCPU/8GB is sized for peak run load, not idle webserver+daemon overhead
- `DefaultRunLauncher` runs all jobs inside the same task — a heavy backfill can starve the daemon or OOM the webserver
- EFS resources remain in Terraform but are unused (migrated to Supabase Postgres)

## Changes

### 1. `dagster.yaml` — EcsRunLauncher

Replace `DefaultRunLauncher` with `EcsRunLauncher`:

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

- `use_current_ecs_task_config: true` (default): inherits cluster, subnets, security groups, and task definition from the running webserver task
- Run tasks pick up secrets from the inherited container definition automatically
- FARGATE_SPOT for run tasks: safe because all assets are idempotent (watermark-based live assets, DELETE+INSERT partitioned backfills)

### 2. Terraform — Always-on task right-sizing

In `aws_ecs_task_definition.dagster`:
- `cpu: 2048 → 512`
- `memory: 8192 → 1024`

### 3. Terraform — Service capacity provider (fix billing bug)

In `aws_ecs_service.dagster`:
- Remove `launch_type = "FARGATE"`
- Add `capacity_provider_strategy` block using `FARGATE_SPOT`

### 4. Terraform — EFS cleanup

Remove (all unused since Supabase Postgres migration):
- `aws_efs_file_system.dagster`
- `aws_efs_mount_target.dagster`
- `aws_efs_access_point.dagster`
- `aws_security_group.efs`
- EFS permissions from `aws_iam_role_policy.ecs_task_policy`

No container definition volume mounts to remove (EFS was never mounted in the task definition).

### 5. Terraform — Task role IAM

Add to `aws_iam_role_policy.ecs_task_policy` so EcsRunLauncher can launch and monitor run tasks:

```
ecs:RunTask
ecs:StopTask
ecs:DescribeTasks
ecs:ListTasks
iam:PassRole  (scoped to ecs_execution + ecs_task role ARNs)
```

Existing `ecs:DescribeTaskDefinition` remains.

## No Changes Needed

- **NAT instance DNAT script**: filters by service name, unaffected by run tasks appearing in the cluster
- **CloudWatch log group**: run tasks inherit the same log group from the task definition
- **Security groups**: run tasks inherit `ecs_tasks` SG which already allows all outbound (ClickHouse, Supabase)
- **Secrets**: injected via container definition, inherited automatically by run tasks

## Cost Estimate

| Component | Before | After |
|---|---|---|
| Always-on task (2vCPU/8GB, on-demand) | ~$58/month | — |
| Always-on task (0.5vCPU/1GB, FARGATE_SPOT) | — | ~$3–4/month |
| Run tasks (short-lived, FARGATE_SPOT) | included above | ~$1–2/month |
| **Total** | **~$58/month** | **~$4–6/month** |

## Files Modified

- `dagster.yaml`
- `infra/terraform/main.tf`
