# Split Deploy Stage: Parallel Dagster + Grafana Deploys

**Date:** 2026-04-15  
**Status:** Approved

## Overview

Replace the single `Deploy` CodeBuild action with three parallel-capable actions within the same pipeline stage: `SchemaApply` (run first), then `DeployDagster` and `DeployGrafana` in parallel. This eliminates unnecessary serialization — Grafana currently waits for Dagster to fully stabilize before it starts, despite having no dependency on it.

## Pipeline Stage Structure

```
Stage: Deploy
  ├── action: SchemaApply     (run_order = 1)  ← ClickHouse migration
  ├── action: DeployDagster   (run_order = 2)  ← Dagster ECS deploy + wait
  └── action: DeployGrafana   (run_order = 2)  ← Grafana ECS deploy + wait
```

`run_order = 1` runs first and must succeed before `run_order = 2` actions start. The two `run_order = 2` actions run in parallel.

## New Buildspecs

### `buildspec/schema.yml` (replaces schema portion of deploy.yml)

- Installs `clickhouse-client`
- Fetches `trading-analysis/clickhouse` secret from Secrets Manager
- Applies `schemas/clickhouse_cloud.sql` via `clickhouse-client --multiquery`
- No ECS involvement

### `buildspec/deploy_dagster.yml` (replaces Dagster portion of deploy.yml)

- Sources `deploy_env.sh` from the `built` secondary artifact (`$CODEBUILD_SRC_DIR_built/deploy_env.sh`)
- Describes current `trading-analysis-dagster` task definition
- Patches `containerDefinitions` images (`dagster-webserver`, `dagster-daemon`) to `$IMAGE_URI`
- Registers new task definition revision
- Calls `ecs update-service --force-new-deployment` on `trading-analysis-dagster`
- Waits: `aws ecs wait services-stable --services trading-analysis-dagster`

### `buildspec/deploy_grafana.yml` (replaces Grafana portion of deploy.yml)

- Sources `deploy_env.sh` from the `built` secondary artifact
- Describes current `trading-analysis-grafana` task definition
- Patches `containerDefinitions` image (`grafana`) to `$GRAFANA_IMAGE_URI`
- Registers new task definition revision
- Calls `ecs update-service --force-new-deployment` on `trading-analysis-grafana`
- Waits: `aws ecs wait services-stable --services trading-analysis-grafana`

### `buildspec/deploy.yml`

Deleted.

## Terraform Changes (`infra/terraform/codepipeline.tf`)

### Remove
- `aws_codebuild_project.deploy`
- `aws_cloudwatch_log_group.codebuild_deploy`

### Add
Three new CodeBuild projects, each `BUILD_GENERAL1_SMALL`, `LINUX_CONTAINER`, standard:7.0:

| Resource | Buildspec | Env Vars | Privileged |
|---|---|---|---|
| `aws_codebuild_project.schema` | `buildspec/schema.yml` | `DEPLOY_REGION`, `CLICKHOUSE_HOST_SECRET` (Secrets Manager) | no |
| `aws_codebuild_project.deploy_dagster` | `buildspec/deploy_dagster.yml` | `DEPLOY_REGION`, `AWS_ACCOUNT_ID`, `ECS_CLUSTER`, `ECS_SERVICE=trading-analysis-dagster`, `IMAGE_NAME` | no |
| `aws_codebuild_project.deploy_grafana` | `buildspec/deploy_grafana.yml` | `DEPLOY_REGION`, `AWS_ACCOUNT_ID`, `ECS_CLUSTER`, `GRAFANA_IMAGE_NAME` | no |

Three new CloudWatch log groups with 14-day retention:
- `/codebuild/{prefix}-schema`
- `/codebuild/{prefix}-deploy-dagster`
- `/codebuild/{prefix}-deploy-grafana`

### Update Pipeline Stage

Replace the single `DeployToECS` action with:

```hcl
action {
  name             = "SchemaApply"
  run_order        = 1
  input_artifacts  = ["source"]
  # only needs source for schemas/clickhouse_cloud.sql
}

action {
  name             = "DeployDagster"
  run_order        = 2
  input_artifacts  = ["source", "built"]
}

action {
  name             = "DeployGrafana"
  run_order        = 2
  input_artifacts  = ["source", "built"]
}
```

### Update IAM Policy

Replace `aws_codebuild_project.deploy.arn` in the CodePipeline role policy `Resource` list with:
- `aws_codebuild_project.schema.arn`
- `aws_codebuild_project.deploy_dagster.arn`
- `aws_codebuild_project.deploy_grafana.arn`

## What Does Not Change

- `buildspec/build.yml` — untouched; `deploy_env.sh` still flows via the `built` artifact
- `buildspec/test.yml`, `buildspec/infra.yml` — untouched
- `Dockerfile`, `Dockerfile.grafana` — untouched
- All other pipeline stages (Source, Test, Infra, Build) — untouched
- The `built` artifact is passed as a secondary source to `DeployDagster` and `DeployGrafana` (same as today's single deploy action). `SchemaApply` only needs `source`.

## Files Changed Summary

| File | Action |
|---|---|
| `buildspec/deploy.yml` | Delete |
| `buildspec/schema.yml` | Create |
| `buildspec/deploy_dagster.yml` | Create |
| `buildspec/deploy_grafana.yml` | Create |
| `infra/terraform/codepipeline.tf` | Modify |
