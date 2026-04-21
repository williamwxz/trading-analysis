# Split Deploy Stage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single monolithic Deploy CodeBuild action with three actions — `SchemaApply` (run first), then `DeployDagster` and `DeployGrafana` in parallel — so Grafana and Dagster deploy independently.

**Architecture:** Three new buildspecs split the work: `schema.yml` handles ClickHouse migration, `deploy_dagster.yml` and `deploy_grafana.yml` each handle their own ECS service. Terraform wires three new CodeBuild projects into the existing Deploy stage with `run_order` so schema runs first, then the two deploys run in parallel. The pipeline change must be applied manually with `terraform apply` before pushing — you can't use the pipeline to update itself.

**Tech Stack:** AWS CodePipeline, AWS CodeBuild, AWS ECS Fargate, Terraform, bash (buildspecs)

---

## File Map

| File | Action |
|---|---|
| `buildspec/deploy.yml` | Delete |
| `buildspec/schema.yml` | Create |
| `buildspec/deploy_dagster.yml` | Create |
| `buildspec/deploy_grafana.yml` | Create |
| `infra/terraform/codepipeline.tf` | Modify |

---

### Task 1: Create `buildspec/schema.yml`

**Files:**
- Create: `buildspec/schema.yml`

This buildspec installs `clickhouse-client`, fetches the ClickHouse credentials from Secrets Manager, and applies the schema. It only needs the `source` artifact (for `schemas/clickhouse_cloud.sql`).

- [ ] **Step 1: Create the file**

Create `buildspec/schema.yml` with this exact content:

```yaml
version: 0.2

# -----------------------------------------------------------------------------
# Stage: Deploy / SchemaApply
# Applies ClickHouse schema. Runs before ECS deploys.
# -----------------------------------------------------------------------------

phases:
  install:
    commands:
      - curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
      - echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | tee /etc/apt/sources.list.d/clickhouse.list
      - apt-get update -q && apt-get install -y -q clickhouse-client

  pre_build:
    commands:
      - |
        export CH_SECRET=$(aws secretsmanager get-secret-value --secret-id trading-analysis/clickhouse --region $DEPLOY_REGION --query SecretString --output text)
        export CLICKHOUSE_HOST=$(echo $CH_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['host'])")
        export CLICKHOUSE_PASSWORD=$(echo $CH_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
        echo "CLICKHOUSE_HOST=$CLICKHOUSE_HOST"

  build:
    commands:
      - echo "=== Applying ClickHouse schema ==="
      - |
        export CH_SECRET=$(aws secretsmanager get-secret-value --secret-id trading-analysis/clickhouse --region $DEPLOY_REGION --query SecretString --output text)
        export CLICKHOUSE_HOST=$(echo $CH_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['host'])")
        export CLICKHOUSE_PASSWORD=$(echo $CH_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
        clickhouse-client --host "$CLICKHOUSE_HOST" --port 9440 --secure --user default --password "$CLICKHOUSE_PASSWORD" --multiquery < schemas/clickhouse_cloud.sql
        echo "Schema applied."
```

- [ ] **Step 2: Commit**

```bash
git add buildspec/schema.yml
git commit -m "feat: add schema.yml buildspec for ClickHouse migration step"
```

---

### Task 2: Create `buildspec/deploy_dagster.yml`

**Files:**
- Create: `buildspec/deploy_dagster.yml`

Sources `deploy_env.sh` from the `built` secondary artifact (available as `$CODEBUILD_SRC_DIR_built`), patches the Dagster task definition with the new image URI, updates the ECS service, and waits for stability.

- [ ] **Step 1: Create the file**

Create `buildspec/deploy_dagster.yml` with this exact content:

```yaml
version: 0.2

# -----------------------------------------------------------------------------
# Stage: Deploy / DeployDagster
# Updates ECS Dagster service with the new image and waits for stability.
# Reads IMAGE_URI from the 'built' secondary artifact (deploy_env.sh).
# -----------------------------------------------------------------------------

phases:
  pre_build:
    commands:
      - . $CODEBUILD_SRC_DIR_built/deploy_env.sh
      - echo "Deploying Dagster image $IMAGE_URI"

  build:
    commands:
      - echo "=== Updating Dagster service ==="
      - |
        . $CODEBUILD_SRC_DIR_built/deploy_env.sh
        TASK_DEF=$(aws ecs describe-task-definition --task-definition trading-analysis-dagster --region $DEPLOY_REGION --query taskDefinition --output json)
        NEW_TASK=$(echo "$TASK_DEF" | python3 -c "import sys, json, os; td = json.load(sys.stdin); img = os.environ['IMAGE_URI']; [c.__setitem__('image', img) for c in td['containerDefinitions'] if c['name'] in ('dagster-webserver', 'dagster-daemon')]; [td.pop(k, None) for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy')]; print(json.dumps(td))")
        NEW_ARN=$(aws ecs register-task-definition --cli-input-json "$NEW_TASK" --region $DEPLOY_REGION --query taskDefinition.taskDefinitionArn --output text)
        echo "Registered Dagster: $NEW_ARN"
        aws ecs update-service --cluster $ECS_CLUSTER --service trading-analysis-dagster --task-definition "$NEW_ARN" --force-new-deployment --region $DEPLOY_REGION --output json > /dev/null

  post_build:
    commands:
      - echo "=== Waiting for Dagster service to stabilize ==="
      - aws ecs wait services-stable --cluster $ECS_CLUSTER --services trading-analysis-dagster --region $DEPLOY_REGION
      - echo "=== Dagster deploy complete ==="
      - aws ecs describe-services --cluster $ECS_CLUSTER --services trading-analysis-dagster --region $DEPLOY_REGION --query 'services[*].{Name:serviceName,Status:status,Running:runningCount,Desired:desiredCount}' --output table
```

- [ ] **Step 2: Commit**

```bash
git add buildspec/deploy_dagster.yml
git commit -m "feat: add deploy_dagster.yml buildspec"
```

---

### Task 3: Create `buildspec/deploy_grafana.yml`

**Files:**
- Create: `buildspec/deploy_grafana.yml`

Same pattern as `deploy_dagster.yml` but for the Grafana service: patches the `grafana` container with `$GRAFANA_IMAGE_URI` and waits for `trading-analysis-grafana`.

- [ ] **Step 1: Create the file**

Create `buildspec/deploy_grafana.yml` with this exact content:

```yaml
version: 0.2

# -----------------------------------------------------------------------------
# Stage: Deploy / DeployGrafana
# Updates ECS Grafana service with the new image and waits for stability.
# Reads GRAFANA_IMAGE_URI from the 'built' secondary artifact (deploy_env.sh).
# -----------------------------------------------------------------------------

phases:
  pre_build:
    commands:
      - . $CODEBUILD_SRC_DIR_built/deploy_env.sh
      - echo "Deploying Grafana image $GRAFANA_IMAGE_URI"

  build:
    commands:
      - echo "=== Updating Grafana service ==="
      - |
        . $CODEBUILD_SRC_DIR_built/deploy_env.sh
        TASK_DEF_G=$(aws ecs describe-task-definition --task-definition trading-analysis-grafana --region $DEPLOY_REGION --query taskDefinition --output json)
        NEW_TASK_G=$(echo "$TASK_DEF_G" | python3 -c "import sys, json, os; td = json.load(sys.stdin); img = os.environ['GRAFANA_IMAGE_URI']; [c.__setitem__('image', img) for c in td['containerDefinitions'] if c['name'] == 'grafana']; [td.pop(k, None) for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy')]; print(json.dumps(td))")
        NEW_ARN_G=$(aws ecs register-task-definition --cli-input-json "$NEW_TASK_G" --region $DEPLOY_REGION --query taskDefinition.taskDefinitionArn --output text)
        echo "Registered Grafana: $NEW_ARN_G"
        aws ecs update-service --cluster $ECS_CLUSTER --service trading-analysis-grafana --task-definition "$NEW_ARN_G" --force-new-deployment --region $DEPLOY_REGION --output json > /dev/null

  post_build:
    commands:
      - echo "=== Waiting for Grafana service to stabilize ==="
      - aws ecs wait services-stable --cluster $ECS_CLUSTER --services trading-analysis-grafana --region $DEPLOY_REGION
      - echo "=== Grafana deploy complete ==="
      - aws ecs describe-services --cluster $ECS_CLUSTER --services trading-analysis-grafana --region $DEPLOY_REGION --query 'services[*].{Name:serviceName,Status:status,Running:runningCount,Desired:desiredCount}' --output table
```

- [ ] **Step 2: Commit**

```bash
git add buildspec/deploy_grafana.yml
git commit -m "feat: add deploy_grafana.yml buildspec"
```

---

### Task 4: Update `infra/terraform/codepipeline.tf` — Replace deploy project

**Files:**
- Modify: `infra/terraform/codepipeline.tf`

Remove the existing `aws_codebuild_project.deploy` block and its CloudWatch log group. Add three new CodeBuild projects and log groups.

- [ ] **Step 1: Remove the existing deploy CodeBuild project block**

In `infra/terraform/codepipeline.tf`, delete the entire `aws_codebuild_project.deploy` resource (lines 170–222) and the `aws_cloudwatch_log_group.codebuild_deploy` resource (lines 463–468).

- [ ] **Step 2: Add the three new CodeBuild projects**

After the closing `}` of `aws_codebuild_project.build` (after line 163), add:

```hcl
# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Schema (ClickHouse migration)
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "schema" {
  name         = "${local.name_prefix}-schema"
  service_role = aws_iam_role.codebuild.arn
  tags         = local.common_tags

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"

    environment_variable {
      name  = "DEPLOY_REGION"
      value = local.deploy_region
    }
    environment_variable {
      name  = "CLICKHOUSE_HOST_SECRET"
      value = aws_secretsmanager_secret.clickhouse.arn
      type  = "SECRETS_MANAGER"
    }
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec/schema.yml"
  }

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/${local.name_prefix}-schema"
      stream_name = "build"
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Deploy Dagster
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "deploy_dagster" {
  name         = "${local.name_prefix}-deploy-dagster"
  service_role = aws_iam_role.codebuild.arn
  tags         = local.common_tags

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"

    environment_variable {
      name  = "DEPLOY_REGION"
      value = local.deploy_region
    }
    environment_variable {
      name  = "AWS_ACCOUNT_ID"
      value = data.aws_caller_identity.current.account_id
    }
    environment_variable {
      name  = "ECS_CLUSTER"
      value = local.name_prefix
    }
    environment_variable {
      name  = "ECS_SERVICE"
      value = "trading-analysis-dagster"
    }
    environment_variable {
      name  = "IMAGE_NAME"
      value = "trading-analysis-dagster"
    }
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec/deploy_dagster.yml"
  }

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/${local.name_prefix}-deploy-dagster"
      stream_name = "build"
    }
  }
}


# ─────────────────────────────────────────────────────────────────────────────
# CodeBuild — Deploy Grafana
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_codebuild_project" "deploy_grafana" {
  name         = "${local.name_prefix}-deploy-grafana"
  service_role = aws_iam_role.codebuild.arn
  tags         = local.common_tags

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type = "BUILD_GENERAL1_SMALL"
    image        = "aws/codebuild/standard:7.0"
    type         = "LINUX_CONTAINER"

    environment_variable {
      name  = "DEPLOY_REGION"
      value = local.deploy_region
    }
    environment_variable {
      name  = "AWS_ACCOUNT_ID"
      value = data.aws_caller_identity.current.account_id
    }
    environment_variable {
      name  = "ECS_CLUSTER"
      value = local.name_prefix
    }
    environment_variable {
      name  = "GRAFANA_IMAGE_NAME"
      value = "trading-analysis-grafana"
    }
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec/deploy_grafana.yml"
  }

  logs_config {
    cloudwatch_logs {
      group_name  = "/codebuild/${local.name_prefix}-deploy-grafana"
      stream_name = "build"
    }
  }
}
```

- [ ] **Step 3: Add the three new CloudWatch log groups**

In the CloudWatch log groups section (after the existing `codebuild_build` log group), add:

```hcl
resource "aws_cloudwatch_log_group" "codebuild_schema" {
  name              = "/codebuild/${local.name_prefix}-schema"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "codebuild_deploy_dagster" {
  name              = "/codebuild/${local.name_prefix}-deploy-dagster"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "codebuild_deploy_grafana" {
  name              = "/codebuild/${local.name_prefix}-deploy-grafana"
  retention_in_days = 14
  tags              = local.common_tags
}
```

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/codepipeline.tf
git commit -m "feat: add schema, deploy_dagster, deploy_grafana CodeBuild projects"
```

---

### Task 5: Update the Deploy pipeline stage in Terraform

**Files:**
- Modify: `infra/terraform/codepipeline.tf`

Replace the single `DeployToECS` action in the `Deploy` stage with three actions.

- [ ] **Step 1: Replace the Deploy stage actions**

Find the `Deploy` stage block in `aws_codepipeline.main` (currently contains a single `DeployToECS` action). Replace the entire stage content with:

```hcl
  stage {
    name = "Deploy"

    action {
      name             = "SchemaApply"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      run_order        = 1
      input_artifacts  = ["source"]

      configuration = {
        ProjectName = aws_codebuild_project.schema.name
      }
    }

    action {
      name             = "DeployDagster"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      run_order        = 2
      input_artifacts  = ["source", "built"]

      configuration = {
        ProjectName   = aws_codebuild_project.deploy_dagster.name
        PrimarySource = "source"
      }
    }

    action {
      name             = "DeployGrafana"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      run_order        = 2
      input_artifacts  = ["source", "built"]

      configuration = {
        ProjectName   = aws_codebuild_project.deploy_grafana.name
        PrimarySource = "source"
      }
    }
  }
```

- [ ] **Step 2: Update the CodePipeline IAM policy**

In `aws_iam_role_policy.codepipeline`, find the `codebuild:BatchGetBuilds` / `codebuild:StartBuild` statement. Replace:

```hcl
Resource = [
  aws_codebuild_project.test.arn,
  aws_codebuild_project.infra.arn,
  aws_codebuild_project.build.arn,
  aws_codebuild_project.deploy.arn,
]
```

with:

```hcl
Resource = [
  aws_codebuild_project.test.arn,
  aws_codebuild_project.infra.arn,
  aws_codebuild_project.build.arn,
  aws_codebuild_project.schema.arn,
  aws_codebuild_project.deploy_dagster.arn,
  aws_codebuild_project.deploy_grafana.arn,
]
```

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/codepipeline.tf
git commit -m "feat: split Deploy stage into SchemaApply + DeployDagster + DeployGrafana"
```

---

### Task 6: Delete `buildspec/deploy.yml`

**Files:**
- Delete: `buildspec/deploy.yml`

- [ ] **Step 1: Delete the file and commit**

```bash
git rm buildspec/deploy.yml
git commit -m "chore: remove monolithic deploy.yml (replaced by schema/deploy_dagster/deploy_grafana)"
```

---

### Task 7: Apply Terraform manually and verify

The pipeline cannot update itself — this Terraform change must be applied manually before pushing.

- [ ] **Step 1: Run terraform plan to review changes**

```bash
cd infra/terraform
terraform plan
```

Expected output: plan shows removing `trading-analysis-deploy` CodeBuild project, adding `trading-analysis-schema`, `trading-analysis-deploy-dagster`, `trading-analysis-deploy-grafana` CodeBuild projects, and updating the pipeline stage. No unexpected destroys.

- [ ] **Step 2: Apply**

```bash
terraform apply
```

Type `yes` when prompted. Expected: all resources created/updated successfully, no errors.

- [ ] **Step 3: Verify in AWS Console**

Open the CodePipeline console in `ap-northeast-1`. Confirm the `Deploy` stage now shows three actions: `SchemaApply`, `DeployDagster`, `DeployGrafana`.

- [ ] **Step 4: Push to main to trigger a full pipeline run**

```bash
git push origin main
```

Watch the pipeline run. Confirm:
- `SchemaApply` completes first
- `DeployDagster` and `DeployGrafana` start simultaneously after `SchemaApply`
- Both stabilize independently
