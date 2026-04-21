# Grafana Cloud Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate Grafana from self-hosted ECS Fargate to Grafana Cloud, pushing dashboards via the Grafana Cloud HTTP API on every push to `main`.

**Architecture:** Remove the Grafana Docker build and ECS deployment from CI/CD. Add a new `deploy-grafana-cloud` job that POSTs each dashboard JSON to the Grafana Cloud API using `curl`. Remove all Grafana-related Terraform resources so ECS infrastructure is cleaned up on next `terraform apply`.

**Tech Stack:** GitHub Actions (`curl`), Grafana Cloud HTTP API, Terraform (resource removal)

---

## File Map

| File | Change |
|------|--------|
| `.github/workflows/ci-cd.yml` | Remove Grafana Docker build step + `deploy-grafana` job; add `deploy-grafana-cloud` job |
| `infra/terraform/main.tf` | Remove 6 Grafana-related Terraform resources |
| `Dockerfile.grafana` | Delete |
| `infra/grafana/provisioning/` | Delete directory (no longer used) |

---

## Pre-requisite: One-Time Manual Setup

**Do this before running CI.** These steps are in the Grafana Cloud UI — not automated.

1. Go to [grafana.com](https://grafana.com) → Sign up → Create a stack (free tier works). Note your stack URL, e.g. `https://yourorg.grafana.net`.
2. In your stack: **Administration → Plugins** → search `ClickHouse` → Install.
3. **Connections → Data sources → Add data source → ClickHouse**:
   - Name: `ClickHouse`
   - Server address: your ClickHouse host (get from AWS Secrets Manager: `trading-analysis/clickhouse` → `host` key, e.g. `qu6o0w9hl8.ap-northeast-1.aws.clickhouse.cloud`)
   - Server port: `8443`
   - Protocol: `HTTP`
   - Secure connection: enabled (TLS)
   - Username: `dev_ro3`
   - Password: from `trading-analysis/clickhouse` → `password` key
   - Expand **Advanced settings** → set **Data source UID** to exactly `clickhouse` (lowercase, no spaces) — this is critical, dashboards hardcode this UID
   - Click **Save & test** — should show green
4. **Administration → Service accounts → Add service account**:
   - Name: `github-actions`
   - Role: `Editor`
   - Click **Add service account token** → copy the token value
5. Add two GitHub Secrets on `williamwxz/trading-analysis` (Settings → Secrets → Actions):
   - `GRAFANA_CLOUD_TOKEN` — the token from step 4
   - `GRAFANA_CLOUD_URL` — your stack URL, e.g. `https://yourorg.grafana.net` (no trailing slash)

---

## Task 1: Add `deploy-grafana-cloud` job to CI

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Add the new job at the end of `ci-cd.yml`**

Open `.github/workflows/ci-cd.yml`. After the closing of the `deploy-grafana` job (line 272), append this new job:

```yaml
  deploy-grafana-cloud:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Push dashboards to Grafana Cloud
        env:
          GRAFANA_CLOUD_TOKEN: ${{ secrets.GRAFANA_CLOUD_TOKEN }}
          GRAFANA_CLOUD_URL: ${{ secrets.GRAFANA_CLOUD_URL }}
        run: |
          set -eu
          for file in infra/grafana/dashboards/*.json; do
            echo "Uploading $file..."
            HTTP_STATUS=$(curl -s -o /tmp/grafana_response.json -w "%{http_code}" \
              -X POST \
              -H "Content-Type: application/json" \
              -H "Authorization: Bearer $GRAFANA_CLOUD_TOKEN" \
              "$GRAFANA_CLOUD_URL/api/dashboards/db" \
              -d "{\"dashboard\": $(cat "$file"), \"overwrite\": true, \"folderId\": 0}")
            if [ "$HTTP_STATUS" -ne 200 ]; then
              echo "ERROR: $file returned HTTP $HTTP_STATUS"
              cat /tmp/grafana_response.json
              exit 1
            fi
            echo "OK: $file (HTTP $HTTP_STATUS)"
          done
          echo "All dashboards uploaded."
```

- [ ] **Step 2: Verify the YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci-cd.yml'))" && echo "YAML valid"
```

Expected: `YAML valid`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add deploy-grafana-cloud job to push dashboards via API"
```

---

## Task 2: Remove Grafana Docker build from CI

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Remove `grafana_image_uri` from `build` job outputs**

In `.github/workflows/ci-cd.yml`, find the `outputs` block of the `build` job (lines 36–39) and remove the `grafana_image_uri` line:

```yaml
    outputs:
      image_uri: ${{ steps.build_dagster.outputs.image_uri }}
```

(Delete the line: `grafana_image_uri: ${{ steps.build_grafana.outputs.grafana_image_uri }}`)

- [ ] **Step 2: Remove the `Build and push Grafana image` step**

Delete the entire step from the `build` job (lines 70–85):

```yaml
      - name: Build and push Grafana image
        id: build_grafana
        env:
          REGISTRY: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com
        run: |
          GRAFANA_IMAGE_URI=$REGISTRY/trading-analysis-grafana:${{ github.sha }}
          GRAFANA_IMAGE_LATEST=$REGISTRY/trading-analysis-grafana:latest
          GRAFANA_IMAGE_CACHE=$REGISTRY/trading-analysis-grafana:cache
          docker buildx build \
            --cache-from type=registry,ref=$GRAFANA_IMAGE_CACHE \
            --cache-to   type=registry,ref=$GRAFANA_IMAGE_CACHE,mode=max \
            --provenance=false \
            -f Dockerfile.grafana \
            -t $GRAFANA_IMAGE_URI -t $GRAFANA_IMAGE_LATEST \
            --push .
          echo "grafana_image_uri=$GRAFANA_IMAGE_URI" >> $GITHUB_OUTPUT
```

- [ ] **Step 3: Remove the `deploy-grafana` job**

Delete the entire `deploy-grafana` job (lines 201–272):

```yaml
  deploy-grafana:
    runs-on: ubuntu-latest
    needs: build
    ...
    # everything through the closing of the job
```

- [ ] **Step 4: Verify the YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci-cd.yml'))" && echo "YAML valid"
```

Expected: `YAML valid`

- [ ] **Step 5: Verify no Grafana ECS references remain**

```bash
grep -n "grafana" .github/workflows/ci-cd.yml
```

Expected output: only lines inside the new `deploy-grafana-cloud` job (the curl loop). There should be no references to `trading-analysis-grafana` ECS service, `GRAFANA_IMAGE_URI`, or `Dockerfile.grafana`.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: remove Grafana ECS build and deploy jobs"
```

---

## Task 3: Remove Grafana Terraform resources

**Files:**
- Modify: `infra/terraform/main.tf`

- [ ] **Step 1: Remove `aws_ecs_service.grafana`**

Find and delete this block in `main.tf` (around line 470):

```hcl
resource "aws_ecs_service" "grafana" {
  name            = "${local.name_prefix}-grafana"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.grafana.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  health_check_grace_period_seconds = 60

  load_balancer {
    target_group_arn = aws_lb_target_group.grafana.arn
    container_name   = "grafana"
    container_port   = 3000
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_lb_listener.http]
}
```

- [ ] **Step 2: Remove `aws_ecs_task_definition.grafana`**

Find and delete this block (around line 415):

```hcl
resource "aws_ecs_task_definition" "grafana" {
  family                   = "${local.name_prefix}-grafana"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "grafana"
      ...
    }
  ])

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}
```

- [ ] **Step 3: Remove `aws_lb_target_group.grafana`**

Find and delete this block (around line 541):

```hcl
resource "aws_lb_target_group" "grafana" {
  name        = "${local.name_prefix}-grafana-v2"
  port        = 3000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"

  health_check {
    path                = "/grafana/api/health"
    healthy_threshold   = 2
    unhealthy_threshold = 10
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}
```

- [ ] **Step 4: Remove `aws_lb_listener_rule.grafana`**

Find and delete this block (around line 572):

```hcl
resource "aws_lb_listener_rule" "grafana" {
  listener_arn = aws_lb_listener.http.arn
  priority     = 10

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.grafana.arn
  }

  condition {
    path_pattern {
      values = ["/grafana*"]
    }
  }
}
```

- [ ] **Step 5: Remove `aws_ecr_repository.grafana`**

Find and delete this block (around line 667):

```hcl
resource "aws_ecr_repository" "grafana" {
  name                 = "${local.name_prefix}-grafana"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}
```

- [ ] **Step 6: Remove `aws_cloudwatch_log_group.grafana`**

Find and delete this block (around line 694):

```hcl
resource "aws_cloudwatch_log_group" "grafana" {
  name              = "/ecs/${local.name_prefix}-grafana"
  retention_in_days = 30
  tags              = local.common_tags
}
```

- [ ] **Step 7: Verify no Grafana references remain in main.tf**

```bash
grep -n "grafana" infra/terraform/main.tf
```

Expected: no output (zero matches).

- [ ] **Step 8: Validate Terraform syntax**

```bash
cd infra/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 9: Commit**

```bash
git add infra/terraform/main.tf
git commit -m "infra: remove Grafana ECS/ECR/ALB Terraform resources"
```

---

## Task 4: Delete unused files

**Files:**
- Delete: `Dockerfile.grafana`
- Delete: `infra/grafana/provisioning/` (directory)

- [ ] **Step 1: Delete `Dockerfile.grafana`**

```bash
git rm Dockerfile.grafana
```

- [ ] **Step 2: Delete `infra/grafana/provisioning/`**

```bash
git rm -r infra/grafana/provisioning/
```

- [ ] **Step 3: Verify dashboard files are untouched**

```bash
ls infra/grafana/dashboards/
```

Expected: all 5 dashboard JSON files still present:
```
strategy-pnl-l1-instance.json
strategy-pnl-l2-sid-underlying.json
strategy-pnl-l3-sid.json
strategy-pnl-l4-underlying.json
strategy-pnl-l5-portfolio.json
```

- [ ] **Step 4: Commit**

```bash
git commit -m "chore: remove Dockerfile.grafana and unused provisioning config"
```

---

## Task 5: Apply Terraform to decommission ECS Grafana

**This task runs in the GitHub Actions UI, not locally.**

- [ ] **Step 1: Push all commits to `main`**

```bash
git push origin main
```

- [ ] **Step 2: Run the Terraform workflow**

Go to the GitHub repository → **Actions** → **Terraform** workflow → **Run workflow** → set `auto_approve` to `true` → click **Run workflow**.

This will destroy:
- `aws_ecs_service.grafana`
- `aws_ecs_task_definition.grafana`
- `aws_ecr_repository.grafana` (and its images)
- `aws_lb_target_group.grafana`
- `aws_lb_listener_rule.grafana`
- `aws_cloudwatch_log_group.grafana`

- [ ] **Step 3: Confirm Terraform apply succeeds**

In the workflow run logs, look for:

```
Destroy complete! Resources: 6 destroyed.
```

(The exact count may vary if other resources have drifted, but the 6 Grafana resources should be in the destroyed list.)

---

## Task 6: Verify dashboards are live on Grafana Cloud

- [ ] **Step 1: Confirm `deploy-grafana-cloud` CI job ran**

After the push in Task 5 Step 1, go to **Actions** → the latest CI/CD run → click `deploy-grafana-cloud` job. Verify each dashboard shows:

```
Uploading infra/grafana/dashboards/strategy-pnl-l1-instance.json...
OK: infra/grafana/dashboards/strategy-pnl-l1-instance.json (HTTP 200)
...
All dashboards uploaded.
```

- [ ] **Step 2: Open Grafana Cloud and confirm dashboards appear**

Go to `$GRAFANA_CLOUD_URL` → **Dashboards**. All 5 dashboards should be visible:
- strategy-pnl-l1-instance
- strategy-pnl-l2-sid-underlying
- strategy-pnl-l3-sid
- strategy-pnl-l4-underlying
- strategy-pnl-l5-portfolio

- [ ] **Step 3: Open one dashboard and verify panels load data**

Click into `strategy-pnl-l5-portfolio` (portfolio-level, most likely to have data). Panels should render charts, not show "datasource not found" or "No data". If panels show "datasource not found", verify the datasource UID is exactly `clickhouse` (Administration → Data sources → ClickHouse → Advanced settings).
