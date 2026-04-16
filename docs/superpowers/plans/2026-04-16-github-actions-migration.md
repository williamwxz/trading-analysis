# GitHub Actions Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace AWS CodePipeline + CodeBuild with GitHub Actions workflows, using OIDC for AWS auth and Secrets Manager for runtime credentials.

**Architecture:** A single `ci-cd.yml` workflow runs test → build → deploy (schema + dagster + grafana in parallel) on every push to `main`. A separate `terraform.yml` workflow is manually triggered only. AWS auth uses GitHub OIDC — no static credentials stored anywhere.

**Tech Stack:** GitHub Actions, `aws-actions/configure-aws-credentials@v4`, `aws-actions/amazon-ecr-login@v2`, AWS CLI, Docker, Terraform 1.5.7, Python 3.11, pytest, clickhouse-client

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| Create | `.github/workflows/ci-cd.yml` | Main pipeline: test + build + deploy |
| Create | `.github/workflows/terraform.yml` | Manual Terraform plan/apply |
| Delete | `infra/terraform/codepipeline.tf` | All CodePipeline/CodeBuild infrastructure |
| Delete | `buildspec/` (entire directory) | Dead code after migration |
| Modify | `CLAUDE.md` | Add GitHub Actions CI/CD setup section |

---

## Task 1: Create `.github/workflows/ci-cd.yml` — test job

**Files:**
- Create: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Create the workflows directory and write the test job**

```bash
mkdir -p .github/workflows
```

Create `.github/workflows/ci-cd.yml` with this content (test job only for now):

```yaml
name: CI/CD

on:
  push:
    branches: [main]

permissions:
  id-token: write
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -e ".[dev]"

      - name: Run tests
        run: pytest tests/ -v --tb=short
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add test job to GitHub Actions workflow"
```

---

## Task 2: Add the build job to `ci-cd.yml`

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

The build job logs into ECR, builds both Docker images, pushes them with commit SHA and `:latest` tags, and exposes the image URIs as job outputs for downstream jobs.

- [ ] **Step 1: Append the build job**

Add to `.github/workflows/ci-cd.yml` after the `test` job (inside `jobs:`):

```yaml
  build:
    runs-on: ubuntu-latest
    needs: test
    outputs:
      image_uri: ${{ steps.export.outputs.image_uri }}
      grafana_image_uri: ${{ steps.export.outputs.grafana_image_uri }}
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to ECR
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push Dagster image
        env:
          IMAGE_URI: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com/trading-analysis-dagster:${{ github.sha }}
          IMAGE_LATEST: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com/trading-analysis-dagster:latest
        run: |
          docker pull $IMAGE_LATEST || true
          docker build --cache-from $IMAGE_LATEST --build-arg BUILDKIT_INLINE_CACHE=1 \
            -t $IMAGE_URI -t $IMAGE_LATEST .
          docker push $IMAGE_URI
          docker push $IMAGE_LATEST

      - name: Build and push Grafana image
        env:
          GRAFANA_IMAGE_URI: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com/trading-analysis-grafana:${{ github.sha }}
          GRAFANA_IMAGE_LATEST: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com/trading-analysis-grafana:latest
        run: |
          docker pull $GRAFANA_IMAGE_LATEST || true
          docker build --cache-from $GRAFANA_IMAGE_LATEST --build-arg BUILDKIT_INLINE_CACHE=1 \
            -f Dockerfile.grafana -t $GRAFANA_IMAGE_URI -t $GRAFANA_IMAGE_LATEST .
          docker push $GRAFANA_IMAGE_URI
          docker push $GRAFANA_IMAGE_LATEST

      - name: Export image URIs
        id: export
        env:
          IMAGE_URI: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com/trading-analysis-dagster:${{ github.sha }}
          GRAFANA_IMAGE_URI: ${{ secrets.AWS_ACCOUNT_ID }}.dkr.ecr.${{ secrets.AWS_REGION }}.amazonaws.com/trading-analysis-grafana:${{ github.sha }}
        run: |
          echo "image_uri=$IMAGE_URI" >> $GITHUB_OUTPUT
          echo "grafana_image_uri=$GRAFANA_IMAGE_URI" >> $GITHUB_OUTPUT
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add build job — ECR login, docker build+push, image URI outputs"
```

---

## Task 3: Add the schema deploy job to `ci-cd.yml`

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

The schema job fetches ClickHouse credentials from Secrets Manager at runtime (same pattern as `buildspec/schema.yml`) and applies `schemas/clickhouse_cloud.sql`.

- [ ] **Step 1: Append the schema job**

Add to `.github/workflows/ci-cd.yml` inside `jobs:`:

```yaml
  schema:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Install clickhouse-client
        run: |
          curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
            | gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
          echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" \
            | sudo tee /etc/apt/sources.list.d/clickhouse.list
          sudo apt-get update -q && sudo apt-get install -y -q clickhouse-client

      - name: Apply ClickHouse schema
        run: |
          CH_SECRET=$(aws secretsmanager get-secret-value \
            --secret-id trading-analysis/clickhouse \
            --region ${{ secrets.AWS_REGION }} \
            --query SecretString --output text)
          CLICKHOUSE_HOST=$(echo $CH_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['host'])")
          CLICKHOUSE_PASSWORD=$(echo $CH_SECRET | python3 -c "import sys,json; print(json.load(sys.stdin)['password'])")
          unset CH_SECRET
          clickhouse-client \
            --host "$CLICKHOUSE_HOST" \
            --port 9440 \
            --secure \
            --user default \
            --password "$CLICKHOUSE_PASSWORD" \
            --multiquery < schemas/clickhouse_cloud.sql
          echo "Schema applied."
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add schema job — fetch CH creds from Secrets Manager, apply SQL"
```

---

## Task 4: Add the deploy-dagster job to `ci-cd.yml`

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

Reads `IMAGE_URI` from the build job outputs, describes the current ECS task definition, patches the image for both `dagster-webserver` and `dagster-daemon` containers, registers a new task definition revision, updates the service, and waits for stability.

- [ ] **Step 1: Append the deploy-dagster job**

Add to `.github/workflows/ci-cd.yml` inside `jobs:`:

```yaml
  deploy-dagster:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Deploy Dagster to ECS
        env:
          IMAGE_URI: ${{ needs.build.outputs.image_uri }}
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
        run: |
          set -eu
          TASK_DEF=$(aws ecs describe-task-definition \
            --task-definition trading-analysis-dagster \
            --region $DEPLOY_REGION \
            --query taskDefinition \
            --output json)

          NEW_TASK=$(echo "$TASK_DEF" | python3 -c "
          import sys, json, os
          td = json.load(sys.stdin)
          img = os.environ['IMAGE_URI']
          for c in td['containerDefinitions']:
              if c['name'] in ('dagster-webserver', 'dagster-daemon'):
                  c['image'] = img
          for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy'):
              td.pop(k, None)
          print(json.dumps(td))
          ")

          NEW_ARN=$(aws ecs register-task-definition \
            --cli-input-json "$NEW_TASK" \
            --region $DEPLOY_REGION \
            --query taskDefinition.taskDefinitionArn \
            --output text)

          [ -n "$NEW_ARN" ] || { echo "ERROR: register-task-definition returned empty ARN"; exit 1; }
          echo "Registered: $NEW_ARN"

          aws ecs update-service \
            --cluster $ECS_CLUSTER \
            --service trading-analysis-dagster \
            --task-definition "$NEW_ARN" \
            --force-new-deployment \
            --region $DEPLOY_REGION \
            --output json > /dev/null

      - name: Wait for Dagster service stability
        env:
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
        run: |
          aws ecs wait services-stable \
            --cluster $ECS_CLUSTER \
            --services trading-analysis-dagster \
            --region $DEPLOY_REGION
          echo "Dagster deploy complete."
          aws ecs describe-services \
            --cluster $ECS_CLUSTER \
            --services trading-analysis-dagster \
            --region $DEPLOY_REGION \
            --query 'services[*].{Name:serviceName,Status:status,Running:runningCount,Desired:desiredCount}' \
            --output table
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add deploy-dagster job — patch ECS task def, update service, wait stable"
```

---

## Task 5: Add the deploy-grafana job to `ci-cd.yml`

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

Same pattern as deploy-dagster but uses `GRAFANA_IMAGE_URI` and patches only the `grafana` container.

- [ ] **Step 1: Append the deploy-grafana job**

Add to `.github/workflows/ci-cd.yml` inside `jobs:`:

```yaml
  deploy-grafana:
    runs-on: ubuntu-latest
    needs: build
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Deploy Grafana to ECS
        env:
          GRAFANA_IMAGE_URI: ${{ needs.build.outputs.grafana_image_uri }}
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
        run: |
          set -eu
          TASK_DEF=$(aws ecs describe-task-definition \
            --task-definition trading-analysis-grafana \
            --region $DEPLOY_REGION \
            --query taskDefinition \
            --output json)

          NEW_TASK=$(echo "$TASK_DEF" | python3 -c "
          import sys, json, os
          td = json.load(sys.stdin)
          img = os.environ['GRAFANA_IMAGE_URI']
          for c in td['containerDefinitions']:
              if c['name'] == 'grafana':
                  c['image'] = img
          for k in ('taskDefinitionArn','revision','status','requiresAttributes','compatibilities','registeredAt','registeredBy'):
              td.pop(k, None)
          print(json.dumps(td))
          ")

          NEW_ARN=$(aws ecs register-task-definition \
            --cli-input-json "$NEW_TASK" \
            --region $DEPLOY_REGION \
            --query taskDefinition.taskDefinitionArn \
            --output text)

          [ -n "$NEW_ARN" ] || { echo "ERROR: register-task-definition returned empty ARN"; exit 1; }
          echo "Registered: $NEW_ARN"

          aws ecs update-service \
            --cluster $ECS_CLUSTER \
            --service trading-analysis-grafana \
            --task-definition "$NEW_ARN" \
            --force-new-deployment \
            --region $DEPLOY_REGION \
            --output json > /dev/null

      - name: Wait for Grafana service stability
        env:
          DEPLOY_REGION: ${{ secrets.AWS_REGION }}
          ECS_CLUSTER: trading-analysis
        run: |
          aws ecs wait services-stable \
            --cluster $ECS_CLUSTER \
            --services trading-analysis-grafana \
            --region $DEPLOY_REGION
          echo "Grafana deploy complete."
          aws ecs describe-services \
            --cluster $ECS_CLUSTER \
            --services trading-analysis-grafana \
            --region $DEPLOY_REGION \
            --query 'services[*].{Name:serviceName,Status:status,Running:runningCount,Desired:desiredCount}' \
            --output table
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "ci: add deploy-grafana job — patch ECS task def, update service, wait stable"
```

---

## Task 6: Create `.github/workflows/terraform.yml`

**Files:**
- Create: `.github/workflows/terraform.yml`

Manual workflow only (`workflow_dispatch`). Runs `terraform plan` always; applies only if `auto_approve` is set to `true`.

- [ ] **Step 1: Create the terraform workflow**

Create `.github/workflows/terraform.yml`:

```yaml
name: Terraform

on:
  workflow_dispatch:
    inputs:
      auto_approve:
        description: "Run terraform apply (true) or plan only (false)"
        required: false
        default: "false"
        type: boolean

permissions:
  id-token: write
  contents: read

jobs:
  terraform:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: infra/terraform

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.GHA_ROLE_ARN }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Install Terraform 1.5.7
        run: |
          curl -sSL https://releases.hashicorp.com/terraform/1.5.7/terraform_1.5.7_linux_amd64.zip -o tf.zip
          unzip tf.zip && sudo mv terraform /usr/local/bin/ && rm tf.zip
          terraform version

      - name: Terraform Init
        run: terraform init -input=false

      - name: Terraform Plan
        run: |
          terraform plan \
            -input=false \
            -var="aws_region=${{ secrets.AWS_REGION }}" \
            -var="environment=default" \
            -var="github_repo=williamwxz/trading-analysis"

      - name: Terraform Apply
        if: ${{ inputs.auto_approve == true }}
        run: |
          terraform apply \
            -auto-approve \
            -input=false \
            -var="aws_region=${{ secrets.AWS_REGION }}" \
            -var="environment=default" \
            -var="github_repo=williamwxz/trading-analysis"
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/terraform.yml
git commit -m "ci: add manual terraform workflow with plan-only default"
```

---

## Task 7: Delete `infra/terraform/codepipeline.tf` and `buildspec/`

**Files:**
- Delete: `infra/terraform/codepipeline.tf`
- Delete: `buildspec/` (entire directory)

- [ ] **Step 1: Delete the files**

```bash
rm infra/terraform/codepipeline.tf
rm -rf buildspec/
```

- [ ] **Step 2: Verify terraform is still valid**

```bash
cd infra/terraform
terraform init -input=false
terraform validate
```

Expected output: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
cd ../..
git add -A
git commit -m "chore: remove CodePipeline/CodeBuild terraform and buildspec files"
```

---

## Task 8: Update `CLAUDE.md` with GitHub Actions CI/CD setup section

**Files:**
- Modify: `CLAUDE.md`

Add a new "GitHub Actions CI/CD" section documenting the one-time setup steps so any operator can reproduce the AWS configuration from scratch.

- [ ] **Step 1: Add the section to `CLAUDE.md`**

Find the existing `## CI/CD` section in `CLAUDE.md` and replace it with the following (keep any content that isn't about CodePipeline):

```markdown
## CI/CD

GitHub Actions drives all CI/CD. Workflows live in `.github/workflows/`.

| Workflow | Trigger | Stages |
|----------|---------|--------|
| `ci-cd.yml` | Push to `main` | test → build → schema + deploy-dagster + deploy-grafana (parallel) |
| `terraform.yml` | Manual (`workflow_dispatch`) | plan (always) → apply (if `auto_approve=true`) |

AWS auth uses GitHub OIDC — no static credentials stored in GitHub. Each job assumes `trading-analysis-github-actions` IAM role via `aws-actions/configure-aws-credentials@v4`.

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
        "Federated": "arn:aws:iam::339163283253:oidc-provider/token.actions.githubusercontent.com"
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
        "arn:aws:ecr:ap-northeast-1:339163283253:repository/trading-analysis-dagster",
        "arn:aws:ecr:ap-northeast-1:339163283253:repository/trading-analysis-grafana"
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
        "arn:aws:secretsmanager:ap-northeast-1:339163283253:secret:trading-analysis/clickhouse*"
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
        "s3:*", "ecr:*", "logs:*"
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
gh secret set AWS_ACCOUNT_ID --body "339163283253" --repo williamwxz/trading-analysis
gh secret set AWS_REGION --body "ap-northeast-1" --repo williamwxz/trading-analysis
gh secret set GHA_ROLE_ARN --body "arn:aws:iam::339163283253:role/trading-analysis-github-actions" --repo williamwxz/trading-analysis
```

### Destroying CodePipeline infrastructure

After the GitHub Actions workflows are confirmed working, run the terraform workflow manually with `auto_approve=true` to destroy the old CodePipeline/CodeBuild resources (already removed from Terraform state via `codepipeline.tf` deletion).
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md with GitHub Actions CI/CD setup and one-time AWS steps"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|-----------------|-----------|
| OIDC provider creation (AWS CLI) | Task 8 CLAUDE.md docs |
| IAM role creation (AWS CLI) | Task 8 CLAUDE.md docs |
| GitHub secrets (gh CLI) | Task 8 CLAUDE.md docs |
| `ci-cd.yml` test job | Task 1 |
| `ci-cd.yml` build job with ECR push + outputs | Task 2 |
| `ci-cd.yml` schema job (Secrets Manager → clickhouse-client) | Task 3 |
| `ci-cd.yml` deploy-dagster job | Task 4 |
| `ci-cd.yml` deploy-grafana job | Task 5 |
| `terraform.yml` manual workflow_dispatch | Task 6 |
| Delete `codepipeline.tf` | Task 7 |
| Delete `buildspec/` | Task 7 |
| `terraform validate` after deletion | Task 7 |
| Update CLAUDE.md | Task 8 |

All spec requirements covered. No gaps.
