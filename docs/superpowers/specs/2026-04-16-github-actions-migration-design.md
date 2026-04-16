# Design: Migrate CI/CD from CodePipeline to GitHub Actions

**Date:** 2026-04-16  
**Status:** Approved

## Overview

Replace AWS CodePipeline + CodeBuild with GitHub Actions. All build/test/deploy logic moves to GitHub Actions runners. Terraform provisioning becomes a manually triggered workflow. AWS authentication uses GitHub OIDC (no static AWS keys).

## Part 1 — One-Time AWS Setup (manual, done once)

### Step 1: Create GitHub OIDC Identity Provider

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1 \
  --region ap-northeast-1
```

### Step 2: Create IAM Role `trading-analysis-github-actions`

Create the trust policy file:

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
```

Create the role:

```bash
aws iam create-role \
  --role-name trading-analysis-github-actions \
  --assume-role-policy-document file:///tmp/gha-trust-policy.json \
  --region ap-northeast-1
```

Create and attach the permissions policy:

```bash
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

### Step 3: Add GitHub Secrets (gh CLI)

```bash
gh secret set AWS_ACCOUNT_ID --body "339163283253" --repo williamwxz/trading-analysis
gh secret set AWS_REGION --body "ap-northeast-1" --repo williamwxz/trading-analysis
gh secret set GHA_ROLE_ARN --body "arn:aws:iam::339163283253:role/trading-analysis-github-actions" --repo williamwxz/trading-analysis
```

## Part 2 — GitHub Actions Workflows

### `.github/workflows/ci-cd.yml`

Triggers on push to `main`. Jobs:

```
test → build → schema      (parallel)
             → deploy-dagster  (parallel)
             → deploy-grafana  (parallel)
```

- **test**: `ubuntu-latest`, Python 3.11, `pip install -e ".[dev]"`, `pytest tests/ -v --tb=short`
- **build**: ECR login, `docker build` both images (Dockerfile + Dockerfile.grafana), push with `${{ github.sha }}` and `:latest` tags. Outputs `IMAGE_URI` and `GRAFANA_IMAGE_URI` for downstream jobs.
- **schema**: fetches ClickHouse credentials from Secrets Manager, installs `clickhouse-client`, applies `schemas/clickhouse_cloud.sql`
- **deploy-dagster**: reads `IMAGE_URI` from build outputs, describes current ECS task definition, patches image field for `dagster-webserver` and `dagster-daemon` containers, registers new task definition, calls `ecs update-service --force-new-deployment`, waits for service stability
- **deploy-grafana**: same pattern using `GRAFANA_IMAGE_URI` for the `grafana` container

All jobs assume the `GHA_ROLE_ARN` via `aws-actions/configure-aws-credentials` with OIDC.

### `.github/workflows/terraform.yml`

Triggers on `workflow_dispatch` only. Input: `auto_approve` (boolean, default `false`).

- Installs Terraform 1.5.7
- Runs `terraform init` + `terraform plan`
- If `auto_approve` is `true`: runs `terraform apply -auto-approve`
- If `auto_approve` is `false`: prints plan output and stops

## Part 3 — Terraform Cleanup

- **Delete** `infra/terraform/codepipeline.tf` — removes CodePipeline, all 6 CodeBuild projects, their IAM roles, S3 artifact bucket, and CloudWatch log groups
- **Check** `main.tf` for any references to `data.aws_caller_identity.current` — remove if only used in `codepipeline.tf`
- **Delete** `buildspec/` directory — all 6 buildspec files become dead code

Running `terraform apply` after this cleanup (via the new manual workflow) will destroy the CodePipeline infrastructure.

## Part 4 — CLAUDE.md Update

Add a "GitHub Actions CI/CD" section with:
- OIDC provider creation command
- IAM role creation commands (trust policy + permissions)
- `gh secret set` commands for the 3 secrets
- Workflow trigger summary table

## Key Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| AWS auth | OIDC (no static keys) | No credentials to rotate or leak |
| Secrets at runtime | Fetched from Secrets Manager | Single source of truth, same as current schema.yml pattern |
| Terraform trigger | Manual `workflow_dispatch` | Not every code push should reprovision infrastructure |
| Workflow structure | Single `ci-cd.yml` | Simpler; all stages visible in one file |
| Deploy parallelism | schema + dagster + grafana run in parallel | Matches current CodePipeline `run_order: 2` behavior |
