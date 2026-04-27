# AWS Account Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate all AWS infrastructure from account `339163283253` (deleted) to new account `068704208855`, restoring full Dagster + CI/CD operation.

**Architecture:** Manual bootstrap creates S3 tfstate bucket + GitHub OIDC provider + IAM role, then Terraform recreates all remaining resources. Secrets Manager values are populated manually. CI/CD is re-pointed via GitHub secrets, then a push triggers the first deploy.

**Tech Stack:** AWS CLI (SSO), Terraform 1.5+, GitHub CLI (`gh`), Bash

---

## Files Changed

| File | Change |
|------|--------|
| `infra/terraform/main.tf` | Update OIDC account ID; remove unused S3 data bucket resources |
| `CLAUDE.md` | Update account ID in one-time setup section |

---

### Task 1: Bootstrap — S3 Terraform State Bucket

**Files:** none (AWS CLI only)

This must run before `terraform init`. The bucket name is the same as the old account.

- [ ] **Step 1: Verify SSO credentials target the new account**

```bash
aws sts get-caller-identity
```

Expected output contains `"Account": "068704208855"`. If not, run `aws sso login` with the correct profile, or set `AWS_PROFILE` to the new account's profile.

- [ ] **Step 2: Create the S3 tfstate bucket**

```bash
aws s3api create-bucket \
  --bucket trading-analysis-tfstate \
  --region ap-northeast-1 \
  --create-bucket-configuration LocationConstraint=ap-northeast-1
```

Expected: `{"Location": "http://trading-analysis-tfstate.s3.amazonaws.com/"}`

- [ ] **Step 3: Enable versioning on the bucket**

```bash
aws s3api put-bucket-versioning \
  --bucket trading-analysis-tfstate \
  --versioning-configuration Status=Enabled
```

Expected: no output (exit code 0).

- [ ] **Step 4: Verify bucket exists and versioning is on**

```bash
aws s3api get-bucket-versioning --bucket trading-analysis-tfstate
```

Expected: `{"Status": "Enabled"}`

---

### Task 2: Bootstrap — GitHub OIDC Identity Provider

**Files:** none (AWS CLI only)

This allows GitHub Actions to assume AWS roles without static credentials.

- [ ] **Step 1: Create the OIDC provider**

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

Expected: `{"OpenIDConnectProviderArn": "arn:aws:iam::068704208855:oidc-provider/token.actions.githubusercontent.com"}`

- [ ] **Step 2: Verify the provider was created**

```bash
aws iam list-open-id-connect-providers
```

Expected: output contains `arn:aws:iam::068704208855:oidc-provider/token.actions.githubusercontent.com`

---

### Task 3: Bootstrap — GitHub Actions IAM Role

**Files:** none (AWS CLI only)

This role is assumed by GitHub Actions via OIDC. It must exist before Terraform runs CI/CD jobs.

- [ ] **Step 1: Write the trust policy**

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
```

Expected: file created, no output.

- [ ] **Step 2: Create the IAM role**

```bash
aws iam create-role \
  --role-name trading-analysis-github-actions \
  --assume-role-policy-document file:///tmp/gha-trust-policy.json
```

Expected: JSON output with `"RoleName": "trading-analysis-github-actions"` and `"Arn": "arn:aws:iam::068704208855:role/trading-analysis-github-actions"`

- [ ] **Step 3: Write the permissions policy**

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
        "arn:aws:secretsmanager:ap-northeast-1:068704208855:secret:trading-analysis/clickhouse*",
        "arn:aws:secretsmanager:ap-northeast-1:068704208855:secret:trading-analysis/grafana-cloudwatch*"
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
```

Expected: file created, no output.

- [ ] **Step 4: Attach the permissions policy to the role**

```bash
aws iam put-role-policy \
  --role-name trading-analysis-github-actions \
  --policy-name github-actions-policy \
  --policy-document file:///tmp/gha-permissions.json
```

Expected: no output (exit code 0).

- [ ] **Step 5: Verify the role and policy**

```bash
aws iam get-role --role-name trading-analysis-github-actions --query 'Role.Arn'
aws iam get-role-policy --role-name trading-analysis-github-actions --policy-name github-actions-policy
```

Expected: first command returns `"arn:aws:iam::068704208855:role/trading-analysis-github-actions"`, second returns the policy JSON.

---

### Task 4: Update `main.tf` — Account ID + Remove S3 Data Bucket

**Files:**
- Modify: `infra/terraform/main.tf`

- [ ] **Step 1: Confirm no hardcoded account ID in main.tf**

The GitHub Actions IAM role is created by manual bootstrap (Task 3), not by Terraform, so `main.tf` has no hardcoded account ID. Verify:

```bash
grep -n "339163283253" infra/terraform/main.tf
```

Expected: no output (zero matches). If a match appears, replace each `339163283253` with `068704208855` before continuing.

- [ ] **Step 2: Remove the S3 data bucket — `aws_s3_bucket.data`**

Delete these three resource blocks from `infra/terraform/main.tf`:

```hcl
resource "aws_s3_bucket" "data" {
  bucket = "${local.name_prefix}-data-v2"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }
}
```

- [ ] **Step 3: Remove the S3 permissions block from `aws_iam_role_policy.ecs_task_policy`**

In the `aws_iam_role_policy` resource named `ecs_task_policy`, remove this Statement block:

```hcl
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
```

- [ ] **Step 4: Remove the `s3_bucket` output**

Delete this output block:

```hcl
output "s3_bucket" {
  value = aws_s3_bucket.data.id
}
```

- [ ] **Step 5: Commit the Terraform changes**

```bash
git add infra/terraform/main.tf
git commit -m "feat: remove unused S3 data bucket, update account ID for new AWS account"
```

---

### Task 5: Run Terraform Against New Account

**Files:** none (Terraform CLI only, run from `infra/terraform/`)

- [ ] **Step 1: Reconfigure Terraform backend to new account's S3 bucket**

```bash
cd infra/terraform
terraform init -reconfigure
```

Expected: output ends with `"Terraform has been successfully initialized!"`. The backend will connect to `s3://trading-analysis-tfstate` in account `068704208855`.

- [ ] **Step 2: Preview the plan**

```bash
terraform plan \
  -var="aws_region=ap-northeast-1" \
  -var="environment=default" \
  -var="github_repo=williamwxz/trading-analysis"
```

Expected: plan shows resources to create (VPC, subnets, IGW, NAT instance, EIP, ECS cluster, ECR, Secrets Manager secrets, IAM roles, CloudWatch log group). No resources to destroy (fresh state). Review for any unexpected changes.

- [ ] **Step 3: Apply**

```bash
terraform apply \
  -auto-approve \
  -var="aws_region=ap-northeast-1" \
  -var="environment=default" \
  -var="github_repo=williamwxz/trading-analysis"
```

Expected: `Apply complete! Resources: N added, 0 changed, 0 destroyed.`

- [ ] **Step 4: Note the outputs**

```bash
terraform output nat_static_ip
terraform output ecr_repository_url
terraform output dagster_url
```

Save these values — you'll need `nat_static_ip` for the ClickHouse allowlist in Task 7, and `ecr_repository_url` is the new ECR endpoint.

---

### Task 6: Populate Secrets Manager

**Files:** none (AWS CLI only)

Terraform created the secret shells. Now set the actual values. Replace the placeholder values with your real credentials.

- [ ] **Step 1: Set ClickHouse credentials**

```bash
aws secretsmanager put-secret-value \
  --secret-id trading-analysis/clickhouse \
  --region ap-northeast-1 \
  --secret-string '{"host":"YOUR_CLICKHOUSE_HOST","password":"YOUR_CLICKHOUSE_PASSWORD"}'
```

Expected: JSON output with `"ARN"` and `"Name": "trading-analysis/clickhouse"`.

The ECS task reads `host` and `password` keys from this secret (see `main.tf` secrets block — `CLICKHOUSE_HOST` from `:host::`, `CLICKHOUSE_PASSWORD` from `:password::`).

- [ ] **Step 2: Set Supabase Postgres credentials**

```bash
aws secretsmanager put-secret-value \
  --secret-id trading-analysis/supabase \
  --region ap-northeast-1 \
  --secret-string '{"host":"YOUR_SUPABASE_HOST","user":"YOUR_SUPABASE_USER","password":"YOUR_SUPABASE_PASSWORD"}'
```

Expected: JSON output with `"Name": "trading-analysis/supabase"`.

The ECS task reads `host`, `user`, and `password` keys from this secret (`DAGSTER_PG_HOST`, `DAGSTER_PG_USER`, `DAGSTER_PG_PASSWORD`).

- [ ] **Step 3: Verify both secrets have values**

```bash
aws secretsmanager get-secret-value \
  --secret-id trading-analysis/clickhouse \
  --region ap-northeast-1 \
  --query SecretString --output text

aws secretsmanager get-secret-value \
  --secret-id trading-analysis/supabase \
  --region ap-northeast-1 \
  --query SecretString --output text
```

Expected: each returns a JSON string with the keys you set. Confirm `host`, `password` (clickhouse) and `host`, `user`, `password` (supabase) are all present and non-empty.

Note: `trading-analysis/grafana-cloudwatch` was auto-populated by Terraform with the new IAM access key — no manual action needed.

---

### Task 7: Update ClickHouse Cloud Allowlist

**Files:** none (ClickHouse Cloud console)

The NAT instance EIP is the static outbound IP for all ECS traffic to ClickHouse. The new account has a new EIP.

- [ ] **Step 1: Get the new EIP**

```bash
cd infra/terraform
terraform output nat_static_ip
```

Note this IP address.

- [ ] **Step 2: Add the new IP to ClickHouse Cloud allowlist**

Log in to ClickHouse Cloud → your service → Security → Allowed IP Addresses. Add the new EIP (e.g. `1.2.3.4/32`). Save.

- [ ] **Step 3: (Optional) Remove the old IP**

The old account is deleted so the old EIP is released, but if the old IP is still in the allowlist, remove it to keep the allowlist clean.

---

### Task 8: Update GitHub Secrets

**Files:** none (GitHub CLI)

CI/CD workflows read `AWS_ACCOUNT_ID` and `GHA_ROLE_ARN` from GitHub secrets. These must point to the new account before the first CI run.

- [ ] **Step 1: Update `AWS_ACCOUNT_ID`**

```bash
gh secret set AWS_ACCOUNT_ID --body "068704208855" --repo williamwxz/trading-analysis
```

Expected: `✓ Set secret AWS_ACCOUNT_ID for williamwxz/trading-analysis`

- [ ] **Step 2: Update `GHA_ROLE_ARN`**

```bash
gh secret set GHA_ROLE_ARN \
  --body "arn:aws:iam::068704208855:role/trading-analysis-github-actions" \
  --repo williamwxz/trading-analysis
```

Expected: `✓ Set secret GHA_ROLE_ARN for williamwxz/trading-analysis`

- [ ] **Step 3: Verify secrets are set (names only — values are masked)**

```bash
gh secret list --repo williamwxz/trading-analysis
```

Expected: table shows `AWS_ACCOUNT_ID`, `GHA_ROLE_ARN`, `AWS_REGION`, `GRAFANA_CLOUD_TOKEN`, `GRAFANA_CLOUD_URL` all present with recent `Updated` timestamps.

---

### Task 9: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Replace old account ID in one-time setup section**

In `CLAUDE.md`, find all occurrences of `339163283253` in the "One-Time AWS Setup" section and replace with `068704208855`. There are three occurrences:
1. The OIDC provider ARN in Step 1
2. The Federated ARN in the trust policy JSON in Step 2
3. The `aws iam create-role` command output in Step 2
4. The `gh secret set AWS_ACCOUNT_ID` value in Step 3
5. The `gh secret set GHA_ROLE_ARN` ARN value in Step 3

```bash
grep -n "339163283253" CLAUDE.md
```

Use Edit to replace each occurrence with `068704208855`.

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update AWS account ID to 068704208855 in one-time setup"
```

---

### Task 10: Trigger First Deploy + Verify

**Files:** none (git + browser)

- [ ] **Step 1: Push to trigger CI/CD**

```bash
git push origin main
```

Or if no changes to push:

```bash
git commit --allow-empty -m "chore: trigger first deploy to new AWS account 068704208855"
git push origin main
```

- [ ] **Step 2: Watch the CI/CD pipeline**

```bash
gh run watch --repo williamwxz/trading-analysis
```

Or open GitHub → Actions tab. The pipeline runs: `test → build + terraform (parallel) → deploy-dagster + deploy-grafana-cloud (parallel)`.

Expected: all jobs green. If `terraform` job fails, it will be because resources already exist from your local apply — this is expected and safe (Terraform will show 0 changes).

- [ ] **Step 3: Verify Dagster UI is accessible**

```bash
cd infra/terraform
terraform output dagster_url
```

Open the URL in a browser. Expected: Dagster web UI loads, showing the asset graph with `pnl_prod_v2_live`, `pnl_1hour_rollup`, etc.

- [ ] **Step 4: Verify the daemon is running**

In the Dagster UI → Deployment → Daemons. All daemons (Scheduler, Sensor, Run Monitoring) should show green / running.

- [ ] **Step 5: Verify a live asset materializes**

Wait up to 5 minutes for `pnl_prod_v2_live` or `binance_futures_ohlcv_minutely` to trigger (they run on cron schedules). In Dagster UI → Assets, check for a recent successful materialization. If ClickHouse is unreachable, the run will fail with a connection error — go back to Task 7 and confirm the EIP is allowlisted.

- [ ] **Step 6: Verify Grafana Cloud dashboards**

Open your Grafana Cloud stack. The strategy PnL dashboards should still load data from ClickHouse (which is unchanged). The AWS Cost overview dashboard will show no data initially (new account, no billing history) — this is expected.

---

## Success Criteria Checklist

- [ ] `aws sts get-caller-identity` returns account `068704208855`
- [ ] `terraform apply` completed with 0 errors
- [ ] Secrets Manager has values for `clickhouse` and `supabase`
- [ ] New EIP is in ClickHouse Cloud allowlist
- [ ] GitHub secrets `AWS_ACCOUNT_ID` and `GHA_ROLE_ARN` updated
- [ ] CI/CD pipeline passes all jobs on first push
- [ ] Dagster UI accessible at new NAT IP
- [ ] At least one asset materializes successfully after deploy
- [ ] Grafana Cloud PnL dashboards show data
