# AWS Account Migration Design
**Date:** 2026-04-26  
**Old account:** 339163283253  
**New account:** 068704208855  
**Region:** ap-northeast-1 (Tokyo) — unchanged

## Overview

Hard cutover from old AWS account (already deleted) to new AWS account. All AWS-hosted infrastructure moves to the new account. External services (ClickHouse Cloud, Supabase Postgres, Grafana Cloud) are unchanged.

## What Moves

| Resource | Type | Notes |
|----------|------|-------|
| VPC + subnets + IGW + route tables | Terraform | Recreated |
| NAT instance (fck-nat t4g.nano) + EIP | Terraform | New EIP — update ClickHouse allowlist |
| ECS cluster `trading-analysis` | Terraform | Recreated |
| ECS service + task definition (Dagster) | Terraform + CI/CD | Image re-pushed from scratch |
| ECR repo `trading-analysis-dagster` | Terraform | Recreated, CI/CD re-pushes image |
| ~~S3 bucket `trading-analysis-data-v2`~~ | Removed | Not used by any active asset — omitted from new account |
| S3 bucket `trading-analysis-tfstate` | Manual bootstrap | Created before Terraform runs |
| Secrets Manager (clickhouse, supabase) | Terraform shell + manual values | Terraform creates shell; values entered manually |
| Secrets Manager (grafana-cloudwatch) | Terraform | Auto-populated by Terraform (IAM key rotation) |
| IAM roles (ECS execution, task, NAT instance) | Terraform | Recreated |
| IAM user (grafana-cloudwatch) + access key | Terraform | New key auto-stored in Secrets Manager |
| GitHub OIDC provider | Manual bootstrap | Created before Terraform runs |
| GitHub Actions IAM role | Manual bootstrap | Created before Terraform runs |
| CloudWatch log group | Terraform | Recreated |

## What Stays External (No Change)

- **ClickHouse Cloud** — only the allowlisted IP changes (new EIP)
- **Supabase Postgres** — connection string unchanged; secret value re-entered manually
- **Grafana Cloud** — dashboards and datasource config unchanged; CloudWatch datasource gets new IAM key from Terraform

## Code Changes

### `infra/terraform/main.tf`
Two changes:
1. **Account ID** — the GitHub OIDC trust policy Federated ARN hardcodes the account ID (one line).
2. **Remove S3 data bucket** — delete `aws_s3_bucket.data`, `aws_s3_bucket_versioning.data`, `aws_s3_bucket_lifecycle_configuration.data`, the S3 permissions block in `aws_iam_role_policy.ecs_task_policy`, and the `s3_bucket` output. The bucket is unused by any active asset.

```hcl
# Before
"Federated": "arn:aws:iam::339163283253:oidc-provider/token.actions.githubusercontent.com"

# After
"Federated": "arn:aws:iam::068704208855:oidc-provider/token.actions.githubusercontent.com"
```

### `CLAUDE.md`
Update the one-time setup section: replace `339163283253` with `068704208855` in the OIDC provider ARN and the IAM role trust policy example.

### GitHub Secrets
Two secrets change:
- `AWS_ACCOUNT_ID`: `339163283253` → `068704208855`
- `GHA_ROLE_ARN`: `arn:aws:iam::339163283253:role/trading-analysis-github-actions` → `arn:aws:iam::068704208855:role/trading-analysis-github-actions`

`AWS_REGION`, `GRAFANA_CLOUD_TOKEN`, `GRAFANA_CLOUD_URL` are unchanged.

## Migration Phases

### Phase 1 — Bootstrap (local, SSO credentials)

Run once. These resources must exist before Terraform can run.

```bash
# 1a. S3 tfstate bucket
aws s3api create-bucket \
  --bucket trading-analysis-tfstate \
  --region ap-northeast-1 \
  --create-bucket-configuration LocationConstraint=ap-northeast-1

aws s3api put-bucket-versioning \
  --bucket trading-analysis-tfstate \
  --versioning-configuration Status=Enabled

# 1b. GitHub OIDC identity provider
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

# 1c. IAM role trust policy (save as /tmp/gha-trust-policy.json)
# For the permissions policy (/tmp/gha-permissions.json), use the same policy
# documented in CLAUDE.md "One-Time AWS Setup" section, with 339163283253
# replaced by 068704208855 in any Resource ARNs.
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
  --assume-role-policy-document file:///tmp/gha-trust-policy.json

# Attach the same inline policy as before (from CLAUDE.md)
# (update Resource ARNs to use 068704208855)
aws iam put-role-policy \
  --role-name trading-analysis-github-actions \
  --policy-name github-actions-policy \
  --policy-document file:///tmp/gha-permissions.json
```

### Phase 2 — Code Changes + Terraform

1. Update `main.tf`: change OIDC Federated ARN account ID (`339163283253` → `068704208855`)
2. Run locally:
   ```bash
   cd infra/terraform
   terraform init -reconfigure  # fresh state in new account's S3 bucket
   terraform apply
   ```
3. Note the outputs: `nat_static_ip` (new EIP), `ecr_repository_url`

### Phase 3 — Populate Secrets Manager

Terraform creates the secret shells. Manually set the values:

```bash
# ClickHouse credentials
aws secretsmanager put-secret-value \
  --secret-id trading-analysis/clickhouse \
  --region ap-northeast-1 \
  --secret-string '{"host":"YOUR_CLICKHOUSE_HOST","password":"YOUR_CLICKHOUSE_PASSWORD"}'

# Supabase Postgres credentials
aws secretsmanager put-secret-value \
  --secret-id trading-analysis/supabase \
  --region ap-northeast-1 \
  --secret-string '{"host":"YOUR_SUPABASE_HOST","user":"YOUR_SUPABASE_USER","password":"YOUR_SUPABASE_PASSWORD"}'
# Keys must match what the ECS task definition reads:
# clickhouse: host, password
# supabase: host, user, password
```

The `trading-analysis/grafana-cloudwatch` secret is auto-populated by Terraform with the new IAM access key.

### Phase 4 — GitHub Secrets

```bash
gh secret set AWS_ACCOUNT_ID --body "068704208855" --repo williamwxz/trading-analysis
gh secret set GHA_ROLE_ARN \
  --body "arn:aws:iam::068704208855:role/trading-analysis-github-actions" \
  --repo williamwxz/trading-analysis
```

### Phase 5 — ClickHouse Allowlist + First Deploy

1. Get the new EIP: `terraform output nat_static_ip`
2. Add the new IP to the ClickHouse Cloud allowlist (and remove the old IP if it's still present)
3. Trigger CI/CD: push to `main` (or `git commit --allow-empty -m "chore: trigger deploy to new account"`)
4. CI/CD will: build image → push to new ECR → deploy to new ECS service
5. Verify Dagster UI is accessible at `http://<new-nat-ip>:3000`

## Rollback

Old account is already deleted — there is no rollback target. The migration is one-way. Proceed carefully through each phase before moving to the next.

## Success Criteria

- [ ] `terraform apply` completes cleanly with no errors
- [ ] Secrets Manager values populated for clickhouse and supabase
- [ ] GitHub Actions CI/CD pipeline passes (test → build → deploy-dagster → terraform → deploy-grafana-cloud)
- [ ] Dagster UI accessible at new NAT IP on port 3000
- [ ] Dagster daemon shows assets materializing (PnL assets running on schedule)
- [ ] ClickHouse queries succeed from ECS tasks (new EIP is allowlisted)
- [ ] Grafana Cloud dashboards still showing data
