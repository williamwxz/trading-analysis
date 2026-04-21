# Grafana Cloud Migration Design

**Date:** 2026-04-17  
**Status:** Approved

## Summary

Migrate Grafana from self-hosted ECS Fargate to Grafana Cloud (SaaS). Dashboards remain as code in `infra/grafana/dashboards/` and are pushed to Grafana Cloud via HTTP API on every push to `main`. The ClickHouse datasource is configured once manually in the Grafana Cloud UI. The ECS Grafana service, ECR repository, and Docker image build are removed.

## Approach

Option A: Grafana Cloud API + GitHub Actions. No new tooling beyond `curl` in CI. Dashboard-as-code is preserved. Datasource config is a one-time manual UI step.

## GitHub Actions Changes (`ci-cd.yml`)

### Remove from `build` job

- Delete the `Build and push Grafana image` step (`id: build_grafana`)
- Remove `grafana_image_uri` from the `outputs` block of the `build` job

### Remove entirely

- Delete the `deploy-grafana` job

### Add new job: `deploy-grafana-cloud`

- Trigger: runs on push to `main`, no dependency on `build` (does not need a Docker image)
- For each `*.json` file in `infra/grafana/dashboards/`, POST to the Grafana Cloud API:

```bash
curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GRAFANA_CLOUD_TOKEN" \
  "$GRAFANA_CLOUD_URL/api/dashboards/db" \
  -d "{\"dashboard\": $(cat "$file"), \"overwrite\": true, \"folderId\": 0}"
```

- Secrets required:
  - `GRAFANA_CLOUD_TOKEN` — Grafana Cloud service account token (Editor role)
  - `GRAFANA_CLOUD_URL` — Grafana Cloud stack URL (e.g. `https://yourorg.grafana.net`)

## One-Time Manual Setup (Grafana Cloud UI)

Do these steps before the first CI run:

1. **Create account and stack** — grafana.com → free tier is sufficient
2. **Install ClickHouse plugin** — Administration → Plugins → search "ClickHouse" → Install
3. **Configure datasource** — Connections → Data sources → Add → ClickHouse
   - Host: value from `trading-analysis/clickhouse` secret (`host` key)
   - User: `dev_ro3`
   - Password: value from `trading-analysis/clickhouse` secret (`password` key)
   - Port: 8443, TLS enabled
4. **Create service account token** — Administration → Service accounts → Add → role: Editor → Add token → copy value
5. **Add GitHub Secrets** on `williamwxz/trading-analysis`:
   - `GRAFANA_CLOUD_TOKEN` — the token from step 4
   - `GRAFANA_CLOUD_URL` — your stack URL (e.g. `https://yourorg.grafana.net`)

## Infrastructure Cleanup (Terraform `main.tf`)

Remove the following resources:

- `aws_ecs_service.grafana`
- `aws_ecs_task_definition.grafana`
- `aws_ecr_repository.grafana`
- `aws_lb_target_group.grafana`
- `aws_lb_listener_rule.grafana`
- `aws_cloudwatch_log_group.grafana`

Apply via the manual `terraform.yml` workflow (`auto_approve=true`).

## Files to Delete

- `Dockerfile.grafana`

## Files Unchanged

- `infra/grafana/dashboards/*.json` — dashboard JSON stays as-is, now pushed via API instead of baked into Docker image
- `infra/grafana/provisioning/` — no longer used (provisioning was file-based, only needed for self-hosted), can be deleted as cleanup

## Auth

Grafana Cloud default: login required. Users managed via Grafana Cloud UI. No anonymous access.

## Risks

- **Dashboard UI edits get overwritten** — if someone edits a dashboard in Grafana Cloud UI, the next `main` push will overwrite it with the JSON from the repo. This is intentional (dashboards-as-code), but the team should know to edit the JSON files, not the UI.
- **Datasource UID must match** — dashboard JSON files reference the datasource by `uid: "clickhouse"`. When creating the datasource in Grafana Cloud UI, you must set the UID to `clickhouse` (under "Advanced settings" when adding the datasource). If the UID doesn't match, all panels will show "datasource not found". The display name should also be `ClickHouse` to match the provisioning config.
