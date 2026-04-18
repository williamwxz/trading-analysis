---
name: AWS Cost Reduction — Option A
description: Remove NAT Gateway, right-size Dagster task, replace RDS with SQLite on EFS, add backfill skip logic
type: project
---

# AWS Cost Reduction — Option A

**Date:** 2026-04-18  
**Goal:** Reduce AWS monthly spend from ~$365/mo to ~$50/mo (~85% reduction) with no change to pipeline reliability or data correctness.

## Cost Breakdown: Before vs After

| Resource | Before | After |
|---|---|---|
| NAT Gateway + Elastic IP | ~$35/mo + data | $0 (removed) |
| ALB | ~$20/mo | ~$20/mo (kept) |
| RDS db.t4g.micro | ~$15/mo | $0 (removed) |
| EFS (SQLite) | $0 | ~$0.30/mo |
| ECS Dagster (8 vCPU/16 GB) | ~$280/mo | ~$35/mo (1 vCPU/2 GB) |
| ECS Grafana (0.5 vCPU/1 GB) | ~$15/mo | ~$15/mo (unchanged) |
| **Total** | **~$365/mo** | **~$70/mo** |

## Change 1: Remove NAT Gateway

**Problem:** NAT Gateway costs ~$35/mo + $0.045/GB data processing. It exists solely to give private-subnet ECS tasks outbound internet access.

**Solution:** Move both ECS services (`dagster`, `grafana`) from private subnets to public subnets with `assign_public_ip = true`. The Internet Gateway already provides outbound internet. ECS tasks get ephemeral public IPs at no cost.

**Security:** Unchanged. The ECS security group (`aws_security_group.ecs_tasks`) only allows inbound on port 3000 from the ALB security group. The public IP is unreachable from the internet because no inbound rule allows direct access.

**Terraform changes:**
- Remove `aws_eip.nat`
- Remove `aws_nat_gateway.main`
- Remove `aws_route_table.private` and `aws_route_table_association.private`
- Remove `aws_subnet.private` (2 subnets, no longer needed)
- Remove `aws_db_subnet_group.main` (depends on private subnets — also removed with RDS)
- Update `aws_ecs_service.dagster` and `aws_ecs_service.grafana`: `subnets = aws_subnet.public[*].id`, `assign_public_ip = true`
- Remove output `nat_static_ip`

## Change 2: Right-size Dagster Task

**Problem:** Task is provisioned at 8 vCPU / 16 GB. Actual workload is:
- Binance REST polling every 5 min (network-bound, <100 MB RAM)
- Python PnL computation in 7-day chunks (single-threaded, <1 GB RAM)
- ClickHouse queries (network-bound)

**Solution:** Reduce to 1 vCPU / 2 GB. Fargate Spot pricing drops from ~$280/mo to ~$35/mo.

**Terraform changes:**
- `aws_ecs_task_definition.dagster`: `cpu = 1024`, `memory = 2048`

**No code changes needed.** The `_CHUNK_DAYS = 7` pattern already caps memory per computation run.

## Change 3: Replace RDS with SQLite on EFS

**Problem:** RDS db.t4g.micro costs ~$15/mo for a Postgres instance used solely as Dagster's metadata store. Single-node Dagster does not need Postgres.

**Solution:** Use SQLite backed by EFS (Elastic File System). EFS is a managed NFS volume — persistent across container restarts, replicated within the AZ. Dagster officially supports SQLite for single-node deployments. EFS costs ~$0.30/GB-month; Dagster metadata stays well under 1 GB.

**Data migration:** None. Dagster run history is observability metadata only — not trading data. Fresh start is acceptable.

**Dagster configuration:** Dagster defaults to SQLite when no `[storage]` Postgres block is present in `dagster.yaml`. Removing the `DAGSTER_PG_*` env vars and secret references is sufficient.

**Terraform changes:**
- Remove `aws_db_instance.dagster`
- Remove `aws_db_subnet_group.main`
- Remove `aws_security_group.db`
- Remove `aws_secretsmanager_secret.dagster_pg` and `aws_secretsmanager_secret_version.dagster_pg`
- Remove `random_password.db_password`
- Add `aws_efs_file_system.dagster` (encrypted, with lifecycle policy to IA after 30 days)
- Add `aws_efs_mount_target.dagster` (one per public subnet AZ, requires NFS port 2049 from ECS SG)
- Add `aws_efs_access_point.dagster` (path `/dagster-home`, uid/gid 1000)
- Add `aws_security_group.efs` allowing inbound 2049 from `aws_security_group.ecs_tasks`
- Update `aws_ecs_task_definition.dagster`: add `volumes` block (EFS volume) and `mountPoints` pointing to `/app` (matches `DAGSTER_HOME`)
- Remove `DAGSTER_PG_*` env vars and `DAGSTER_PG_PASSWORD` secret from both container definitions
- Remove `aws_iam_role_policy` for secrets access to `dagster_pg` secret (or narrow the resource list)
- Add `elasticfilesystem:ClientMount`, `elasticfilesystem:ClientWrite` to `aws_iam_role_policy.ecs_task_policy`

## Change 4: Backfill Skip Logic (Binance)

**Problem:** When running bulk backfills across many already-filled dates, the current logic unconditionally deletes and re-fetches. This wastes Binance API calls and time.

**Solution:** In `binance_futures_backfill_asset`, before processing each instrument, query the row count for that partition date. If count >= 1440 (full day = 24 h × 60 min), skip that instrument. If count < 1440 (partial or empty), delete existing rows and re-fetch from Binance.

**Code change in** `trading_dagster/assets/binance_futures_ohlcv.py`:

```python
# Per instrument, before the DELETE:
existing = query_scalar(
    f"SELECT count() FROM {TARGET_TABLE} "
    f"WHERE exchange='binance' AND instrument='{instrument}' "
    f"AND ts >= '{start_dt_str}' AND ts < '{end_dt_str}'",
    client
)
if existing >= 1440:
    context.log.info(f"[{instrument}] Full day already present ({existing} rows), skipping.")
    continue
# else: DELETE + re-fetch as before
```

**Correctness:** 1440 = 60 min × 24 h. A complete day has exactly 1440 1-minute bars. Partial days (e.g. today, or gaps from API downtime) are always re-fetched.

## What Is Not Changed

- ALB: kept — needed for webserver and Grafana access
- Fargate Spot: already configured as default capacity provider
- CloudWatch log retention: kept at 30 days
- Secrets Manager: `clickhouse` secret kept; only `dagster_pg` secret removed
- CI/CD workflows: no changes needed
- ClickHouse Cloud: external service, not managed here
- S3 lifecycle policy: already transitions to Glacier IR after 90 days — kept

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| EFS mount latency causes Dagster SQLite contention | Dagster's metadata writes are low-frequency; EFS ~1-3ms latency is negligible |
| EFS volume corruption loses run history | Run history is observability only — not trading data. Acceptable loss. |
| 1 vCPU insufficient for large backfills | `_CHUNK_DAYS=7` caps per-run memory. CPU is adequate for single-threaded Python. Monitor and upsize if needed. |
| ECS task in public subnet exposed | Security group only allows inbound from ALB. Public IP is ephemeral and not in DNS. |
