# Migration Plan — Redpanda Fargate Right-Size (PR #59)

**Change:** broker task 1024 CPU / 2048 MB → **256 CPU / 1024 MB** (`rpk --memory 750M`,
add `--overprovisioned`); image pinned `latest` → `v26.1.12` (digest-identical to the
task running today); topic-create polls broker readiness (was `sleep 15`); service
deploys stop-then-start (min 0 / max 100).

**Not changing:** topic name/partitions/retention, security groups, Cloud Map DNS,
producer/consumer code, ClickHouse anything.

**Blast radius:** one single-node broker task. Expected stream pause ≈ 1–3 min.
Tolerated pause ≈ 15 min (ws-consumer buffers 120 candles = 15 min at 8 instruments,
flushed on reconnect). Broker replacement is already routine: Fargate Spot, no
persistent volume, 26 replacements to date. Consumers use `auto.offset.reset=earliest`
and commit offsets only after ClickHouse writes — redelivery, never skips;
ReplacingMergeTree dedupes.

**Evidence for the size:** 14-day CloudWatch — CPU avg 1.01% / hourly max 4.96% of
1 vCPU; memory ~11% (~240 MB of 2048 MB). On the new size: peak CPU ≈ 20% of
allocation, memory limit 750M ≈ 3.1× observed.

Shell prelude for all commands below:

```bash
export AWS_PROFILE=AdministratorAccess-068704208855
export AWS_REGION=ap-northeast-1
CLUSTER=trading-analysis
SVC=trading-analysis-redpanda
```

---

## 1 · Pre-flight (5 min, same day as merge)

**Timing rule:** merge any time EXCEPT **15:00–15:45 UTC** (daily `fix_bt_window`
Lambda pauses/repairs/resumes the bt consumer — don't overlap two moving parts).
Be available for ~20 min after merge.

- [ ] PR #59 approved; branch up to date with `main`.
- [ ] Confirm rollback target still current (expect revision **6**):

```bash
aws ecs describe-services --cluster $CLUSTER --services $SVC \
  --query 'services[0].taskDefinition' --output text
```

- [ ] Baseline freshness (note the values; ClickHouse creds: `source .env` in the main repo):

```sql
SELECT max(ts) FROM analytics.futures_price_1min WHERE ts > now() - INTERVAL 1 HOUR;
SELECT max(ts) FROM analytics.strategy_pnl_1min_prod_v2 WHERE ts > now() - INTERVAL 1 HOUR;
```

- [ ] Grafana `streaming-infra` dashboard green (Messages Published/Received per min > 0).

## 2 · Cutover (automated — merge the PR)

Merge PR #59 → GitHub Actions `ci-cd.yml`: test → terraform apply (~5–10 min after
merge; only the terraform job matters — no service paths changed, so no image
rebuilds). Terraform registers task def revision 7 and updates the service; ECS
**stops the old task, then starts the new one** (min 0 / max 100).

Expected sequence: old task drains (~30 s) → new task pulls `v26.1.12` + starts
(~1–2 min at 0.25 vCPU) → readiness poll passes → topic recreated → producer flushes
buffer → consumers reset to earliest and drain.

Record merge time as **T0**.

## 3 · Verification (T0+5 → T0+20 min)

Run in order; all must pass.

**3.1 ECS steady + new revision:**

```bash
aws ecs describe-services --cluster $CLUSTER --services $SVC \
  --query 'services[0].{def:taskDefinition,running:runningCount,events:events[0:3].message}'
```

Expect: task def `:7`, running 1, recent event "has reached a steady state".

**3.2 Broker + producer + consumer logs** (log group `/ecs/trading-analysis`, 3-day retention):

```bash
for P in redpanda ws-consumer pnl-consumer-price pnl-consumer-prod pnl-consumer-real-trade pnl-consumer-bt; do
  echo "== $P"; aws logs tail /ecs/trading-analysis --since 15m --format short \
    --filter-pattern "" 2>/dev/null | grep "/$P/" | tail -5
done
```

Look for: redpanda "Successfully started Redpanda!" + topic create OK; ws-consumer
"Sent SUBSCRIBE for 8 instruments" and no repeated "Failed to publish candle";
each pnl-consumer either keeps flushing (stayed alive, reset to earliest) or logs
"Cold-start reference_ts …" / "Subscribed to binance.price.ticks as group …" then
flushes (restarted → bootstrap). Both paths are correct.

**3.3 ClickHouse freshness advancing** (re-run the two baseline queries; `max(ts)`
must be ≥ T0 and advancing on a second run ~2 min later).

**3.4 Price completeness over the cutover window** (replace timestamps, UTC):

```sql
WITH toDateTime('YYYY-MM-DD HH:00:00') AS w0, toDateTime('YYYY-MM-DD HH:00:00') AS w1
SELECT instrument, dateDiff('minute', w0, w1) AS expected, uniqExact(ts) AS have
FROM analytics.futures_price_1min WHERE ts >= w0 AND ts < w1
GROUP BY instrument HAVING have < expected;
```

Empty result = zero missing minutes → done. If rows appear, heal immediately
instead of waiting for the 00:30 UTC daily run:

```bash
aws lambda invoke --function-name trading-analysis-backfill-prices \
  --payload '{"lookback_hours": 3}' --cli-binary-format raw-in-base64-out /dev/stdout
```

then re-run the SQL → must be empty. (The Lambda does per-minute gap detection
against Binance REST — the authoritative source — and idempotent inserts.)

**3.5 New task not OOM/throttled:** no stopped tasks with reason `OutOfMemoryError`:

```bash
aws ecs list-tasks --cluster $CLUSTER --service-name $SVC --desired-status STOPPED \
  --query 'taskArns' --output text | xargs -r -n1 -I{} aws ecs describe-tasks \
  --cluster $CLUSTER --tasks {} --query 'tasks[0].{stopped:stoppedReason,containers:containers[0].reason}'
```

## 4 · Rollback

**Triggers (any one):** new task restarts ≥3× / OOM stopped reason / any consumer
crash-looping at T0+15 / freshness (3.3) not advancing by T0+15 / gap SQL (3.4)
growing after a backfill invoke.

**Fast path (~3 min) — do this first:**

```bash
aws ecs update-service --cluster $CLUSTER --service $SVC \
  --task-definition trading-analysis-redpanda:6
```

**Then required:** revert PR #59 on `main` (otherwise the next terraform apply
re-deploys revision 7 over your CLI rollback). The rollback is itself a broker
swap — re-run section 3. Note: revision 6 uses image `latest`, which may by then
resolve newer than v26.1.12; acceptable for an emergency, it's today's config.

## 5 · Post-migration (T0 + 1 day)

- [ ] `backfill_prices` 00:30 UTC run: Lambda Errors = 0 (it currently runs 1×/day,
      7/7 days green).
- [ ] `fix_bt_window` 15:00 UTC run completed normally (bt consumer resumed).
- [ ] 24 h utilization on the new size — healthy = CPU max < 50%, memory max < 60%:

```bash
for M in CPUUtilization MemoryUtilization; do
  aws cloudwatch get-metric-statistics --namespace AWS/ECS --metric-name $M \
    --dimensions Name=ClusterName,Value=$CLUSTER Name=ServiceName,Value=$SVC \
    --start-time $(date -u -v-24H +%Y-%m-%dT%H:%M:%S) --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 3600 --statistics Average Maximum \
    --query 'sort_by(Datapoints,&Timestamp)[].{a:Average,m:Maximum}' --output text | tail -3
done
```

- [ ] Grafana note: CPU/Memory **percentage** panels re-baseline against the smaller
      task (~×4 CPU%, ×2 mem%) — expected arithmetic, not load growth. Absolute
      `MemoryUtilized` MiB panels unchanged.
- [ ] Only if 3.4 found gaps that were healed: optionally run the read-only audit
      (`python scripts/audit_pnl.py --type prod`) — a candle dropped before healing
      would show as a 1-min coverage hole in prod/real_trade PnL (bt self-heals via
      the daily 49 h rebuild). Fix with `--fix-window` over the cutover hour if so.

## Symptom → action quick map

| Symptom | Meaning | Action |
|---|---|---|
| New task cycles, health check failing | broker not serving Kafka API on new size | Rollback §4 |
| `OutOfMemoryError` stopped reason | 750M/1024 too tight (not expected: 3× headroom) | Rollback §4 |
| ws-consumer "Failed to publish candle" persists > 5 min after task RUNNING | DNS/SG issue or broker half-up | Check redpanda log; if not "Successfully started" by T0+15 → Rollback |
| Consumers alive but freshness stalled | consumers stuck on stale assignment | Force restart one consumer: `aws ecs update-service --cluster $CLUSTER --service trading-analysis-pnl-consumer-price --force-new-deployment`; bootstrap re-seeds |
| Gap SQL non-empty after cutover | candles dropped (outage > buffer) | Invoke backfill Lambda (§3.4); prices heal in ~1 min |
