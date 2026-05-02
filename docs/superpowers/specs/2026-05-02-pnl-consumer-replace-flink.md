# Replace PyFlink Job with Plain Python PnL Consumer

**Date:** 2026-05-02
**Status:** Approved

## Problem

The PyFlink job (`flink_job/`) carries a JVM + Apache Beam + PyFlink gateway (~2GB image, complex startup) to do work that fits comfortably in a plain Python process: consume ~8 instruments × 1 msg/min from Kafka, compute PnL, batch-insert to ClickHouse. Operational costs include invisible CloudWatch logs (JVM logs written to `/opt/flink/log/` inside the container), crash-loop false alarms caused by CI/CD rolling deploys, and S3 checkpoint infrastructure that provides no value since offsets were set to `latest()`.

## Goal

Replace the Flink runner with a `confluent-kafka` consumer loop while keeping all business logic files unchanged. Rename everything `flink` → `pnl-consumer` throughout the repo.

## What Changes

### New directory: `pnl_consumer/`

Replaces `flink_job/`. Files:

| File | Change |
|---|---|
| `pnl_consumer.py` | New — plain consumer loop (replaces `pnl_stream_job.py`) |
| `anchor_state.py` | Moved unchanged |
| `ch_lookup.py` | Moved unchanged |
| `__init__.py` | Moved unchanged |
| `Dockerfile` | New base image, no JVM |
| `requirements.txt` | Swap `apache-flink` → `confluent-kafka>=2.4` |
| `connectors/` | Deleted (JARs no longer needed) |

### Consumer loop (`pnl_consumer.py`)

```
startup:
  consumer = Consumer(group_id="flink-pnl-consumer", auto.offset.reset=earliest, enable.auto.commit=false)
  consumer.subscribe(["binance.price.ticks"])
  _bootstrap_anchors(state)

loop:
  msg = consumer.poll(timeout=1.0)
  if msg is None: continue
  if msg.error(): log + continue (or exit on fatal)
  candle = CandleEvent parsed from msg.value()
  rows = process_candle(candle, state)
  accumulate into price_batch / pnl_batch
  if len(batch) >= FLUSH_EVERY (50):
    flush to ClickHouse
    consumer.commit(asynchronous=False)   ← only after successful insert

on unhandled exception: exit(1), ECS restarts
```

`process_candle()` is moved from `pnl_stream_job.py` into `pnl_consumer.py` unchanged — it has no Flink imports.

**Offset commit strategy:** manual, synchronous, after successful ClickHouse flush only. Provides at-least-once delivery. Duplicate inserts are safe due to `ReplacingMergeTree`.

**Kafka consumer group:** `flink-pnl-consumer` — unchanged to preserve committed offsets across the migration.

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pnl_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY trading_dagster/utils/clickhouse_client.py ./trading_dagster/utils/clickhouse_client.py
COPY trading_dagster/utils/__init__.py ./trading_dagster/utils/__init__.py
COPY trading_dagster/utils/pnl_compute.py ./trading_dagster/utils/pnl_compute.py
RUN echo "" > ./trading_dagster/__init__.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "pnl_consumer.pnl_consumer"]
```

### Terraform (`infra/terraform/main.tf`)

| Resource | Old name | New name |
|---|---|---|
| `aws_ecr_repository` | `trading-analysis-flink-job` | `trading-analysis-pnl-consumer` |
| `aws_iam_role` | `trading-analysis-flink-task` | `trading-analysis-pnl-consumer-task` |
| `aws_iam_role_policy` | `flink-s3-checkpoints` | **deleted** (S3 no longer needed) |
| ECS task family | `trading-analysis-flink-job` | `trading-analysis-pnl-consumer` |
| Container name | `flink-job` | `pnl-consumer` |
| ECS service | `trading-analysis-flink-job` | `trading-analysis-pnl-consumer` |
| CloudWatch stream prefix | `flink-job` | `pnl-consumer` |

`S3_BUCKET` env var removed from task definition. `task_role_arn` still present but points to new minimal role with no S3 policy.

### CI/CD (`.github/workflows/ci-cd.yml`)

| Item | Old | New |
|---|---|---|
| Path filter key | `flink` | `pnl-consumer` |
| Path filter glob | `flink_job/**` | `pnl_consumer/**` |
| Build job | `build-flink` | `build-pnl-consumer` |
| Deploy job | `deploy-flink` | `deploy-pnl-consumer` |
| ECR repo refs | `trading-analysis-flink-job` | `trading-analysis-pnl-consumer` |
| Task definition ref | `trading-analysis-flink-job` | `trading-analysis-pnl-consumer` |
| Container name ref | `flink-job` | `pnl-consumer` |
| ECS service ref | `trading-analysis-flink-job` | `trading-analysis-pnl-consumer` |
| JAR download steps | removed | — |

### Tests

`tests/flink_job/` → `tests/pnl_consumer/`. All import paths updated from `flink_job.*` to `pnl_consumer.*`.

## What Does NOT Change

- `streaming/` package — untouched
- `anchor_state.py`, `ch_lookup.py` — logic unchanged, just moved
- `process_candle()` — moved verbatim, no logic changes
- `_bootstrap_anchors()` — moved verbatim
- Kafka consumer group id: `flink-pnl-consumer`
- ClickHouse tables, schemas, env vars (except `S3_BUCKET` removed)
- ECS memory/CPU allocation (512 CPU / 2048 MB — can reduce later)

## AWS Migration Note

ECR repos and IAM roles cannot be renamed in Terraform — they will be destroyed and recreated. The old `trading-analysis-flink-job` ECR repo will be deleted. A fresh image is pushed on every CI run so no image loss occurs. The old ECS service will be stopped and a new one started; there will be a brief gap in PnL streaming during the deploy, which is acceptable.

## Error Handling

| Scenario | Behaviour |
|---|---|
| ClickHouse insert fails | Log error, do not commit offsets, raise exception → ECS restart |
| Kafka transient error | Log warning, continue polling |
| Kafka fatal error | Log error, exit(1) → ECS restart |
| Unhandled exception | exit(1) → ECS restart |

## Testing

Existing unit tests in `tests/pnl_consumer/` cover `process_candle()`, `AnchorState`, and `ch_lookup` — no changes to test logic needed, only import path updates. No new tests required for the consumer loop itself (it's thin wiring around already-tested functions).
