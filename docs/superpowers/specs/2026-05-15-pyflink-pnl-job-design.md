# PyFlink PnL Job on ECS Fargate — Design Spec

**Date:** 2026-05-15
**Status:** Approved

## Problem

The current `pnl_consumer` is a plain Python confluent-kafka loop that works well at today's scale (8 instruments × 1 msg/min). The goals of this project are:

1. **Operational maturity** — add checkpointing, replay, and fault tolerance that survive ECS task restarts without cold-start re-bootstrap from ClickHouse
2. **Throughput headroom** — validate that PyFlink on ECS Fargate can handle higher instrument counts without architectural changes
3. **Evaluation** — determine concretely whether PyFlink on ECS Fargate is viable before committing to a full cutover

The new job runs **alongside** `pnl_consumer` during evaluation. No cutover, no removal of the current consumer until Flink is validated in production.

---

## Architecture

```
Redpanda
  binance.price.ticks  (CandleEvent JSON, 1-min closed candles)
       │
       ▼
 PyFlink Job (ECS Fargate, single task — JobManager + TaskManager in-process)
 ┌──────────────────────────────────────────────────────────────────────┐
 │  FlinkKafkaConsumer                                                  │
 │       │  deserialize JSON → CandleEvent                              │
 │       ▼                                                              │
 │  PnlProcessFunction  (parallelism=1, non-keyed)                      │
 │    open():                                                           │
 │      read SinkConfig from env (price/prod/bt/real_trade flags)       │
 │      bootstrap only enabled PnL modes from ClickHouse               │
 │    processElement(candle):                                           │
 │      underlying = candle.instrument.removesuffix("USDT")  # "BTC"   │
 │      if cfg.price: emit PriceRow                                     │
 │      if cfg.prod:  fetch, carry-forward, emit PnlRows (prod)         │
 │      if cfg.bt:    fetch, carry-forward, emit PnlRows (bt)           │
 │      if cfg.real_trade: fetch, revision guard, emit PnlRows          │
 │       ▼                                                              │
 │  ClickHouseSinkFunction                                              │
 │    invoke(): buffer rows in-memory                                   │
 │    snapshotState(): flush buffer to ClickHouse on checkpoint (60s)   │
 │    idempotent upsert via ReplacingMergeTree                          │
 └──────────────────────────────────────────────────────────────────────┘
       │
       ▼
 ClickHouse
   analytics.futures_price_1min
   analytics.strategy_pnl_1min_prod_v2
   analytics.strategy_pnl_1min_bt_v2
   analytics.strategy_pnl_1min_real_trade_v2
```

**Key decisions:**

- **Standalone mini-cluster**: JobManager + TaskManager run in-process in a single ECS task. No separate JM/TM networking, no cluster management.
- **Parallelism=1**: The anchor state spans all strategies across all underlyings in a single dict. Parallelism > 1 would require keyed streams and per-operator bootstrap — deferred to phase 2.
- **New directory `flink_pnl/`**: Additive only. `pnl_consumer/` is untouched until Flink is validated.
- **Shared business logic**: `flink_pnl/` imports from `libs/computation/` — same `AnchorState`, bootstrap, candle lookup functions as `pnl_consumer`. No logic duplicated.

---

## Anchor State Structure

Three independent state dicts, one per mode:

```python
# underlying ("BTC") → strategy_table_name → AnchorRecord
StateMap = dict[str, dict[str, AnchorRecord]]

state_prod:       StateMap  # production
state_bt:         StateMap  # backtest
state_real_trade: StateMap  # real_trade
```

`AnchorRecord` is the existing dataclass from `libs/computation/anchor_state.py`:

```python
@dataclass
class AnchorRecord:
    pnl: float
    price: float           # last known candle open price (from Redpanda, not ClickHouse PnL table)
    position: float        # from strategy_output_history — never from PnL table
    bar_ts: datetime       # last bar timestamp
    revision_ts: datetime  # for real_trade revision guard
    # strategy metadata for output rows
    strategy_instance_id: int
    strategy_table_name: str
    strategy_name: str
    underlying: str
    config_timeframe: str
    weighting: float
    final_signal: float
    benchmark: bool
```

**Why `underlying → strategy_table_name` and not flat `strategy_table_name`?**

Each candle arrives for one instrument (e.g. `BTCUSDT` → underlying `BTC`). On each candle, carry-forward must fire for **all strategies under that underlying** — both those that got a new bar and those that did not (late-arriving 15m, 1h, 1d bars). Grouping by underlying makes this O(1) lookup: `state_prod.get("BTC", {})` gives all BTC strategies instantly without scanning the full state.

---

## Per-Mode Differences (Critical)

All three modes share the same carry-forward and PnL formula. They differ in bar sourcing, revision logic, and cold-start seeding.

| | Prod | Backtest | Real-trade |
|---|---|---|---|
| History table | `strategy_output_history_v2` | `strategy_output_history_bt_v2` | `strategy_output_history_v2` |
| Revision logic | `argMin(row_json, revision_ts)` — first revision only | `argMin` — first revision only | All revisions; latest `revision_ts <= candle_ts` |
| Bar gate | `closing_ts <= candle_ts` | `closing_ts <= candle_ts` | No closing_ts gate — revision applies immediately |
| Anchor update trigger | New bar (`bar_ts` changed) | New bar (`bar_ts` changed) | New revision passes guard `(bar_ts, revision_ts) > anchor` |
| Carry-forward | Yes — same position, new candle open price | Yes | Yes — plus revision guard rejects stale revisions |
| Cold-start seed | Previous day tail from pnl table | Previous day tail; fallback to `cumulative_pnl` from `row_json` only when no prior anchor exists | Previous day tail only — no row_json seed |
| PnL output table | `strategy_pnl_1min_prod_v2` | `strategy_pnl_1min_bt_v2` | `strategy_pnl_1min_real_trade_v2` |

**Price invariant (all modes):** `price` in all output rows is always the candle `open` from Redpanda — never from `futures_price_1min` or from the PnL table's `price` column. `futures_price_1min` is only used during bootstrap/walk (historical price reconstruction).

**Position invariant (all modes):** `position` always comes from `strategy_output_history_*` — never from the PnL table.

---

## Candle Processing — Per-Candle Logic

```python
def processElement(candle: CandleEvent):
    underlying = candle.instrument.removesuffix("USDT")  # "BTCUSDT" → "BTC"
    
    # --- Price row (always emitted) ---
    emit PriceRow(candle)

    # --- Prod ---
    new_prod_bars = fetch_strategies_for_candle(underlying, candle.ts)
    prod_seen = set()
    for bar in new_prod_bars:
        update state_prod[underlying][bar.strategy_table_name] with new bar
        emit PnlRow(prod, bar, candle.open)
        prod_seen.add(bar.strategy_table_name)
    for stn, anchor in state_prod.get(underlying, {}).items():
        if stn not in prod_seen:
            emit CarryForwardPnlRow(prod, anchor, candle.open)  # same position, new price

    # --- Backtest --- (same pattern, fetch_bt_strategies_for_candle)

    # --- Real-trade --- (fetch_real_trade_for_candle, apply revision guard)
    for rev in new_rt_revs:
        if not state_real_trade.should_apply_revision(rev.strategy_table_name, rev.bar_ts, rev.revision_ts):
            emit CarryForwardPnlRow(real_trade, anchor, candle.open)
            continue
        update state_real_trade[underlying][rev.strategy_table_name]
        emit PnlRow(real_trade, rev, candle.open)
    for stn, anchor in state_real_trade.get(underlying, {}).items():
        if stn not in rt_seen:
            emit CarryForwardPnlRow(real_trade, anchor, candle.open)
```

---

## Cold-Start Bootstrap

Reuses existing `_bootstrap_state()` from `pnl_consumer/pnl_consumer.py` verbatim (via `libs/computation/`):

1. `peek_reference_ts()` — read Kafka committed offset for this group → `reference_ts`
2. `seed_ts = reference_ts - 48h` — covers all timeframes including 1d bars
3. `walk_ts = reference_ts - 2h` — walk window
4. Seed position from `strategy_output_history_*` at `seed_ts`
5. Seed pnl/price from `strategy_pnl_1min_*` at `walk_ts`
6. Walk `[walk_ts, reference_ts)` — replay stored rows, verify PnL deviation < 0.2%, crash if exceeded
7. After walk, populate `state_prod`, `state_bt`, `state_real_trade` with final anchor per strategy, grouped by `underlying`

Bootstrap runs in `PnlProcessFunction.open()` — once on job start. Flink restores from S3 checkpoint on restart, skipping bootstrap entirely.

**Kafka consumer group:** `flink-pnl-consumer-v2` — new group ID to avoid interfering with `pnl_consumer`'s committed offsets (`flink-pnl-consumer`).

---

## Exactly-Once Semantics

- **Flink checkpoints** every 60s to `s3://trading-analysis-data-v2/flink-pnl-checkpoints/`
- On checkpoint: `ClickHouseSinkFunction.snapshotState()` flushes in-memory buffer to ClickHouse, then Flink snapshots the Kafka consumer offset
- On restart from checkpoint: Kafka replays from last checkpointed offset; any rows already written are harmless — `ReplacingMergeTree(updated_at)` deduplicates on background merge; `LIMIT 1 BY` reads give correct results immediately
- **At-least-once delivery + idempotent upsert = effectively exactly-once** from the data perspective, without two-phase commit complexity

---

## Docker Image & ECS Deployment

### Dockerfile (`flink_pnl/Dockerfile`)

```dockerfile
FROM apache/flink:2.2.1-java21
# The official flink image no longer ships a python3 variant.
# Install Python + PyFlink pip package (must match JVM image version exactly).
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip && \
    pip3 install --no-cache-dir \
        apache-flink==2.2.1 \
        clickhouse-connect>=0.8 \
        confluent-kafka>=2.4 \
        boto3>=1.34
WORKDIR /opt/flink/usrlib
COPY libs/ ./libs/
COPY streaming/ ./streaming/
COPY flink_pnl/ ./flink_pnl/
ENV PYTHONPATH=/opt/flink/usrlib
CMD ["python3", "-m", "flink_pnl.entrypoint"]
```

**Version pinning note:** The `apache/flink` Docker image version and `apache-flink` PyPI package version must match exactly. Do not use `latest` — if the Docker image bumps to 2.3 while `requirements.txt` stays at 2.2.1, the job fails to start with a version mismatch. Update both together deliberately.

### requirements.txt (`flink_pnl/requirements.txt`)

```
apache-flink==2.2.1
clickhouse-connect>=0.8
confluent-kafka>=2.4
boto3>=1.34
```

### ECS Task (`infra/terraform/main.tf` — additive)

| Property | Value |
|---|---|
| Task family | `trading-analysis-flink-pnl` |
| CPU | 1024 |
| Memory | 3072 MB (JVM needs headroom) |
| ECR repo | `trading-analysis-flink-pnl` |
| Container name | `flink-pnl` |
| Task role | `trading-analysis-flink-pnl-task` (S3 read/write for checkpoints) |
| Log stream prefix | `flink-pnl` |

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_HOST` | (required, from secret) | ClickHouse host |
| `CLICKHOUSE_PASSWORD` | (required, from secret) | ClickHouse password |
| `CLICKHOUSE_PORT` | `8443` | ClickHouse port |
| `CLICKHOUSE_USER` | `dev_ro3` | ClickHouse user |
| `CLICKHOUSE_SECURE` | `true` | TLS |
| `REDPANDA_BROKERS` | (required) | Redpanda broker address |
| `AWS_REGION` | `ap-northeast-1` | AWS region for CloudWatch metrics |
| `KAFKA_GROUP_ID` | `flink-pnl-consumer-v2` | Kafka consumer group |
| `ENABLE_PRICE_SINK` | `true` | Write to `futures_price_1min` |
| `ENABLE_PROD_SINK` | `false` | Write prod PnL |
| `ENABLE_BT_SINK` | `false` | Write backtest PnL |
| `ENABLE_REAL_TRADE_SINK` | `false` | Write real-trade PnL |

Sink flags default to `false` for all PnL modes — price sink only by default, matching the current `pnl_consumer` default. Enable modes explicitly per ECS task definition to avoid running all three modes in one task (keeps memory footprint and ClickHouse query load predictable).

**Logs:** PyFlink embedded mode writes to stdout → CloudWatch. No `/opt/flink/log/` black hole.

### S3 IAM Policy (task role)

```hcl
resource "aws_iam_role_policy" "flink_pnl_s3" {
  name = "flink-pnl-s3-checkpoints"
  role = aws_iam_role.flink_pnl_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        "arn:aws:s3:::trading-analysis-data-v2",
        "arn:aws:s3:::trading-analysis-data-v2/flink-pnl-checkpoints/*"
      ]
    }]
  })
}
```

---

## File Map

| Action | Path |
|---|---|
| Create | `flink_pnl/__init__.py` |
| Create | `flink_pnl/entrypoint.py` — Flink env setup, checkpoint config, job registration |
| Create | `flink_pnl/pnl_process_function.py` — `PnlProcessFunction(ProcessFunction)` |
| Create | `flink_pnl/clickhouse_sink.py` — `ClickHouseSinkFunction(SinkFunction)` |
| Create | `flink_pnl/Dockerfile` |
| Create | `flink_pnl/requirements.txt` |
| Create | `tests/flink_pnl/__init__.py` |
| Create | `tests/flink_pnl/test_pnl_process_function.py` |
| Modify | `infra/terraform/main.tf` — add ECR repo, IAM role, ECS task/service (additive) |
| Modify | `.github/workflows/ci-cd.yml` — add `build-flink-pnl` and `deploy-flink-pnl` jobs |

---

## What Does NOT Change

- `pnl_consumer/` — untouched, continues running
- `libs/computation/` — imported by `flink_pnl/`, not modified
- `streaming/` — untouched
- Existing ClickHouse tables, schemas, env vars
- Existing Terraform resources (no renames, additive only)
- Kafka consumer group `flink-pnl-consumer` (used by `pnl_consumer`) — new group `flink-pnl-consumer-v2` for Flink job

---

## Error Handling

| Scenario | Behaviour |
|---|---|
| ClickHouse insert fails in `snapshotState()` | Exception propagates → Flink aborts checkpoint → retries checkpoint at next interval |
| ClickHouse lookup fails in `processElement()` | Log error, raise → Flink task fails → ECS restarts → Flink restores from last checkpoint |
| Kafka transient error | Flink's built-in retry handles reconnect |
| Bootstrap PnL deviation > 0.2% | `RuntimeError` in `open()` → job fails → ECS restarts (same as current consumer) |
| S3 checkpoint write fails | Flink aborts checkpoint, retries — does not fail the job |

---

## Testing

| Test | Type | What it covers |
|---|---|---|
| `test_pnl_process_function.py` | Unit | `processElement()` with mocked ClickHouse: new bar, carry-forward, all three modes, real_trade revision guard |
| `test_pnl_process_function.py` | Unit | `underlying` stripping: `BTCUSDT` → `BTC` |
| `test_pnl_process_function.py` | Unit | Late-arriving strategy carry-forward emits row with last known position |
| Manual smoke test | Integration | Docker build + `docker run` with fake ClickHouse env vars — verify Flink starts, logs to stdout, no JVM noise |

No new integration tests against live ClickHouse — existing `libs/computation/` tests cover the business logic.

---

## Phase 2 (Future — Not In Scope)

- **Keyed state**: key stream by `underlying`, store `AnchorRecord` in Flink `ValueState` per `strategy_instance_id` — eliminates ClickHouse re-bootstrap on restart
- **Parallelism > 1**: once keyed, operators can run in parallel per underlying
- **Cutover**: disable `pnl_consumer` once Flink job is validated in production for 1+ week
