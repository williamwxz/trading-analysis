# Real-Time Streaming Design: Binance WebSocket → Redpanda → Flink → ClickHouse

**Date:** 2026-04-25  
**Status:** Draft  
**Latency target:** < 1 minute (30s nominal)  
**Path:** Option A now → Option C later (strategy service publishes to Redpanda)

---

## 1. Goals

- Replace the 5-minute Binance REST polling with a WebSocket-based feed so prices land in ClickHouse within 30s of the minute boundary
- Compute PnL continuously (per closed 1-min candle) rather than on a 5-minute Dagster cron
- Keep infrastructure cost low (~$39/month additional on Fargate Spot)
- Design the broker layer so that migrating to Option C (stream-stream join) requires only a Flink job graph change, not a new broker deployment

## 2. Out of Scope

- Backtest PnL (`pnl_bt_v2`) — remains commented out and unchanged
- Real-trade PnL (`pnl_real_trade_v2`) — stub remains; not wired to Flink in this phase
- Sub-second tick-level data — Binance `kline_1m` closed events are sufficient
- Strategy service changes — `strategy_output_history_v2` continues to be written directly to ClickHouse by the external service

---

## 3. Architecture Overview

```
Binance Futures WS (kline_1m, 8 instruments)
    │  closed candle events only (~every 60s per instrument)
    ▼
Python WS Consumer (ECS Fargate Spot, 0.25 vCPU / 512MB)
    │  JSON: {instrument, ts, open, high, low, close, volume}
    ▼
Redpanda (ECS Fargate Spot, 1 vCPU / 2GB)
    │  topic: binance.price.ticks  │  3 partitions  │  1h retention
    ▼
Flink Job (ECS Fargate Spot, 2 vCPU / 4GB)
    ├── Source: Kafka connector → binance.price.ticks
    ├── Async lookup join: strategy_output_history_v2 (ClickHouse, 30s TTL cache)
    │     carry-forward last known position if no bar for this minute
    ├── PnL compute: anchor-chain formula (same as pnl_compute.py)
    └── Sink A: futures_price_1min  (candle record)
        Sink B: strategy_pnl_1min_prod_v2  (PnL rows, one per strategy per minute)

Dagster (unchanged schedule)
    ├── binance_futures_backfill   — historical fill, daily partitioned
    ├── pnl_prod_v2_daily          — daily partitioned backfill
    └── pnl_daily_safety_scan      — 02:00 UTC row-count validation

Decommission (after Flink is stable in prod):
    ├── binance_futures_ohlcv_minutely  (replaced by WS consumer + Flink Sink A)
    └── pnl_prod_v2_live               (replaced by Flink Sink B)
```

### Instruments (Phase 1)

`BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `ADAUSDT`, `AVAXUSDT`, `DOGEUSDT`, `XRPUSDT`, `FETUSDT`

New instruments are added by updating the consumer's symbol list — no infrastructure change required.

---

## 4. Component Design

### 4.1 Python WebSocket Consumer

**Source:** `trading_dagster/streaming/binance_ws_consumer.py` (new module)  
**Deployment:** Standalone ECS Fargate Spot task, separate from Dagster

**Binance stream URL:**
```
wss://fstream.binance.com/stream?streams=
  btcusdt@kline_1m/ethusdt@kline_1m/solusdt@kline_1m/
  adausdt@kline_1m/avaxusdt@kline_1m/dogeusdt@kline_1m/
  xrpusdt@kline_1m/fetusdt@kline_1m
```

One combined WebSocket connection for all 8 instruments.

**Message filter:** Only process events where `k.x == true` (candle closed). Intermediate ticks (every ~2s) are ignored — we don't need sub-minute updates.

**Message published to Redpanda:**
```json
{
  "exchange": "binance",
  "instrument": "BTCUSDT",
  "ts": "2026-04-25T10:01:00",
  "open": 93100.0,
  "high": 93250.0,
  "low": 93050.0,
  "close": 93200.0,
  "volume": 12.34
}
```

**Reconnect logic:**
- Exponential backoff: 1s, 2s, 4s, 8s, up to 60s cap
- On reconnect, re-subscribes to all 8 streams
- No replay needed — Flink receives the next closed candle naturally
- In-memory buffer: bounded `deque(maxlen=120)` holds up to 2 min of closed candles during Redpanda unavailability, then drops oldest

**Libraries:** `websockets`, `confluent-kafka` (Redpanda Kafka-compatible producer)

---

### 4.2 Redpanda

**Deployment:** Single ECS Fargate Spot task, 1 vCPU / 2GB RAM  
**Storage:** Fargate ephemeral storage (21GB default) — sufficient for 1h retention of 8 instruments at ~1 message/min each (~500KB/h)  
**Configuration:**

| Setting | Value | Reason |
|---------|-------|--------|
| Topic | `binance.price.ticks` | Single topic, all instruments |
| Partitions | 3 | Parallelism headroom; 8 instruments round-robin |
| Replication factor | 1 | Single-node; Binance WS replays current minute on reconnect |
| Retention | 1 hour | Covers Flink restart recovery window |
| Kafka API port | 9092 | Flink Kafka connector compatible |

**No ZooKeeper required** — Redpanda uses Raft internally.

---

### 4.3 Flink Job

**Deployment:** Single ECS Fargate Spot task, 2 vCPU / 4GB RAM (JobManager + TaskManager combined, parallelism=1)  
**Checkpointing:** Every 30s to S3 `trading-analysis-data-v2/flink-checkpoints/`  
**Language:** PyFlink (Python) — Flink's Table API from Python. Connectors (Kafka source, ClickHouse JDBC sink) are Java jars configured from Python; the job logic (lookup join, PnL compute) is written in Python, consistent with the rest of the codebase. Chosen over plain Java Flink for consistency, and over a plain Python consumer for built-in state management, fault tolerance, and Option C migration path.

**Job graph:**

```
KafkaSource(binance.price.ticks)
    → deserialize JSON → CandleEvent
    → AsyncLookupJoinFunction (ClickHouse, 30s TTL)
        query: SELECT strategy_table_name, strategy_id, strategy_name,
                      underlying, config_timeframe, weighting,
                      argMin(row_json, revision_ts) AS row_json
               FROM strategy_output_history_v2
               WHERE underlying = ? AND ts <= ? AND ts >= ? - INTERVAL 1 DAY
               GROUP BY strategy_table_name, strategy_id, strategy_name,
                        underlying, config_timeframe, weighting
               ORDER BY ts DESC LIMIT 1 BY strategy_table_name
        fallback: carry-forward cached position (last known anchor)
    → PnLComputeFunction (anchor-chain, same formula as pnl_compute.py)
        cumulative_pnl = anchor_pnl + position * (close - anchor_price) / anchor_price
    → split:
        ├── SinkA: ClickHouseJdbcSink → futures_price_1min
        └── SinkB: ClickHouseJdbcSink → strategy_pnl_1min_prod_v2
```

**Anchor state:** Flink keyed state (key = `strategy_table_name`) holds `(anchor_pnl, anchor_price, anchor_position)` per strategy. Persisted in S3 checkpoints. On cold start, bootstraps from `MAX(ts)` row in `strategy_pnl_1min_prod_v2` (same bootstrap as current Python logic).

**ClickHouse sink configuration:**
- Batch size: 50 rows or 15s flush interval (whichever first) — smaller than current 200k batch since we're writing continuously
- Both sinks use `ReplacingMergeTree` idempotency; Flink restarts re-insert at-least-once, deduplication handled by ClickHouse

---

### 4.4 Dagster Changes

**Remove from `definitions/__init__.py`** (after Flink stable):
- `binance_futures_ohlcv_minutely_asset`
- `pnl_prod_v2_live_asset`

**Keep unchanged:**
- `binance_futures_backfill_asset` — historical candle backfill (REST API, daily partitioned)
- `pnl_prod_v2_daily_asset` — backfill PnL, daily partitioned
- `pnl_real_trade_v2_live_asset` / `pnl_real_trade_v2_daily_asset` — unchanged (stub + daily)
- `pnl_1hour_rollup_asset` — reads `strategy_pnl_1min_prod_v2`, unaffected
- `pnl_daily_safety_scan_asset` — row-count validation, unaffected

---

## 5. Robustness

| Failure | Handling |
|---------|----------|
| WS consumer crash | ECS restarts; consumer reconnects with backoff; Flink watermark stalls then resumes |
| Redpanda crash | ECS restarts; consumer in-memory buffer holds ~2 min of candles; Flink restores from checkpoint |
| Flink crash | ECS restarts; restores from S3 checkpoint (≤30s reprocess); ClickHouse deduplicates via ReplacingMergeTree |
| ClickHouse unavailable | Flink backpressures; Redpanda absorbs up to 1h of messages; no data loss |
| Binance WS disconnect | Consumer reconnects with exponential backoff; next closed candle resumes normally |
| Instrument added | Update consumer symbol list + redeploy; no Redpanda or Flink changes |

**Gap detection:** `pnl_daily_safety_scan` (existing, 02:00 UTC) validates row counts. If Flink was down for a period, Dagster's `pnl_prod_v2_daily` backfill job fills the gap on demand.

---

## 6. Infrastructure Changes

### New ECS Tasks

| Task | Image | CPU | Memory | Capacity |
|------|-------|-----|--------|----------|
| `binance-ws-consumer` | New Python image | 256 | 512MB | FARGATE_SPOT |
| `redpanda` | `redpandadata/redpanda:latest` | 1024 | 2048MB | FARGATE_SPOT |
| `flink-job` | New Java image | 2048 | 4096MB | FARGATE_SPOT |

### New ECR Repositories
- `trading-analysis-ws-consumer`
- `trading-analysis-flink-job`

### S3
- New prefix: `trading-analysis-data-v2/flink-checkpoints/` (existing bucket)

### Terraform
- New ECS task definitions and services for the 3 tasks above
- Security group rules: consumer → Redpanda (9092), Flink → Redpanda (9092), Flink → ClickHouse (8443)
- IAM: Flink task role needs S3 read/write on `flink-checkpoints/` prefix

### New GitHub Secrets
- None required — Redpanda is internal to VPC; Flink uses existing ClickHouse secrets

---

## 7. Cost Estimate (ap-northeast-1, Fargate Spot, 24/7)

| Component | vCPU | Memory | Monthly |
|-----------|------|--------|---------|
| Python WS Consumer | 0.25 | 512MB | ~$3 |
| Redpanda | 1.0 | 2GB | ~$12 |
| Flink Job | 2.0 | 4GB | ~$24 |
| **Total additional** | | | **~$39/month** |

Existing costs (NAT instance ~$8, RDS ~varies, Dagster ECS ~$4) are unchanged.

---

## 8. Migration Plan

1. **Deploy Redpanda** → verify Kafka API accessible within VPC
2. **Deploy WS Consumer** → verify closed candles appearing in `binance.price.ticks` topic
3. **Deploy Flink job in shadow mode** → write to staging tables (`futures_price_1min_v2_shadow`, `strategy_pnl_1min_prod_v2_shadow`) and compare with existing Dagster output
4. **Validate** → run `pnl_daily_safety_scan` equivalent against shadow tables for 24h
5. **Cutover** → switch Flink sinks to production tables; decommission `binance_futures_ohlcv_minutely` and `pnl_prod_v2_live` from Dagster

---

## 9. Path to Option C (Future)

When the external strategy service is ready:
- Add Redpanda topic: `strategy.bars`
- Strategy service publishes bar events to this topic instead of (or in addition to) writing directly to ClickHouse
- Flink's `AsyncLookupJoinFunction` becomes a `IntervalJoin` or `TemporalJoin` between `binance.price.ticks` and `strategy.bars`
- No changes to Redpanda infrastructure, consumer, or ClickHouse schema

The only change is the Flink job graph — all other components remain identical.
