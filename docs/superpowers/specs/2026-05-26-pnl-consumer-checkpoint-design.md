# PnL Consumer Supabase Checkpoint — Design

**Status**: Approved (pending spec review)
**Date**: 2026-05-26
**Author**: William (with Claude)
**Related**: scale up `pnl-consumer-bt` and `pnl-consumer-real-trade` (Phase 0)

## Problem

Every `pnl_consumer` cold start issues a large ClickHouse query (~30–50k rows per strategy across a 48h window) to rebuild in-memory `AnchorState`. The current bootstrap also performs a 2h verification walk to guard against drift. Two motivating constraints:

1. **Cold-start cost**: deploys, autoscale events, and crashes all pay this cost.
2. **No external watermark exists**: the consumer can't tell which rows in `analytics.strategy_pnl_1min_*` were written by itself versus by Dagster's periodic recompute, so it cannot use the ClickHouse pnl tables as a checkpoint of its own processing position.

Eventual Flink migration is planned; the design must not box that in.

## Goals

- Persist consumer state (`AnchorState`) to Supabase Postgres after each successful flush, so cold start can restore from Supabase instead of querying ClickHouse.
- Maintain correctness across crash-restart: a restored consumer must produce identical PnL to one that never crashed.
- Align with industry-standard stream-processor patterns so the eventual Flink migration is conceptual continuity, not a rewrite.
- Best-effort write — Supabase outages must not block the Kafka commit / ClickHouse write path.

## Non-goals

- Periodic correctness reconciliation against ClickHouse — Dagster will own this as a separate workstream.
- Multi-writer coordination — each `mode` has a single ECS service / consumer-group instance.
- Schema migration framework — single tenant, single env; manual apply is fine.

## Decision: atomic checkpoint, no replay verification

Industry-standard stream processors (Flink, Kafka Streams, Storm Trident) do **not** replay-verify state on restart. They invest in atomic state+offset commits and idempotent retries instead. Replay verification compares the live code path against itself, so it cannot catch formula bugs — it only catches drift between checkpoint and observed-output, which becomes irrelevant once the checkpoint *is* the source of truth.

We adopt the same pattern. Sanity checks (hash, version, freshness, offset match, optional invariant cross-check) replace the replay walk. This mental model maps 1:1 onto Flink's checkpoint barriers, minimizing migration cost.

The current 48h ClickHouse bootstrap remains as the fallback path — untouched code, our safety net.

## Architecture

```
Redpanda (binance.price.ticks)
   │
   ▼
pnl_consumer  ───►  ClickHouse (analytics.strategy_pnl_1min_*)
   │
   │ on every flush, best-effort:
   ▼
Supabase Postgres (streaming.pnl_checkpoint + streaming.pnl_commit_state)
   │
   │ on cold start: load + sanity check
   ▼
seed AnchorState  ─or─►  fall back to existing 48h ClickHouse bootstrap

(Separate workstream: Dagster periodic reconciliation reads checkpoint, alerts on drift)
```

Key invariant: **Kafka offset is authoritative**. Supabase may lag during outages; the cold-start path detects lag via `kafka_offset` comparison and falls back to bootstrap when it occurs.

## Supabase schema

```sql
CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE streaming.pnl_checkpoint (
    mode                  TEXT NOT NULL,         -- 'prod' | 'bt' | 'real_trade'
    strategy_table_name   TEXT NOT NULL,
    pnl                   DOUBLE PRECISION NOT NULL,
    price                 DOUBLE PRECISION NOT NULL,
    position              DOUBLE PRECISION NOT NULL,
    bar_ts                TIMESTAMPTZ,           -- real_trade only
    revision_ts           TIMESTAMPTZ,           -- real_trade only
    strategy_id           TEXT,
    strategy_name         TEXT,
    underlying            TEXT,
    config_timeframe      TEXT,
    weighting             DOUBLE PRECISION,
    strategy_instance_id  TEXT,
    final_signal          TEXT,
    benchmark             DOUBLE PRECISION,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mode, strategy_table_name)
);

CREATE TABLE streaming.pnl_commit_state (
    mode             TEXT NOT NULL PRIMARY KEY,
    last_candle_ts   TIMESTAMPTZ NOT NULL,
    kafka_topic      TEXT NOT NULL,
    kafka_partition  INTEGER NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    state_hash       TEXT NOT NULL,
    schema_version   INTEGER NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pnl_checkpoint_mode_updated
    ON streaming.pnl_checkpoint (mode, updated_at);
```

`pnl_checkpoint` holds per-strategy state. `pnl_commit_state` is the atomic-commit anchor; its `state_hash` is sha256 over the canonical encoding of all `pnl_checkpoint` rows for the mode. A consistent read pair (checkpoint rows + commit_state) is guaranteed by writing both in one Postgres transaction.

`bar_ts` and `revision_ts` are nullable for prod/bt rows, NOT NULL semantically for real_trade rows (enforced in code on read).

## Write path

Per-batch flush ordering, executed in `pnl_consumer.py` flush loop:

1. **INSERT** to ClickHouse (idempotent via `ReplacingMergeTree`)
2. **try**: UPSERT to Supabase in a single transaction:
   - Bulk UPSERT of `pnl_checkpoint` rows for this mode
   - UPSERT of `pnl_commit_state` row for this mode
   - **except**: log + increment failure counter + continue
3. **Commit** Kafka offset

Best-effort means step 2 never raises; Kafka offset always advances after a successful ClickHouse write.

Consequence: Supabase may lag behind Kafka during a Supabase outage. The next successful write rewrites the full current state (UPSERT semantics), so the checkpoint self-heals as soon as Supabase recovers. Cold starts during the gap pay the 48h bootstrap cost — visible via monitoring, expected behavior.

`state_hash` computation: `sha256(json.dumps(canonical_records, sort_keys=True))` where `canonical_records` is the sorted list of per-strategy tuples with floats rounded to 12 decimal places.

## Cold-start read path

Replaces the entry point of `_bootstrap_state` / `_bootstrap_bt_state`; falls back to them on any failure.

```
1. Read streaming.pnl_commit_state row for this mode
   → no row → FALLBACK (first start or wipe)

2. Sanity checks (cheap, no ClickHouse):
   a. schema_version matches code's CURRENT_VERSION
   b. last_candle_ts within 24h of now
   c. kafka_offset matches Kafka consumer-group committed offset
      (specifically: supabase.kafka_offset == consumer.committed() - 1,
       because Supabase stores the offset *processed* while Kafka commits
       offset+1 as the next-to-read position; mismatch indicates Supabase
       fell behind during an outage)
   d. Recomputed state_hash matches stored state_hash
   e. Non-empty strategy set
   → any fail → FALLBACK (log reason)

3. Invariant cross-check (configurable, default on):
   One ClickHouse query: latest position per strategy_instance_id
   in strategy_output_history_v2 / _bt_v2, compared to checkpoint.position
   → mismatch → FALLBACK (log diff)

4. Seed AnchorState from checkpoint rows.
   Kafka resume = standard committed-offset behavior.

5. Enter live loop.
```

Fallback path is the existing `_bootstrap_state` / `_bootstrap_bt_state` code, called unmodified. After a successful fallback, the first flush writes a fresh checkpoint; steady-state resumes.

Happy-path cold-start cost: ~250–650 ms (one Postgres SELECT pair + sanity checks + one small ClickHouse query). Today: ~1–3 s plus ~30–50k ClickHouse rows scanned.

## Real-trade specifics

Real-trade is the highest-risk mode due to the revision guard tuple `(bar_ts, revision_ts)`.

**Persistence**: both fields are part of the checkpoint row. After restore, `AnchorState.should_apply_revision()` works unchanged — the tuple comparison is the only state needed.

**Additional sanity checks** for real-trade mode:
- All checkpoint rows have non-null `bar_ts` and `revision_ts`
- `revision_ts ≤ last_candle_ts` for every row

**Variant invariant check**: query `strategy_output_history_v2 WHERE ts = checkpoint.bar_ts AND revision_ts = checkpoint.revision_ts`, extract `position` from `row_json`, compare to `checkpoint.position`. Fall back on mismatch.

**Required correctness tests** (see Testing):
- Older `(bar_ts, revision_ts)` rejected after restore
- Equal tuple rejected (no double-apply)
- Newer revision accepted
- Newer bar with older `revision_ts` accepted
- Older bar with newer `revision_ts` rejected
- Restore + serialize/deserialize roundtrip preserves tuple comparison

## Code organization

### New modules

```
libs/
  postgres_client.py                                 NEW — thin Supabase/Postgres client
  computation/
    checkpoint_store.py                              NEW — read/write/hash, SCHEMA_VERSION constant

services/pnl_consumer/pnl_consumer/
  cold_start.py                                      NEW — orchestrates checkpoint try / sanity / fallback
```

`libs/postgres_client.py` mirrors `libs/clickhouse_client.py`: `get_client()`, `execute()`, `query_rows()`, `query_dicts()`. One connection per call by default; accepts `client=` for batched ops. Env vars: `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_DATABASE`, `SUPABASE_SSLMODE`.

`libs/computation/checkpoint_store.py`:
- `SCHEMA_VERSION: int`
- `write_checkpoint(mode, anchor_state, kafka_topic, kafka_partition, kafka_offset, last_candle_ts, *, client)`
- `read_checkpoint(mode, *, client) -> CheckpointLoadResult | None`
- `compute_state_hash(anchor_state) -> str`
- Knows `AnchorState` / `AnchorRecord` types; no Kafka knowledge.

`services/pnl_consumer/pnl_consumer/cold_start.py`:
- `load_or_bootstrap(mode, sink_config, ...) -> AnchorState`
- Sanity-check helpers, each returning `Ok` or `FallbackReason`
- Invariant cross-check function
- Calls `_bootstrap_state` / `_bootstrap_bt_state` on fallback

### Modifications

```
services/pnl_consumer/pnl_consumer/pnl_consumer.py
    - run() calls cold_start.load_or_bootstrap(...) instead of _bootstrap_state directly
    - Flush path: after ClickHouse insert, try checkpoint_store.write_checkpoint(...)
      with try/except, before consumer.commit()
    - Open one psycopg connection per mode at startup, pass via client=

services/pnl_consumer/pyproject.toml
    - Add psycopg[binary] dependency

infra/schemas/streaming_supabase.sql                NEW — schema + tables
infra/terraform/main.tf                             MODIFIED — env vars + Secrets Manager
```

### Config / env vars

| Var | Default | Purpose |
|---|---|---|
| `STREAMING_CHECKPOINT_ENABLED` | `true` | Master switch; `false` reverts to today's behavior |
| `STREAMING_CHECKPOINT_READ_ENABLED` | rollout-controlled | Read separately gated for safe rollout (see Rollout) |
| `STREAMING_CHECKPOINT_INVARIANT_CHECK_ENABLED` | `true` | ClickHouse cross-check at cold start |
| `STREAMING_CHECKPOINT_FRESHNESS_SECONDS` | `86400` | 24h freshness window |
| `SUPABASE_HOST` / `_PORT` / `_USER` / `_DATABASE` / `_SSLMODE` | per env | Connection config |
| `SUPABASE_PASSWORD` | Secrets Manager (`trading-analysis/supabase`) | Credential |

`STREAMING_CHECKPOINT_ENABLED=false` is the emergency kill switch — flip to disable both reads and writes, consumer reverts to bootstrap-only behavior.

## Testing strategy

### Layer 1 — Unit (pytest marker: `unit`)

`tests/unit/test_checkpoint_store.py` — hash + serialization correctness:
- Deterministic hash
- Insertion-order independence
- Sensitivity to `pnl` / `revision_ts` changes
- Rounding-boundary behavior
- Metadata excluded from hash

`tests/unit/test_cold_start_sanity_checks.py` — each check in isolation with fixtures:
- Accept fresh checkpoint
- Reject schema-version mismatch
- Reject staleness
- Reject offset lag
- Reject hash mismatch
- Reject empty strategy set
- Handle missing commit_state row

`tests/unit/test_real_trade_revision_restore.py` — the 7 explicit cases from Real-trade specifics.

`tests/unit/test_state_serialization_roundtrip.py` — property test (hypothesis-driven if available): write → read → hash → must equal originals.

### Layer 2 — Integration (marker: `integration`, requires live Postgres + ClickHouse)

`tests/integration/test_checkpoint_postgres.py`:
- Write creates expected rows
- Write atomicity (failure mid-transaction leaves nothing)
- Concurrent modes don't conflict
- Idempotent UPSERT
- Strategy-removed row remains until pruned (acceptable)

`tests/integration/test_cold_start_invariant_check.py`:
- Pass when position matches `strategy_output_history`
- Fail on tampered position
- Disabled-via-env skips the check

### Layer 3 — End-to-end (marker: `streaming_integration`, Docker compose)

`tests/streaming_integration/test_cold_start_e2e.py`:
- First start falls back to bootstrap (no checkpoint yet)
- Warm restart uses checkpoint (no 48h query issued)
- Supabase outage during flush doesn't block consumer; recovery self-heals checkpoint
- Supabase outage + restart → fallback to bootstrap
- Schema-version bump triggers fallback; first flush writes new-version checkpoint

`tests/streaming_integration/test_real_trade_recovery_e2e.py`:
- Restore + late old revision is rejected
- Crash-restart-resume produces identical final state to no-crash baseline (strongest correctness guarantee)

### Out of scope

- Postgres performance under multi-mode contention (single writer per mode)
- sha256 collision testing
- Dagster reconciliation tests (separate workstream)

## Rollout

### Phase 0 — Scale up bt and real-trade (separate PR, ships first)

`infra/terraform/main.tf`: set `desired_count = 1` for `pnl-consumer-bt` and `pnl-consumer-real-trade`. Both services start cold on the current code path (48h bootstrap). Monitor 48h before proceeding.

### Phase 1 — Schema + infra (no consumer code change)

- Apply `infra/schemas/streaming_supabase.sql` to Supabase manually
- Create Secrets Manager entry `trading-analysis/supabase`
- Terraform: inject `SUPABASE_*` env vars and `STREAMING_CHECKPOINT_*` flags into the four pnl-consumer task definitions
- `terraform apply` — no consumer behavior change yet

### Phase 2 — Shadow mode (write-only)

Deploy consumer code with `STREAMING_CHECKPOINT_ENABLED=true`, `STREAMING_CHECKPOINT_READ_ENABLED=false`. Writes happen, reads do not. After 24h validate:

- `pnl_checkpoint` row counts match expected strategy counts per mode
- Sampled `(pnl, price, position)` matches latest ClickHouse pnl rows
- Hashes recompute correctly
- For real-trade: sampled `(bar_ts, revision_ts)` matches latest accepted revision in `strategy_output_history_v2`
- `streaming_checkpoint_write_failures_total` near zero

### Phase 3 — Enable reads, prod first

Flip `STREAMING_CHECKPOINT_READ_ENABLED=true` for `pnl-consumer-prod` only. Force cold start. Verify:

- Log shows `cold-start: checkpoint-restored, mode=prod`
- No 48h bootstrap query issued
- First flush writes fresh checkpoint
- No PnL discrepancies vs baseline

Watch 48h.

### Phase 4 — Enable reads for bt, then real-trade

- Flip `pnl-consumer-bt`, watch 48h.
- Flip `pnl-consumer-real-trade`, watch 72h. Sample late revisions to confirm guard correctness after restore.

### Phase 5 — Steady state

- Collapse `STREAMING_CHECKPOINT_READ_ENABLED` into `STREAMING_CHECKPOINT_ENABLED`
- Keep master kill switch indefinitely
- Keep 48h bootstrap fallback indefinitely
- Hand off Dagster reconciliation job to its own design

### Monitoring (must exist before Phase 2)

| Signal | Threshold | Action |
|---|---|---|
| `streaming_checkpoint_write_failures_total` rate | > 0 over 5 min | Page on-call |
| `streaming_checkpoint_last_successful_write_seconds` | > 300 | Page on-call |
| Cold-start `fallback-bootstrap` after Phase 4 | Any | Slack alert |
| ClickHouse query rate on `strategy_pnl_1min_*_v2` | Step-down after Phase 3 | Confirms feature working |

Grafana dashboard: `infra/grafana/dashboards/streaming-checkpoint.json`.

### Rollback (any phase)

1. `STREAMING_CHECKPOINT_ENABLED=false` in affected task definition
2. `terraform apply` → ECS rolling update
3. Consumer reverts to 48h bootstrap behavior
4. Checkpoint rows in Supabase remain (harmless)

Time to revert: ~5 min.

## What we are NOT changing

- The 48h bootstrap path itself — unchanged code, always the fallback
- `analytics.strategy_pnl_1min_*_v2` schema or write logic
- Kafka topic / consumer group config
- Dagster assets

## Open follow-ups (not in this design)

- Dagster periodic reconciliation job — separate workstream, will use `libs/computation/checkpoint_store.read_checkpoint`
- Periodic GC of stale `pnl_checkpoint` rows (strategies removed from `strategy_output_history` > 14 days ago)
