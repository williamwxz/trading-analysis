# Incident — Broker Spot Reclaims Wedge PnL Consumers (2026-07-13)

**Impact:** `pnl-consumer-prod`, `-bt`, and `-price` stopped writing PnL/price rows
from 2026-07-13 ~20:29 UTC until manual redeploys on 2026-07-14 ~17:00 UTC (~20 h).
`ws-consumer` (producer) and `-real-trade` recovered on their own. Data was
recoverable via `audit_pnl.py --fix-window` + `backfill_prices`.

## Timeline (UTC, 2026-07-13)

| Time | Event |
|------|-------|
| 20:22–20:26 | Redpanda task `36e630a5` and ws-consumer task killed — same Fargate Spot sweep |
| 20:27–20:29 | ECS starts replacement ws-consumer + redpanda tasks; new broker = **new cluster** (ephemeral storage → new ClusterId + new topic ids) |
| 20:29:58 | Consumers log `PARTCNT … partition count changed from 1 to 0` and `CLUSTERID … reports different ClusterId` — then silence; `poll()` returns `None` forever |
| 21:50 | Second Spot reclaim kills replacement broker task `b5111be0` (81 min old); same wedge signature repeats |
| next day 17:00 | Manual consumer redeploys restore flow |

## Root cause

1. **Restarts = Fargate Spot reclaims, not memory pressure.** The ws-consumer's
   replacement task started (20:27:51) *before* its predecessor stopped logging
   (20:28:16) — replace-before-kill only happens with a Spot interruption notice.
   Both broker tasks died mid-flight with clean INFO logs to the last second (a
   Seastar OOM logs `bad_alloc`; a cgroup kill can't explain the correlated
   ws-consumer reclaim). June history shows near-daily reclaims; the 18-day quiet
   spell before 7/13 was the outlier. The 7/12 right-size to 256 CPU / 1 GB
   (PR #59) is coincidental — observed usage is ~240 MB and the post-incident task
   ran >21 h at 1 GB. **Keep the sizing.**
2. **librdkafka treats a ClusterId change as unrecoverable but only WARNs.** The
   recreated broker has the same DNS name but a new ClusterId and new topic ids.
   librdkafka logs `CLUSTERID` / `PARTCNT → 0` at warn level on its internal
   logger, stops fetching, and never surfaces an error event through `poll()` —
   the consumer loop saw no exception and wedged silently. The prior assumption
   ("broker replacement is routine; consumers just redeliver", PR #59 runbook)
   held for the *producer* but not for consumer group clients.

## Fix (this PR)

Crash-to-recover in `services/pnl_consumer/pnl_consumer/pnl_consumer.py`:

- librdkafka logs are routed to Python logging (`pnl_consumer.rdkafka`);
  `RdkafkaWedgeDetector` flags `CLUSTERID` / `PARTCNT → 0` lines and the poll
  loop exits non-zero (`ConsumerWedgedError`, logged at CRITICAL) so ECS
  replaces the task. A fresh client bootstraps cleanly on the new cluster —
  `peek_reference_ts` already handles a group with no committed offsets, and
  ClickHouse writes are idempotent (ReplacingMergeTree).
- `PollSilenceWatchdog` backstop: if `poll()` yields nothing for
  `MAX_POLL_SILENCE_MINUTES` (default 30, `0` disables), exit the same way —
  covers wedge modes without a known log signature. The topic normally carries
  ~8 msgs/min around the clock.

## Ops notes

- Wedge exits are greppable: `Kafka client wedged` (CRITICAL) in
  `/ecs/trading-analysis` `pnl-consumer-*` streams.
- Expect one consumer task restart per broker replacement (Spot reclaim or
  deploy). Restart ≈ bootstrap (~2 min) + resume from committed offset.
- A crash-*loop* (restarts every few minutes) means bootstrap itself is failing
  (ClickHouse unreachable, empty topic) — investigate the dependency, don't
  fight the loop; it self-heals when the dependency returns.
- If consumers ever wedge again without these log lines, capture the ECS
  stopped-task record within ~1 h (`aws ecs describe-tasks`) — it expires, and
  losing `stoppedReason` cost us the OOM-vs-Spot distinction this time.
