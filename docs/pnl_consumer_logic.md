# PnL Consumer Logic

## Overview

The PnL consumer is a Kafka consumer that reads 1-minute closed candles from `binance.price.ticks` and computes running cumulative PnL per strategy, writing results to ClickHouse in real-time.

Three sink modes operate independently:
- **prod**: production strategy positions (first revision per bar)
- **bt**: backtest strategy positions (first revision per bar)
- **real_trade**: all revisions per bar, applied at `execution_ts`

---

## Core Formula

```
new_pnl = prev_pnl + position * (current_price - prev_price) / prev_price
```

- `position` always comes from `strategy_output_history_v2` (prod/real_trade) or `strategy_output_history_bt_v2` (bt) — never from the PnL table
- `price` always comes from `futures_price_1min` or the Redpanda candle's `open` field — never from the PnL table's `price` column

---

## Cold-Start Bootstrap (Prod and BT)

### Step 1 — Get `reference_ts`

Read the Kafka committed offset for this consumer group. Decode the candle message at that offset to get `reference_ts` (the candle `ts` the consumer will replay from).

- If no committed offset exists (first-ever start): fall back to latest Kafka message.
- If Kafka is unreachable: fall back to `now()`.

### Step 2 — Compute `start_ts`

```
start_ts = reference_ts - 3 days
```

### Step 3 — Seed anchor state at `start_ts`

For each `strategy_instance_id`:

**PnL + Price baseline** (from ClickHouse):
- Query `strategy_pnl_1min_prod_v2` (or `_bt_v2`) for the latest row with `ts < start_ts`
- Get `cumulative_pnl` from that row
- Get `price` from `futures_price_1min` at the same minute (NOT from the PnL table)
- If no PnL row exists: `cumulative_pnl = 0.0`, `price = 0.0`

**Position** (from history, always authoritative):
- Query `strategy_output_history_v2` (or `_bt_v2`) for the latest bar with `ts <= start_ts`
- Take the **first revision** (`argMin(row_json, revision_ts)`) — subsequent revisions for the same bar are discarded
- If no bar exists: `position = 0.0`

**Why position comes from history, not the PnL table:**
The PnL table's position column reflects what position was applied when that row was written, which may differ from the authoritative position in the strategy history (e.g. if a Dagster backfill rewrote rows). The history table is always the source of truth for position.

**Late-arrival note:** All bars arrive late. A bar with `ts = 01:00` and `closing_ts = 01:10` (for a 10-min timeframe) may not appear in `strategy_output_history_v2` until `01:13`. This is expected — the seed query with `ts <= start_ts` will only see bars that have already arrived, which is correct. The consumer's position at `start_ts` is the last *known* position; it updates naturally when late bars arrive during streaming.

### Step 4 — Walk `[start_ts, reference_ts)` and verify

For each stored PnL row in `[start_ts, reference_ts)` (excluding `reference_ts`):

1. **Price**: read from `futures_price_1min` at `row.ts`
2. **Position**: find the latest bar in history with `ts <= row.ts` and take first revision — this is the position that *was active* at that minute
3. **Recompute**: `recomputed = prev_pnl + position * (price - prev_price) / prev_price`
4. **Compare** against stored `cumulative_pnl`:
   - Deviation `> 0.2%`: crash — indicates real corruption
   - Deviation `> 1e-6` but `<= 0.2%`: log warning — expected from Dagster REST-price backfills vs WebSocket prices
5. **Advance chain** using stored value (not recomputed) to stay in sync with ClickHouse

After the walk, seed `AnchorState` with the final `(pnl, price, position)` per strategy.

### Dedup set

During the walk, each `(strategy_instance_id, bar_ts)` pair that was processed is added to a **seen set**. This set carries into the live streaming loop to prevent duplicate position changes for bars already processed during bootstrap.

---

## Live Streaming Loop (Prod and BT)

On each candle received from Kafka:

1. Re-query `strategy_output_history_v2` (or `_bt_v2`) for the active bar per `strategy_instance_id`:
   - Latest bar whose `closing_ts (= ts + tf_minutes) <= candle_ts`
   - Within a 1-day lookback window
   - First revision only (`argMin(row_json, revision_ts)`)

2. **Dedup check**: for each result, check `(strategy_instance_id, bar_ts)` against the seen set:
   - If already seen: skip (this bar's position was already applied, we don't want to reapply it)
   - If new: add to seen set, apply position

3. Compute PnL via `AnchorState.compute_pnl(strategy_table_name, candle.open, position)`

4. **Lazy-seed fallback**: if `AnchorState` has no entry for a strategy (brand-new strategy that appeared after bootstrap):
   - Try to fetch latest PnL row from ClickHouse as seed
   - If none: seed from `(pnl=0, price=candle.open, position=0)`

5. Accumulate rows; flush to ClickHouse when batch size reaches `FLUSH_EVERY` (1000 rows)

### Why re-query every candle?

Positions change when new bars arrive in `strategy_output_history_v2` (late arrivals). Re-querying each candle ensures the consumer picks up position changes as soon as they appear, without needing an explicit notification or change-data-capture mechanism.

### Dedup rule detail

The seen set stores `(strategy_instance_id, bar_ts)` pairs. If two revisions for the same `strategy_instance_id` and `ts` arrive (e.g., a position correction), we only apply the **first** `revision_ts`. This is enforced by:
1. `argMin(row_json, revision_ts)` in the SQL query (first revision wins at query time)
2. The seen set check in the consumer loop (once a bar's position has been applied, it is not re-applied even if subsequent candle queries return the same bar)

---

## Real-Trade Mode

Real-trade differs from prod/bt in that **multiple revisions per bar are accepted**, each applied at the minute it took effect:

```
execution_ts = toStartOfMinute(revision_ts + 59s)
```

A revision is active if `execution_ts <= candle_ts`. The latest active revision per strategy governs the position for that candle. Strategies with no active revision carry forward their previous revision.

Cold-start seeding for real_trade:
- Same `reference_ts` anchor
- Seed `last_real_trade_revisions` from ClickHouse: the last active revision per strategy as of `reference_ts`
- No position seen-set for real_trade (carry-forward handles continuity)

---

## Data Sources Summary

| Data | Source |
|------|--------|
| Candle price | Redpanda `binance.price.ticks` (`open` field) |
| Historical price (bootstrap/walk) | `analytics.futures_price_1min` |
| Position (always) | `strategy_output_history_v2` / `_bt_v2` |
| PnL baseline (bootstrap seed) | `analytics.strategy_pnl_1min_prod_v2` / `_bt_v2` (`cumulative_pnl` only) |
| PnL output (prod) | `analytics.strategy_pnl_1min_prod_v2` |
| PnL output (bt) | `analytics.strategy_pnl_1min_bt_v2` |
| PnL output (real_trade) | `analytics.strategy_pnl_1min_real_trade_v2` |

---

## Key Invariants

1. **Position is never read from the PnL table.** Only `strategy_output_history_*` is authoritative for position.
2. **Price is never read from the PnL table.** Price comes from `futures_price_1min` (bootstrap/walk) or Redpanda candle `open` (live).
3. **First revision per bar wins** for prod and bt. `argMin(row_json, revision_ts)` in SQL + seen-set in consumer.
4. **Seen-set persists from bootstrap into live loop.** A bar processed during the 3-day walk is not re-applied when the live loop re-queries the same bar.
5. **Walk uses `[start_ts, reference_ts)`.** `reference_ts` itself is excluded — the consumer will process that candle live from Kafka.
6. **Bootstrap does not crash on empty ClickHouse.** If no prior PnL data exists, strategies are seeded from zero and lazy-seeded as they first appear.
