# PnL Consumer Logic

## Overview

The PnL consumer is a Kafka consumer that reads 1-minute closed candles from `binance.price.ticks` and computes running cumulative PnL per strategy, writing results to ClickHouse in real-time.

Three sink modes operate independently:
- **prod**: production strategy positions (first revision per bar)
- **bt**: backtest strategy positions (first revision per bar)
- **real_trade**: latest revision per bar, applied as soon as `revision_ts <= candle_ts`

---

## Core Formula

```
new_pnl = prev_pnl + position * (current_price - prev_price) / prev_price
```

- `position` always comes from `strategy_output_history_v2` (prod/real_trade) or `strategy_output_history_bt_v2` (bt) — never from the PnL table
- `price` always comes from `futures_price_1min` or the Redpanda candle's `open` field — never from the PnL table's `price` column

---

## Cold-Start Bootstrap (All Modes)

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
- Query `strategy_pnl_1min_prod_v2` (or `_bt_v2` / `_real_trade_v2`) for the latest row with `ts < start_ts`
- Get `cumulative_pnl` from that row
- Get `price` from `futures_price_1min` at the same minute (NOT from the PnL table)
- If no PnL row exists: `cumulative_pnl = 0.0`, `price = 0.0`

**Position** (from history, always authoritative):
- Prod/BT: latest bar with `ts <= start_ts`, first revision only (`argMin(row_json, revision_ts)`)
- Real-trade: latest revision with `revision_ts <= start_ts` per `strategy_instance_id`
- If no bar/revision exists: `position = 0.0`

**Why position comes from history, not the PnL table:**
The PnL table's position column reflects what position was applied when that row was written, which may differ from the authoritative position in the strategy history (e.g. if a Dagster backfill rewrote rows). The history table is always the source of truth for position.

**Late-arrival note:** All bars arrive late. A bar with `ts = 01:00` and `closing_ts = 01:10` (for a 10-min timeframe) may not appear in `strategy_output_history_v2` until `01:13`. This is expected — the seed query with `ts <= start_ts` will only see bars that have already arrived, which is correct. The consumer's position at `start_ts` is the last *known* position; it updates naturally when late bars arrive during streaming.

### Step 4 — Walk `[start_ts, reference_ts)` and verify

For each stored PnL row in `[start_ts, reference_ts)` (excluding `reference_ts`):

1. **Price**: read from `futures_price_1min` at `row.ts`
2. **Position**:
   - Prod/BT: latest bar in history with `ts <= row.ts`, first revision only
   - Real-trade: latest revision with `revision_ts <= row.ts` per `strategy_instance_id`
3. **Recompute**: `recomputed = prev_pnl + position * (price - prev_price) / prev_price`
4. **Compare** against stored `cumulative_pnl`:
   - Deviation `> 0.2%`: crash — indicates real corruption
   - Deviation `> 1e-6` but `<= 0.2%`: log warning — expected from Dagster REST-price backfills vs WebSocket prices
5. **Advance chain** using stored value (not recomputed) to stay in sync with ClickHouse

After the walk, seed `AnchorState` with the final `(pnl, price, position, bar_ts, revision_ts)` per strategy.

### Dedup set (prod/bt only)

During the walk, each `(strategy_instance_id, bar_ts)` pair that was processed is added to a **seen set**. This set carries into the live streaming loop to prevent duplicate position changes for bars already processed during bootstrap.

Real-trade does not use a seen set — the `AnchorState` revision guard handles continuity (see below).

---

## Live Streaming Loop — Prod and BT

On each candle received from Kafka:

1. Re-query `strategy_output_history_v2` (or `_bt_v2`) for the active bar per `strategy_instance_id`:
   - Latest bar whose `closing_ts (= ts + tf_minutes) <= candle_ts`
   - Within a 1-day lookback window
   - First revision only (`argMin(row_json, revision_ts)`)

2. **Dedup check**: for each result, check `(strategy_instance_id, bar_ts)` against the seen set:
   - If already seen: skip (this bar's position was already applied)
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

## Live Streaming Loop — Real-Trade

Real-trade accepts position changes from any revision as soon as it appears in `strategy_output_history_v2`. The unit of change is the **revision**, not the bar.

On each candle received from Kafka:

1. Re-query `strategy_output_history_v2` for the latest revision per `strategy_instance_id`:
   - Latest revision whose `revision_ts <= candle_ts`
   - No `closing_ts` gate — a revision applies as soon as it is visible, regardless of bar close time
   - Within a 1-day lookback window

2. **AnchorState revision guard**: for each result, compare `(bar_ts, revision_ts)` against what is stored in `AnchorState`:
   - `bar_ts < anchor.bar_ts` → **ignore** — a newer bar is already active; this is a stale late revision for an old bar
   - `bar_ts == anchor.bar_ts` and `revision_ts <= anchor.revision_ts` → **ignore** — same bar, not a newer revision
   - `bar_ts > anchor.bar_ts` OR `(bar_ts == anchor.bar_ts` and `revision_ts > anchor.revision_ts)` → **apply**

   Equivalently: apply if `(bar_ts, revision_ts) > (anchor.bar_ts, anchor.revision_ts)` as a tuple comparison.

3. Compute PnL via `AnchorState.compute_pnl(strategy_table_name, candle.open, position)`

4. **Lazy-seed fallback**: same as prod/bt — seed from ClickHouse or zero on first appearance.

5. Flush when batch size reaches `FLUSH_EVERY`.

### Why `bar_ts` matters for the guard

`revision_ts` alone is not sufficient: a late revision for bar `ts=01:00` could arrive at `revision_ts=03:30` after bar `ts=02:00` is already active. Comparing only `revision_ts` would incorrectly treat this as a new position. The `bar_ts` check ensures that once a newer bar is active, revisions for older bars are silently dropped.

### AnchorRecord fields for real_trade

`AnchorRecord` stores `(pnl, price, position, bar_ts, revision_ts)` for real_trade strategies. `bar_ts` and `revision_ts` default to `datetime.min` so the first revision always passes the guard.

---

## Data Sources Summary

| Data | Source |
|------|--------|
| Candle price | Redpanda `binance.price.ticks` (`open` field) |
| Historical price (bootstrap/walk) | `analytics.futures_price_1min` |
| Position (always) | `strategy_output_history_v2` / `_bt_v2` |
| PnL baseline (bootstrap seed) | `analytics.strategy_pnl_1min_prod_v2` / `_bt_v2` / `_real_trade_v2` (`cumulative_pnl` only) |
| PnL output (prod) | `analytics.strategy_pnl_1min_prod_v2` |
| PnL output (bt) | `analytics.strategy_pnl_1min_bt_v2` |
| PnL output (real_trade) | `analytics.strategy_pnl_1min_real_trade_v2` |

---

## Key Invariants

1. **Position is never read from the PnL table.** Only `strategy_output_history_*` is authoritative for position.
2. **Price is never read from the PnL table.** Price comes from `futures_price_1min` (bootstrap/walk) or Redpanda candle `open` (live).
3. **First revision per bar wins** for prod and bt. `argMin(row_json, revision_ts)` in SQL + seen-set in consumer.
4. **Seen-set persists from bootstrap into live loop** (prod/bt only). A bar processed during the 3-day walk is not re-applied when the live loop re-queries the same bar.
5. **Walk uses `[start_ts, reference_ts)`.** `reference_ts` itself is excluded — the consumer will process that candle live from Kafka.
6. **Bootstrap does not crash on empty ClickHouse.** If no prior PnL data exists, strategies are seeded from zero and lazy-seeded as they first appear.
7. **Real-trade revision guard**: `(bar_ts, revision_ts)` tuple comparison in `AnchorState` prevents stale late revisions from overwriting a newer bar's position.
8. **Bootstrap walk applies to all three modes.** Real-trade walk resolves position as "latest revision with `revision_ts <= row.ts`" rather than first revision.
