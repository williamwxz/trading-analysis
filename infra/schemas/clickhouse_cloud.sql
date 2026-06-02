-- ============================================================================
-- ClickHouse Cloud Schema — trading-analysis (Optimized)
-- ============================================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Source Tables (Input from external strategy execution)
-- ─────────────────────────────────────────────────────────────────────────────

-- Production strategy output history
CREATE TABLE IF NOT EXISTS analytics.strategy_output_history_v2
(
    strategy_table_name  String,
    config_timeframe     String,
    ts                   DateTime,
    revision_id          UUID,
    revision_ts          DateTime,
    strategy_id          Int32,
    strategy_no          Int32,
    strategy_instance_id String,
    underlying           String,
    strategy_name        String,
    freq                 String,
    weighting            Float64,
    row_hash             String,
    row_json             String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, config_timeframe, ts, revision_ts);

-- Backtest strategy output history
CREATE TABLE IF NOT EXISTS analytics.strategy_output_history_bt_v2
(
    strategy_table_name  String,
    config_timeframe     String,
    ts                   DateTime,
    revision_id          UUID,
    revision_ts          DateTime,
    strategy_id          Int32,
    strategy_no          Int32,
    strategy_instance_id String,
    underlying           String,
    strategy_name        String,
    freq                 String,
    weighting            Float64,
    row_hash             String,
    row_json             String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, config_timeframe, ts, revision_ts);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Market Data
--    Real-time: pnl_consumer (Kafka → ClickHouse, via ws_consumer WebSocket feed)
--    Backfill:  Dagster binance_futures_backfill (daily partitioned)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.futures_price_1min
(
    exchange   String,
    instrument String,
    ts         DateTime,
    open       Float64,
    high       Float64,
    low        Float64,
    close      Float64,
    volume     Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, instrument, ts);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. PnL Result Tables (1-min)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_prod_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_bt_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_real_trade_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. PnL Rollup Tables (Hourly) + Materialized Views
--
-- Each 1hour target table is populated by a Materialized View that fires on every
-- INSERT into the corresponding 1min table (from pnl_consumer streaming flushes
-- and Dagster batch backfills). The MV aggregates the inserted block only — not
-- a full table scan — so per-flush cost is low regardless of write frequency.
--
-- Because each flush covers a partial hour, multiple MV writes land per hourly
-- bucket. ReplacingMergeTree(updated_at) on the target deduplicates on background
-- merge. Always query with FINAL or LIMIT 1 BY to get clean results immediately.
--
-- One-time backfill required after creating each MV (populates history):
--   INSERT INTO analytics.strategy_pnl_1hour_{variant}_v2
--   SELECT ... FROM analytics.strategy_pnl_1min_{variant}_v2 FINAL
--   GROUP BY ..., toStartOfHour(ts);
-- (Full backfill queries are shown inline below each MV.)
-- ─────────────────────────────────────────────────────────────────────────────

-- 4a. prod ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1hour_prod_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1hour_prod_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfHour(src_ts)           AS ts,
--       argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
--       argMax(benchmark, src_ts)       AS benchmark,
--       argMax(position, src_ts)        AS position,
--       argMax(price, src_ts)           AS price,
--       argMax(final_signal, src_ts)    AS final_signal,
--       argMax(weighting, src_ts)       AS weighting,
--       now()                           AS updated_at,
--       any(strategy_instance_id)       AS strategy_instance_id
--   FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_prod_v2 FINAL)
--   GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
--            config_timeframe, source, version, toStartOfHour(src_ts);
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pnl_1hour_prod_mv
TO analytics.strategy_pnl_1hour_prod_v2
AS
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfHour(src_ts)           AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_prod_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts);

-- 4b. bt ──────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1hour_bt_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1hour_bt_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfHour(src_ts)           AS ts,
--       argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
--       argMax(benchmark, src_ts)       AS benchmark,
--       argMax(position, src_ts)        AS position,
--       argMax(price, src_ts)           AS price,
--       argMax(final_signal, src_ts)    AS final_signal,
--       argMax(weighting, src_ts)       AS weighting,
--       now()                           AS updated_at,
--       any(strategy_instance_id)       AS strategy_instance_id
--   FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_bt_v2 FINAL)
--   GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
--            config_timeframe, source, version, toStartOfHour(src_ts);
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pnl_1hour_bt_mv
TO analytics.strategy_pnl_1hour_bt_v2
AS
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfHour(src_ts)           AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_bt_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts);

-- 4c. real_trade ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1hour_real_trade_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1hour_real_trade_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfHour(src_ts)           AS ts,
--       argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
--       argMax(benchmark, src_ts)       AS benchmark,
--       argMax(position, src_ts)        AS position,
--       argMax(price, src_ts)           AS price,
--       argMax(final_signal, src_ts)    AS final_signal,
--       argMax(weighting, src_ts)       AS weighting,
--       now()                           AS updated_at,
--       any(strategy_instance_id)       AS strategy_instance_id
--   FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_real_trade_v2 FINAL)
--   GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
--            config_timeframe, source, version, toStartOfHour(src_ts);
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pnl_1hour_real_trade_mv
TO analytics.strategy_pnl_1hour_real_trade_v2
AS
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfHour(src_ts)           AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_real_trade_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts);

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. PnL Rollup Tables (Daily) + Materialized Views
--
-- Each 1day target table is populated by a Materialized View that fires on
-- every INSERT into the corresponding 1hour table. Because ClickHouse cascades
-- MV triggers, writes from the upstream 1hour MV (driven by pnl_consumer
-- streaming flushes and Dagster batch backfills) and from the
-- pnl_hourly_rollup Dagster asset both fan out into the 1day MV automatically.
--
-- Each MV fire aggregates only the inserted block, not the full 1hour table —
-- per-flush cost is low regardless of write frequency. Each daily slot
-- accumulates multiple ReplacingMergeTree(updated_at) rows (one per upstream
-- write); background merge deduplicates. Always query with FINAL or
-- LIMIT 1 BY (key) ORDER BY updated_at DESC for immediate clean results.
--
-- argMax-of-argMax soundness: each 1hour row already represents the last
-- 1min observation in that hour. Taking argMax again across hours within a
-- day yields the last hourly row, which equals the last 1min observation of
-- the day — i.e. an end-of-day snapshot, consistent with the 1hour pattern.
--
-- One-time backfill required after creating each MV (populates history):
--   INSERT INTO analytics.strategy_pnl_1day_{variant}_v2
--   SELECT ... FROM analytics.strategy_pnl_1hour_{variant}_v2 FINAL
--   GROUP BY ..., toStartOfDay(ts);
-- (Full backfill queries are shown inline below each MV.)
-- ─────────────────────────────────────────────────────────────────────────────

-- 5a. prod ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1day_prod_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1day_prod_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfDay(src_ts)            AS ts,
--       argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
--       argMax(benchmark, src_ts)       AS benchmark,
--       argMax(position, src_ts)        AS position,
--       argMax(price, src_ts)           AS price,
--       argMax(final_signal, src_ts)    AS final_signal,
--       argMax(weighting, src_ts)       AS weighting,
--       now()                           AS updated_at,
--       any(strategy_instance_id)       AS strategy_instance_id
--   FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_prod_v2 FINAL)
--   GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
--            config_timeframe, source, version, toStartOfDay(src_ts);
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pnl_1day_prod_mv
TO analytics.strategy_pnl_1day_prod_v2
AS
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfDay(src_ts)            AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_prod_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfDay(src_ts);

-- 5b. bt ──────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1day_bt_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1day_bt_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfDay(src_ts)            AS ts,
--       argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
--       argMax(benchmark, src_ts)       AS benchmark,
--       argMax(position, src_ts)        AS position,
--       argMax(price, src_ts)           AS price,
--       argMax(final_signal, src_ts)    AS final_signal,
--       argMax(weighting, src_ts)       AS weighting,
--       now()                           AS updated_at,
--       any(strategy_instance_id)       AS strategy_instance_id
--   FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_bt_v2 FINAL)
--   GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
--            config_timeframe, source, version, toStartOfDay(src_ts);
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pnl_1day_bt_mv
TO analytics.strategy_pnl_1day_bt_v2
AS
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfDay(src_ts)            AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_bt_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfDay(src_ts);

-- 5c. real_trade ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1day_real_trade_v2
(
    strategy_table_name  String,
    strategy_id          Int32,
    strategy_name        String,
    underlying           String,
    config_timeframe     String,
    source               String,
    version              String,
    ts                   DateTime,
    cumulative_pnl       Nullable(Float64),
    benchmark            Float64,
    position             Float64,
    price                Float64,
    final_signal         Float64,
    weighting            Float64,
    updated_at           DateTime DEFAULT now(),
    strategy_instance_id String   DEFAULT ''
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1day_real_trade_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfDay(src_ts)            AS ts,
--       argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
--       argMax(benchmark, src_ts)       AS benchmark,
--       argMax(position, src_ts)        AS position,
--       argMax(price, src_ts)           AS price,
--       argMax(final_signal, src_ts)    AS final_signal,
--       argMax(weighting, src_ts)       AS weighting,
--       now()                           AS updated_at,
--       any(strategy_instance_id)       AS strategy_instance_id
--   FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_real_trade_v2 FINAL)
--   GROUP BY strategy_table_name, strategy_id, strategy_name, underlying,
--            config_timeframe, source, version, toStartOfDay(src_ts);
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pnl_1day_real_trade_mv
TO analytics.strategy_pnl_1day_real_trade_v2
AS
SELECT
    strategy_table_name,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    source,
    version,
    toStartOfDay(src_ts)            AS ts,
    argMax(cumulative_pnl, src_ts)  AS cumulative_pnl,
    argMax(benchmark, src_ts)       AS benchmark,
    argMax(position, src_ts)        AS position,
    argMax(price, src_ts)           AS price,
    argMax(final_signal, src_ts)    AS final_signal,
    argMax(weighting, src_ts)       AS weighting,
    now()                           AS updated_at,
    any(strategy_instance_id)       AS strategy_instance_id
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1hour_real_trade_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfDay(src_ts);

-- ─────────────────────────────────────────────────────────────────────────────
-- strategy_pause_events — one row per paused strategy table per pause event
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.strategy_pause_events
(
    event_uuid                       String,
    event_ts_utc                     DateTime64(6, 'UTC'),
    sid                              String,
    trace_text_paused_strategy_table String
)
ENGINE = MergeTree()
ORDER BY (event_ts_utc, sid, event_uuid);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.strategy_pause_events_mv
TO analytics.strategy_pause_events
AS
SELECT
    event_uuid,
    parseDateTime64BestEffort(event_ts_utc) AS event_ts_utc,
    extract(pst, 'sid=([^|]+)')             AS sid,
    pst                                     AS trace_text_paused_strategy_table
FROM analytics.errors_history
ARRAY JOIN JSONExtract(traceback_text, 'paused_strategy_tables', 'Array(String)') AS pst
WHERE category = 'strategy_pause'
  AND traceback_text != '';

