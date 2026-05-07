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
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_bt_v2
(
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_real_trade_v2
(
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now(),
    closing_ts          DateTime,
    execution_ts        DateTime,
    traded              Bool     DEFAULT false
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
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now()
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
--       now()                           AS updated_at
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
    now()                           AS updated_at
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_prod_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts);

-- 4b. bt ──────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1hour_bt_v2
(
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now()
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
--       now()                           AS updated_at
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
    now()                           AS updated_at
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_bt_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts);

-- 4c. real_trade ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1hour_real_trade_v2
(
    strategy_table_name String,
    strategy_id         Int32,
    strategy_name       String,
    underlying          String,
    config_timeframe    String,
    source              String,
    version             String,
    ts                  DateTime,
    cumulative_pnl      Nullable(Float64),
    benchmark           Float64,
    position            Float64,
    price               Float64,
    final_signal        Float64,
    weighting           Float64,
    updated_at          DateTime DEFAULT now(),
    closing_ts          DateTime,
    execution_ts        DateTime,
    traded              Bool     DEFAULT false
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts);

-- Backfill:
--   INSERT INTO analytics.strategy_pnl_1hour_real_trade_v2
--   SELECT
--       strategy_table_name, strategy_id, strategy_name, underlying,
--       config_timeframe, source, version,
--       toStartOfHour(src_ts)             AS ts,
--       argMax(cumulative_pnl, src_ts)    AS cumulative_pnl,
--       argMax(benchmark, src_ts)         AS benchmark,
--       argMax(position, src_ts)          AS position,
--       argMax(price, src_ts)             AS price,
--       argMax(final_signal, src_ts)      AS final_signal,
--       argMax(weighting, src_ts)         AS weighting,
--       now()                             AS updated_at,
--       argMax(closing_ts, src_ts)        AS closing_ts,
--       argMax(execution_ts, src_ts)      AS execution_ts,
--       any(traded)                       AS traded
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
    toStartOfHour(src_ts)             AS ts,
    argMax(cumulative_pnl, src_ts)    AS cumulative_pnl,
    argMax(benchmark, src_ts)         AS benchmark,
    argMax(position, src_ts)          AS position,
    argMax(price, src_ts)             AS price,
    argMax(final_signal, src_ts)      AS final_signal,
    argMax(weighting, src_ts)         AS weighting,
    now()                             AS updated_at,
    argMax(closing_ts, src_ts)        AS closing_ts,
    argMax(execution_ts, src_ts)      AS execution_ts,
    any(traded)                       AS traded
FROM (SELECT *, ts AS src_ts FROM analytics.strategy_pnl_1min_real_trade_v2)
GROUP BY
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, toStartOfHour(src_ts);

