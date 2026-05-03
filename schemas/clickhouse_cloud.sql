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
-- 4. PnL Rollup Tables (Hourly)
-- ─────────────────────────────────────────────────────────────────────────────

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

