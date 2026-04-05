-- ============================================================================
-- ClickHouse Cloud Schema — trading-analysis
-- ============================================================================
-- Simplified from falcon-lakehouse for ClickHouse Cloud:
--   - No Replicated* engines (Cloud handles replication transparently)
--   - No Kafka engine tables (Dagster polls Binance API directly)
--   - No bronze database (market data goes directly to analytics)
--   - V2 PnL tables only (no v1)
--
-- Apply: clickhouse-client --host <cloud-host> --secure --password <pw> < clickhouse_cloud.sql
-- ============================================================================


-- ============================================================================
-- SECTION 1: Database
-- ============================================================================

CREATE DATABASE IF NOT EXISTS analytics;


-- ============================================================================
-- SECTION 2: Source tables (written by external strategy execution service)
-- ============================================================================

-- Production strategy output — all bar revisions with full JSON payload
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
ORDER BY (strategy_table_name, config_timeframe, ts, revision_ts)
SETTINGS index_granularity = 8192;

-- Backtest strategy output
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
ORDER BY (strategy_table_name, config_timeframe, ts, revision_ts)
SETTINGS index_granularity = 8192;

-- Deduplicated latest view of each bar (prod)
CREATE TABLE IF NOT EXISTS analytics.strategy_output_latest_v2
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
ENGINE = ReplacingMergeTree(revision_ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, config_timeframe, ts)
SETTINGS index_granularity = 8192;

-- Deduplicated latest view of each bar (backtest)
CREATE TABLE IF NOT EXISTS analytics.strategy_output_latest_bt_v2
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
ENGINE = ReplacingMergeTree(revision_ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, config_timeframe, ts)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- SECTION 3: Market data (populated by Dagster binance_futures_ohlcv_1min asset)
-- ============================================================================

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
ORDER BY (exchange, instrument, ts)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- SECTION 4: PnL 1-min tables (v2 only — anchor-chained)
-- ============================================================================

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
ORDER BY (strategy_table_name, ts)
SETTINGS index_granularity = 8192;

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
ORDER BY (strategy_table_name, ts)
SETTINGS index_granularity = 8192;

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
ORDER BY (strategy_table_name, ts)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- SECTION 5: PnL rollup tables (hourly, v2 only)
-- ============================================================================

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
ORDER BY (strategy_table_name, ts)
SETTINGS index_granularity = 8192;

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
ORDER BY (strategy_table_name, ts)
SETTINGS index_granularity = 8192;

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
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (strategy_table_name, ts)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- SECTION 6: Watermark tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.pnl_refresh_watermarks
(
    underlying       String,
    target_table     String,
    last_revision_ts DateTime,
    updated_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (underlying, target_table)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- SECTION 7: Query-time views
-- ============================================================================

-- Unified view across all v2 1-min PnL tables
CREATE VIEW IF NOT EXISTS analytics.strategy_pnl_1min_v2 AS
SELECT *, 'prod' AS pnl_type FROM analytics.strategy_pnl_1min_prod_v2 FINAL
UNION ALL
SELECT *, 'backtest' AS pnl_type FROM analytics.strategy_pnl_1min_bt_v2 FINAL
UNION ALL
SELECT
    strategy_table_name, strategy_id, strategy_name, underlying,
    config_timeframe, source, version, ts, cumulative_pnl, benchmark,
    position, price, final_signal, weighting, updated_at,
    'real_trade' AS pnl_type
FROM analytics.strategy_pnl_1min_real_trade_v2 FINAL;
