-- ============================================================================
-- ClickHouse Cloud Shadow Tables — trading-analysis
-- ============================================================================
-- Shadow tables for validating Flink output before cutover.
-- Drop these after successful cutover validation.

-- Shadow table for market data
CREATE TABLE IF NOT EXISTS analytics.futures_price_1min_shadow
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

-- Shadow table for production PnL
CREATE TABLE IF NOT EXISTS analytics.strategy_pnl_1min_prod_v2_shadow
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
