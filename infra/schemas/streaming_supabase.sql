-- Streaming pipeline state in Supabase Postgres.
-- Apply once manually: psql "<SUPABASE_CONN_STRING>" -f streaming_supabase.sql
-- Idempotent (all statements use IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS streaming;

-- ─────────────────────────────────────────────────────────────────────────────
-- streaming.pnl_checkpoint
-- One row per (mode, strategy_table_name). Source of truth for AnchorState.
-- bar_ts and revision_ts are NOT NULL — the "no anchor yet" sentinel is
-- datetime.min (0001-01-01 00:00:00+00), matching AnchorRecord defaults.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS streaming.pnl_checkpoint (
    mode                  TEXT             NOT NULL,
    strategy_table_name   TEXT             NOT NULL,
    pnl                   DOUBLE PRECISION NOT NULL,
    price                 DOUBLE PRECISION NOT NULL,
    position              DOUBLE PRECISION NOT NULL,
    bar_ts                TIMESTAMPTZ      NOT NULL,
    revision_ts           TIMESTAMPTZ      NOT NULL,
    strategy_id           INTEGER          NOT NULL DEFAULT 0,
    strategy_name         TEXT             NOT NULL DEFAULT '',
    underlying            TEXT             NOT NULL DEFAULT '',
    config_timeframe      TEXT             NOT NULL DEFAULT '',
    weighting             DOUBLE PRECISION NOT NULL DEFAULT 0,
    strategy_instance_id  TEXT             NOT NULL DEFAULT '',
    final_signal          DOUBLE PRECISION NOT NULL DEFAULT 0,
    benchmark             DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (mode, strategy_table_name)
);

CREATE INDEX IF NOT EXISTS idx_pnl_checkpoint_mode_updated
    ON streaming.pnl_checkpoint (mode, updated_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- streaming.pnl_commit_state
-- One row per mode. Atomic-commit anchor: Kafka offset + state_hash + version.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS streaming.pnl_commit_state (
    mode             TEXT        NOT NULL PRIMARY KEY,
    last_candle_ts   TIMESTAMPTZ NOT NULL,
    kafka_topic      TEXT        NOT NULL,
    kafka_partition  INTEGER     NOT NULL,
    kafka_offset     BIGINT      NOT NULL,
    state_hash       TEXT        NOT NULL,
    schema_version   INTEGER     NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
