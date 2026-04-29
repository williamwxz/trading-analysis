"""
PyFlink streaming job: Binance candles → PnL rows → ClickHouse.

Job graph:
  KafkaSource(binance.price.ticks)
  → process_candle() [lookup join + anchor-chain PnL]
  → SinkA: futures_price_1min
  → SinkB: strategy_pnl_1min_prod_v2

process_candle() is the pure business logic — kept separate so it can be
unit-tested without a Flink cluster.

In production this runs as a PyFlink job with:
- Flink keyed state for anchor (checkpointed to S3 every 30s)
- Kafka connector source (Redpanda, port 9092)
- ClickHouse JDBC sink (via flink-connector-jdbc + clickhouse-jdbc driver)
"""

import json
import logging
import os
from datetime import UTC, datetime

from flink_job.anchor_state import AnchorRecord, AnchorState
from flink_job.ch_lookup import fetch_strategies_for_candle
from streaming.models import CandleEvent

logger = logging.getLogger(__name__)

PROD_INSERT_COLUMNS = [
    "strategy_table_name",
    "strategy_id",
    "strategy_name",
    "underlying",
    "config_timeframe",
    "source",
    "version",
    "ts",
    "cumulative_pnl",
    "benchmark",
    "position",
    "price",
    "final_signal",
    "weighting",
    "updated_at",
]


def process_candle(
    candle: CandleEvent,
    state: AnchorState,
) -> list[dict]:
    """Pure business logic: lookup strategies, compute PnL, return rows.

    Returns a list of dicts. Each dict has a '_sink' key:
      '_sink' == 'price'  → write to futures_price_1min
      '_sink' == 'pnl'    → write to strategy_pnl_1min_prod_v2

    Always emits one price row. Emits one pnl row per active strategy.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []

    # Always write the candle to the price table
    rows.append(
        {
            "_sink": "price",
            "exchange": candle.exchange,
            "instrument": candle.instrument,
            "ts": candle.ts,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
        }
    )

    strategies = fetch_strategies_for_candle(candle.instrument, candle.ts)
    if not strategies:
        logger.debug(
            "No strategies for %s at %s — skipping PnL",
            candle.instrument,
            candle.ts,
        )
        return rows

    for bar in strategies:
        pnl = state.compute_pnl(
            strategy_table_name=bar.strategy_table_name,
            close_price=candle.close,
            position=bar.position,
        )
        rows.append(
            {
                "_sink": "pnl",
                "strategy_table_name": bar.strategy_table_name,
                "strategy_id": bar.strategy_id,
                "strategy_name": bar.strategy_name,
                "underlying": bar.underlying,
                "config_timeframe": bar.config_timeframe,
                "source": "production",
                "version": "v2",
                "ts": candle.ts,
                "cumulative_pnl": pnl,
                "benchmark": bar.benchmark,
                "position": bar.position,
                "price": candle.close,
                "final_signal": bar.final_signal,
                "weighting": bar.weighting,
                "updated_at": now,
            }
        )

    return rows


def _bootstrap_anchors(state: AnchorState) -> None:
    """On cold start, seed anchor state from last known PnL rows in ClickHouse."""
    from trading_dagster.utils.clickhouse_client import query_dicts

    sql = """\
SELECT
    strategy_table_name,
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position
FROM analytics.strategy_pnl_1min_prod_v2
WHERE ts >= now() - INTERVAL 2 HOUR
ORDER BY strategy_table_name, ts DESC, updated_at DESC
LIMIT 1 BY strategy_table_name
"""
    for row in query_dicts(sql):
        state.update(
            row["strategy_table_name"],
            AnchorRecord(
                anchor_pnl=row["anchor_pnl"],
                anchor_price=row["anchor_price"],
                anchor_position=row["anchor_position"],
            ),
        )
    logger.info("Bootstrapped %d anchor(s) from ClickHouse", len(state._store))


def run_flink_job() -> None:
    """Entry point for PyFlink job. Wires up Kafka source + CH sinks."""
    from pyflink.common import WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )

    from trading_dagster.utils.clickhouse_client import insert_rows

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000, CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage_uri(
        f"s3://{os.environ['S3_BUCKET']}/flink-checkpoints/"
    )

    brokers = os.environ["REDPANDA_BROKERS"]
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(brokers)
        .set_topics("binance.price.ticks")
        .set_group_id("flink-pnl-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    state = AnchorState()
    _bootstrap_anchors(state)

    price_batch: list[list] = []
    pnl_batch: list[list] = []
    FLUSH_EVERY = 50

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Redpanda")

    def handle(raw: str) -> None:
        nonlocal price_batch, pnl_batch
        data = json.loads(raw)
        candle = CandleEvent(
            exchange=data["exchange"],
            instrument=data["instrument"],
            ts=datetime.fromisoformat(data["ts"]),
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            volume=data["volume"],
        )
        for row in process_candle(candle, state):
            if row["_sink"] == "price":
                price_batch.append(
                    [
                        row["exchange"],
                        row["instrument"],
                        row["ts"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                    ]
                )
            elif row["_sink"] == "pnl":
                pnl_batch.append([row[c] for c in PROD_INSERT_COLUMNS])

        if len(price_batch) >= FLUSH_EVERY:
            insert_rows(
                "analytics.futures_price_1min",
                [
                    "exchange",
                    "instrument",
                    "ts",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ],
                price_batch,
            )
            price_batch = []

        if len(pnl_batch) >= FLUSH_EVERY:
            insert_rows(
                "analytics.strategy_pnl_1min_prod_v2",
                PROD_INSERT_COLUMNS,
                pnl_batch,
            )
            pnl_batch = []

    ds.map(handle)
    env.execute("binance-pnl-stream")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_flink_job()
