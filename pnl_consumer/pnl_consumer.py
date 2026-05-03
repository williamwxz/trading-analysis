import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, KafkaError, KafkaException

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import (
    fetch_bt_strategies_for_candle,
    fetch_real_trade_revisions_for_candle,
    fetch_strategies_for_candle,
)
from streaming.models import CandleEvent
from trading_dagster.utils.clickhouse_client import insert_rows
from trading_dagster.utils.pnl_compute import PROD_INSERT_COLUMNS

logger = logging.getLogger(__name__)

FLUSH_EVERY = 10
TOPIC = "binance.price.ticks"
GROUP_ID = "flink-pnl-consumer"

PRICE_COLUMNS = [
    "exchange",
    "instrument",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
]
HOUR_INSERT_COLUMNS = PROD_INSERT_COLUMNS


def _aggregate_hourly(pnl_batch: list[list]) -> list[list]:
    """From a 1min PnL batch, produce one hourly snapshot row per strategy.

    Groups by (strategy_table_name, hour_bucket). For each group, picks the
    row with the latest ts as the representative snapshot for that hour.
    Returns rows in HOUR_INSERT_COLUMNS order with ts replaced by the hour bucket.
    """
    COL = {name: i for i, name in enumerate(PROD_INSERT_COLUMNS)}

    groups: dict[tuple, list] = {}
    for row in pnl_batch:
        ts: datetime = row[COL["ts"]]
        hour_bucket = ts.replace(minute=0, second=0, microsecond=0)
        key = (row[COL["strategy_table_name"]], hour_bucket)
        existing = groups.get(key)
        if existing is None or ts > existing[COL["ts"]]:
            groups[key] = row

    now = datetime.now(UTC).replace(tzinfo=None)
    result: list[list] = []
    for (_, hour_bucket), row in groups.items():
        hourly_row = list(row)
        hourly_row[COL["ts"]] = hour_bucket
        hourly_row[COL["updated_at"]] = now
        result.append(hourly_row)
    return result


def process_candle(
    candle: CandleEvent,
    state_prod: AnchorState,
    state_real_trade: AnchorState,
) -> list[dict]:
    """Pure business logic: lookup strategies, compute PnL, return rows.

    Returns a list of dicts. Each dict has a '_sink' key:
      '_sink' == 'price'          → write to futures_price_1min
      '_sink' == 'pnl_prod'       → write to strategy_pnl_1min_prod_v2
      '_sink' == 'pnl_real_trade' → write to strategy_pnl_1min_real_trade_v2
      '_sink' == 'pnl_bt'         → write to strategy_pnl_1min_bt_v2

    Always emits one price row. Emits one pnl row per active strategy per sink.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []

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

    # --- prod ---
    prod_strategies = fetch_strategies_for_candle(candle.instrument, candle.ts)
    for bar in prod_strategies:
        pnl = state_prod.compute_pnl(
            strategy_table_name=bar.strategy_table_name,
            close_price=candle.close,
            position=bar.position,
        )
        rows.append(
            {
                "_sink": "pnl_prod",
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

    # --- real_trade ---
    real_trade_revisions = fetch_real_trade_revisions_for_candle(
        candle.instrument, candle.ts
    )
    for rev in real_trade_revisions:
        pnl = state_real_trade.compute_pnl(
            strategy_table_name=rev.strategy_table_name,
            close_price=candle.close,
            position=rev.position,
        )
        execution_ts = (rev.revision_ts + timedelta(seconds=59)).replace(second=0)
        rows.append(
            {
                "_sink": "pnl_real_trade",
                "strategy_table_name": rev.strategy_table_name,
                "strategy_id": rev.strategy_id,
                "strategy_name": rev.strategy_name,
                "underlying": rev.underlying,
                "config_timeframe": rev.config_timeframe,
                "source": "real_trade",
                "version": "v2",
                "ts": candle.ts,
                "cumulative_pnl": pnl,
                "benchmark": rev.benchmark,
                "position": rev.position,
                "price": candle.close,
                "final_signal": rev.final_signal,
                "weighting": rev.weighting,
                "updated_at": now,
                "closing_ts": rev.closing_ts,
                "execution_ts": execution_ts,
                "traded": False,
            }
        )

    # --- bt ---
    bt_bars = fetch_bt_strategies_for_candle(candle.instrument, candle.ts)
    for bar in bt_bars:
        rows.append(
            {
                "_sink": "pnl_bt",
                "strategy_table_name": bar.strategy_table_name,
                "strategy_id": bar.strategy_id,
                "strategy_name": bar.strategy_name,
                "underlying": bar.underlying,
                "config_timeframe": bar.config_timeframe,
                "source": "backtest",
                "version": "v2",
                "ts": candle.ts,
                "cumulative_pnl": bar.cumulative_pnl,
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
    logger.info("Bootstrapped %d anchor(s) from ClickHouse", len(state))


def _flush(
    consumer: Consumer,
    price_batch: list[list],
    pnl_batch: list[list],
) -> None:
    """Insert price and PnL batches to ClickHouse, aggregate PnL to hourly snapshots.

    Raises on failure. Commits offsets after successful insert.
    """
    if price_batch:
        insert_rows("analytics.futures_price_1min", PRICE_COLUMNS, price_batch)
    if pnl_batch:
        insert_rows(
            "analytics.strategy_pnl_1min_prod_v2", PROD_INSERT_COLUMNS, pnl_batch
        )
        hourly_rows = _aggregate_hourly(pnl_batch)
        if hourly_rows:
            insert_rows(
                "analytics.strategy_pnl_1hour_prod_v2",
                HOUR_INSERT_COLUMNS,
                hourly_rows,
            )
            logger.info("Upserted %d hourly snapshot(s)", len(hourly_rows))
    price_batch.clear()
    pnl_batch.clear()
    consumer.commit(asynchronous=False)


def run() -> None:
    logging.basicConfig(level=logging.INFO)

    state_prod = AnchorState()
    state_real_trade = AnchorState()
    _bootstrap_anchors(state_prod)

    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    logger.info("Subscribed to %s as group %s", TOPIC, GROUP_ID)

    price_batch: list[list] = []
    pnl_batch: list[list] = []

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, flushing and closing")
        try:
            _flush(consumer, price_batch, pnl_batch)
        except Exception:
            logger.exception("Error during shutdown flush")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                logger.warning("Kafka error: %s", msg.error())
                continue

            data = json.loads(msg.value().decode())
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
            logger.info(
                "Received %s close=%.2f ts=%s batch=%d/%d",
                candle.instrument,
                candle.close,
                candle.ts,
                len(price_batch) + 1,
                FLUSH_EVERY,
            )

            for row in process_candle(candle, state_prod, state_real_trade):
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
                elif row["_sink"] == "pnl_prod":
                    pnl_batch.append([row[c] for c in PROD_INSERT_COLUMNS])

            if len(price_batch) >= FLUSH_EVERY or len(pnl_batch) >= FLUSH_EVERY:
                n_price, n_pnl = len(price_batch), len(pnl_batch)
                _flush(consumer, price_batch, pnl_batch)
                logger.info("Flushed %d price + %d pnl rows", n_price, n_pnl)

    except Exception:
        logger.exception("Fatal error in consumer loop")
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    run()
