import json
import logging
import os
import signal
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import (
    fetch_bt_strategies_for_candle,
    fetch_real_trade_revisions_for_candle,
    fetch_strategies_for_candle,
)
from streaming.models import CandleEvent
from trading_dagster.utils.clickhouse_client import insert_rows, query_dicts
from trading_dagster.utils.pnl_compute import (
    PROD_INSERT_COLUMNS,
    REAL_TRADE_INSERT_COLUMNS,
)

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
        try:
            pnl = state_prod.compute_pnl(
                strategy_table_name=bar.strategy_table_name,
                close_price=candle.close,
                position=bar.position,
            )
        except RuntimeError:
            logger.warning(
                "Skipping prod PnL for %s — no anchor (new strategy?); will pick up after next reseed",
                bar.strategy_table_name,
            )
            continue
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
        try:
            pnl = state_real_trade.compute_pnl(
                strategy_table_name=rev.strategy_table_name,
                close_price=candle.close,
                position=rev.position,
            )
        except RuntimeError:
            logger.warning(
                "Skipping real_trade PnL for %s — no anchor (new strategy?); will pick up after next reseed",
                rev.strategy_table_name,
            )
            continue
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
    for bt_bar in bt_bars:
        rows.append(
            {
                "_sink": "pnl_bt",
                "strategy_table_name": bt_bar.strategy_table_name,
                "strategy_id": bt_bar.strategy_id,
                "strategy_name": bt_bar.strategy_name,
                "underlying": bt_bar.underlying,
                "config_timeframe": bt_bar.config_timeframe,
                "source": "backtest",
                "version": "v2",
                "ts": candle.ts,
                "cumulative_pnl": bt_bar.cumulative_pnl,
                "benchmark": bt_bar.benchmark,
                "position": bt_bar.position,
                "price": candle.close,
                "final_signal": bt_bar.final_signal,
                "weighting": bt_bar.weighting,
                "updated_at": now,
            }
        )

    return rows


def _bootstrap_anchors(state_prod: AnchorState, state_real_trade: AnchorState) -> None:
    """On cold start, seed prod and real_trade anchor states from ClickHouse."""
    for table, state in [
        ("analytics.strategy_pnl_1min_prod_v2", state_prod),
        ("analytics.strategy_pnl_1min_real_trade_v2", state_real_trade),
    ]:
        sql = f"""\
SELECT
    strategy_table_name,
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position
FROM {table}
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
    if len(state_prod) == 0 and len(state_real_trade) == 0:
        raise RuntimeError(
            "Bootstrap failed: no anchor rows found in ClickHouse within the last 2 hours. "
            "Cannot start consumer without a valid PnL baseline."
        )
    logger.info(
        "Bootstrapped %d prod anchor(s) and %d real_trade anchor(s) from ClickHouse",
        len(state_prod),
        len(state_real_trade),
    )


def emit_candle_lag(candle_ts: datetime, cw_client: Any) -> None:
    """Emit CandleLagSeconds and CandleProcessingTs to CloudWatch. Swallows errors."""
    try:
        lag = (datetime.now(UTC).replace(tzinfo=None) - candle_ts).total_seconds()
        cw_client.put_metric_data(
            Namespace="trading-analysis",
            MetricData=[
                {
                    "MetricName": "CandleLagSeconds",
                    "Value": lag,
                    "Unit": "Seconds",
                },
                {
                    "MetricName": "CandleProcessingTs",
                    "Value": candle_ts.timestamp(),
                    "Unit": "None",
                },
            ],
        )
    except Exception:
        logger.warning("Failed to emit candle metrics", exc_info=True)


def _flush(
    consumer: Consumer,
    price_batch: list[list],
    pnl_prod_batch: list[list],
    pnl_real_trade_batch: list[list],
    pnl_bt_batch: list[list],
) -> None:
    """Insert batches to ClickHouse, then commit offsets. Raises on failure."""
    if price_batch:
        insert_rows("analytics.futures_price_1min", PRICE_COLUMNS, price_batch)
    if pnl_prod_batch:
        insert_rows(
            "analytics.strategy_pnl_1min_prod_v2", PROD_INSERT_COLUMNS, pnl_prod_batch
        )
    if pnl_real_trade_batch:
        insert_rows(
            "analytics.strategy_pnl_1min_real_trade_v2",
            REAL_TRADE_INSERT_COLUMNS,
            pnl_real_trade_batch,
        )
    if pnl_bt_batch:
        insert_rows(
            "analytics.strategy_pnl_1min_bt_v2", PROD_INSERT_COLUMNS, pnl_bt_batch
        )
    price_batch.clear()
    pnl_prod_batch.clear()
    pnl_real_trade_batch.clear()
    pnl_bt_batch.clear()
    consumer.commit(asynchronous=False)


def _flush_and_reseed(
    consumer: Consumer,
    price_batch: list[list],
    pnl_prod_batch: list[list],
    pnl_real_trade_batch: list[list],
    pnl_bt_batch: list[list],
    state_prod: AnchorState,
    state_real_trade: AnchorState,
) -> None:
    """Flush batches then re-seed anchor state from ClickHouse.

    Re-seeding after every flush means a manual backfill that overwrites
    ClickHouse rows is picked up within one flush cycle (~10 candles) rather
    than requiring a consumer restart.
    """
    _flush(consumer, price_batch, pnl_prod_batch, pnl_real_trade_batch, pnl_bt_batch)
    _bootstrap_anchors(state_prod, state_real_trade)


def run() -> None:
    logging.basicConfig(level=logging.INFO)

    state_prod = AnchorState()
    state_real_trade = AnchorState()
    _bootstrap_anchors(state_prod, state_real_trade)

    cw_client = boto3.client("cloudwatch", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))

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
    pnl_prod_batch: list[list] = []
    pnl_real_trade_batch: list[list] = []
    pnl_bt_batch: list[list] = []

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, flushing and closing")
        try:
            _flush(
                consumer,
                price_batch,
                pnl_prod_batch,
                pnl_real_trade_batch,
                pnl_bt_batch,
            )
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
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                if err.fatal():
                    raise KafkaException(err)
                logger.warning("Kafka error: %s", err)
                continue

            raw = msg.value()
            data = json.loads(raw.decode() if raw is not None else "{}")
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
                    pnl_prod_batch.append([row[c] for c in PROD_INSERT_COLUMNS])
                elif row["_sink"] == "pnl_real_trade":
                    pnl_real_trade_batch.append(
                        [row[c] for c in REAL_TRADE_INSERT_COLUMNS]
                    )
                elif row["_sink"] == "pnl_bt":
                    pnl_bt_batch.append([row[c] for c in PROD_INSERT_COLUMNS])

            total_batch = (
                len(price_batch)
                + len(pnl_prod_batch)
                + len(pnl_real_trade_batch)
                + len(pnl_bt_batch)
            )
            if total_batch >= FLUSH_EVERY:
                n_price = len(price_batch)
                n_prod = len(pnl_prod_batch)
                n_rt = len(pnl_real_trade_batch)
                n_bt = len(pnl_bt_batch)
                _flush_and_reseed(
                    consumer,
                    price_batch,
                    pnl_prod_batch,
                    pnl_real_trade_batch,
                    pnl_bt_batch,
                    state_prod,
                    state_real_trade,
                )
                emit_candle_lag(candle.ts, cw_client)
                logger.info(
                    "Flushed %d price + %d prod + %d real_trade + %d bt rows",
                    n_price,
                    n_prod,
                    n_rt,
                    n_bt,
                )
                if n_prod:
                    logger.info("ClickHouseSink prod rows=%d", n_prod)
                if n_rt:
                    logger.info("ClickHouseSink real_trade rows=%d", n_rt)
                if n_bt:
                    logger.info("ClickHouseSink bt rows=%d", n_bt)

    except Exception:
        logger.exception("Fatal error in consumer loop")
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    run()
