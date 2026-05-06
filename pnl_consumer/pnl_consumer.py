import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException

from pnl_consumer.anchor_state import AnchorRecord, AnchorState
from pnl_consumer.ch_lookup import (
    StrategyRevision,
    fetch_anchor_for_strategy,
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


@dataclass
class SinkConfig:
    price: bool
    prod: bool
    real_trade: bool
    bt: bool

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "SinkConfig":
        if env is None:
            env = os.environ
        def _flag(key: str, default: bool) -> bool:
            return env.get(key, "true" if default else "false").lower() == "true"
        return cls(
            price=_flag("ENABLE_PRICE_SINK", True),
            prod=_flag("ENABLE_PROD_SINK", False),
            real_trade=_flag("ENABLE_REAL_TRADE_SINK", False),
            bt=_flag("ENABLE_BT_SINK", False),
        )


FLUSH_EVERY = 10
TOPIC = "binance.price.ticks"
_DEFAULT_GROUP_ID = "flink-pnl-consumer"


def resolve_group_id(env: dict[str, str] | None = None) -> str:
    if env is None:
        env = os.environ
    return env.get("KAFKA_GROUP_ID", _DEFAULT_GROUP_ID)

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


_ALL_SINKS_ENABLED = SinkConfig(price=True, prod=True, real_trade=True, bt=True)


def process_candle(
    candle: CandleEvent,
    state_prod: AnchorState,
    state_real_trade: AnchorState,
    state_bt: AnchorState,
    cfg: SinkConfig | None = None,
) -> list[dict]:
    """Pure business logic: lookup strategies, compute PnL, return rows.

    Returns a list of dicts. Each dict has a '_sink' key:
      '_sink' == 'price'          → write to futures_price_1min
      '_sink' == 'pnl_prod'       → write to strategy_pnl_1min_prod_v2
      '_sink' == 'pnl_real_trade' → write to strategy_pnl_1min_real_trade_v2
      '_sink' == 'pnl_bt'         → write to strategy_pnl_1min_bt_v2

    cfg controls which sinks are active. Defaults to all enabled for backward
    compatibility. In production, pass SinkConfig.from_env() to honour env vars.
    """
    if cfg is None:
        cfg = _ALL_SINKS_ENABLED

    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []

    if cfg.price:
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

    def _compute_pnl_with_lazy_seed(
        state: AnchorState, strategy_table_name: str, close_price: float, position: float
    ) -> float | None:
        """Compute PnL; lazy-seed anchor from ClickHouse if missing. Returns None to skip."""
        try:
            return state.compute_pnl(strategy_table_name, close_price, position)
        except RuntimeError:
            anchor = fetch_anchor_for_strategy(strategy_table_name)
            if anchor is None:
                logger.warning(
                    "No anchor found for strategy '%s' — skipping PnL row for ts=%s",
                    strategy_table_name,
                    candle.ts,
                )
                return None
            logger.info(
                "Lazy-seeded anchor for strategy '%s' from ClickHouse (pnl=%.4f, price=%.2f)",
                strategy_table_name,
                anchor.anchor_pnl,
                anchor.anchor_price,
            )
            state.update(strategy_table_name, anchor)
            return state.compute_pnl(strategy_table_name, close_price, position)

    # --- prod ---
    if cfg.prod:
        prod_strategies = fetch_strategies_for_candle(candle.instrument, candle.ts)
        for bar in prod_strategies:
            pnl = _compute_pnl_with_lazy_seed(
                state_prod, bar.strategy_table_name, candle.open, bar.position
            )
            if pnl is None:
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
                    "price": candle.open,
                    "final_signal": bar.final_signal,
                    "weighting": bar.weighting,
                    "updated_at": now,
                }
            )

    # --- real_trade ---
    if cfg.real_trade:
        # Revisions are ordered by revision_ts ASC. We find the latest revision whose
        # execution_ts <= candle.ts — that is the currently-active position for this minute.
        # Revisions arriving in the future (execution_ts > candle.ts) are not yet active.
        real_trade_revisions = fetch_real_trade_revisions_for_candle(
            candle.instrument, candle.ts
        )
        # Group by strategy, pick the latest revision that is already active.
        active_revisions: dict[str, StrategyRevision] = {}
        for rev in real_trade_revisions:
            execution_ts = (rev.revision_ts + timedelta(seconds=59)).replace(
                second=0, microsecond=0
            )
            if execution_ts <= candle.ts:
                active_revisions[rev.strategy_table_name] = rev  # last one wins (ASC order)

        for stn, rev in active_revisions.items():
            pnl = _compute_pnl_with_lazy_seed(
                state_real_trade, stn, candle.open, rev.position
            )
            if pnl is None:
                continue
            execution_ts = (rev.revision_ts + timedelta(seconds=59)).replace(
                second=0, microsecond=0
            )
            rows.append(
                {
                    "_sink": "pnl_real_trade",
                    "strategy_table_name": stn,
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
                    "price": candle.open,
                    "final_signal": rev.final_signal,
                    "weighting": rev.weighting,
                    "updated_at": now,
                    "closing_ts": rev.closing_ts,
                    "execution_ts": execution_ts,
                    "traded": False,
                }
            )

    # --- bt ---
    if cfg.bt:
        bt_bars = fetch_bt_strategies_for_candle(candle.instrument, candle.ts)
        for bt_bar in bt_bars:
            pnl = _compute_pnl_with_lazy_seed(
                state_bt, bt_bar.strategy_table_name, candle.open, bt_bar.position
            )
            if pnl is None:
                continue
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
                    "cumulative_pnl": pnl,
                    "benchmark": bt_bar.benchmark,
                    "position": bt_bar.position,
                    "price": candle.open,
                    "final_signal": bt_bar.final_signal,
                    "weighting": bt_bar.weighting,
                    "updated_at": now,
                }
            )

    return rows


def _bootstrap_anchors(
    state_prod: AnchorState,
    state_real_trade: AnchorState,
    state_bt: AnchorState,
    cfg: SinkConfig | None = None,
) -> None:
    """On cold start, seed anchor states from ClickHouse for enabled PnL sinks."""
    if cfg is not None and not any([cfg.prod, cfg.real_trade, cfg.bt]):
        logger.info("No PnL sinks enabled — skipping anchor bootstrap")
        return

    candidates = [
        ("analytics.strategy_pnl_1min_prod_v2", state_prod, cfg.prod if cfg else True),
        ("analytics.strategy_pnl_1min_real_trade_v2", state_real_trade, cfg.real_trade if cfg else True),
        ("analytics.strategy_pnl_1min_bt_v2", state_bt, cfg.bt if cfg else True),
    ]
    for table, state, enabled in candidates:
        if not enabled:
            continue
        sql = f"""\
SELECT
    strategy_table_name,
    cumulative_pnl  AS anchor_pnl,
    price           AS anchor_price,
    position        AS anchor_position
FROM {table}
WHERE ts >= now() - INTERVAL 48 HOUR
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
    enabled_states = [
        (state, enabled) for _, state, enabled in candidates if enabled
    ]
    if all(len(state) == 0 for state, _ in enabled_states):
        raise RuntimeError(
            "Bootstrap failed: no anchor rows found in ClickHouse. "
            "Cannot start consumer without a valid PnL baseline."
        )
    logger.info(
        "Bootstrapped %d prod, %d real_trade, %d bt anchor(s) from ClickHouse",
        len(state_prod),
        len(state_real_trade),
        len(state_bt),
    )


def emit_candle_lag(candle_ts: datetime, cw_client: Any, sink: str) -> None:
    """Emit CandleLagSeconds and CandleProcessingTs to CloudWatch. Swallows errors."""
    try:
        lag = (datetime.now(UTC).replace(tzinfo=None) - candle_ts).total_seconds()
        dimensions = [{"Name": "Sink", "Value": sink}]
        cw_client.put_metric_data(
            Namespace="trading-analysis",
            MetricData=[
                {
                    "MetricName": "CandleLagSeconds",
                    "Value": lag,
                    "Unit": "Seconds",
                    "Dimensions": dimensions,
                },
                {
                    "MetricName": "CandleProcessingTs",
                    "Value": candle_ts.timestamp(),
                    "Unit": "None",
                    "Dimensions": dimensions,
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
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        if e.args[0].code() == KafkaError._NO_OFFSET:
            pass  # no messages consumed yet — nothing to commit
        else:
            raise


def _flush_and_reseed(
    consumer: Consumer,
    price_batch: list[list],
    pnl_prod_batch: list[list],
    pnl_real_trade_batch: list[list],
    pnl_bt_batch: list[list],
    state_prod: AnchorState,
    state_real_trade: AnchorState,
    state_bt: AnchorState,
    cfg: SinkConfig | None = None,
) -> None:
    """Flush batches then re-seed anchor state from ClickHouse.

    Re-seeding after every flush means a manual backfill that overwrites
    ClickHouse rows is picked up within one flush cycle (~10 candles) rather
    than requiring a consumer restart.
    """
    _flush(consumer, price_batch, pnl_prod_batch, pnl_real_trade_batch, pnl_bt_batch)
    _bootstrap_anchors(state_prod, state_real_trade, state_bt, cfg)


def run() -> None:
    logging.basicConfig(level=logging.INFO)

    sink_cfg = SinkConfig.from_env()
    logger.info(
        "Sink config: price=%s prod=%s real_trade=%s bt=%s",
        sink_cfg.price, sink_cfg.prod, sink_cfg.real_trade, sink_cfg.bt,
    )

    state_prod = AnchorState()
    state_real_trade = AnchorState()
    state_bt = AnchorState()
    _bootstrap_anchors(state_prod, state_real_trade, state_bt, sink_cfg)

    cw_client = boto3.client("cloudwatch", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))

    group_id = resolve_group_id()
    sink_label = group_id.removeprefix("pnl-consumer-") or group_id
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    logger.info("Subscribed to %s as group %s", TOPIC, group_id)

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

            for row in process_candle(candle, state_prod, state_real_trade, state_bt, sink_cfg):
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
                    state_bt,
                    sink_cfg,
                )
                emit_candle_lag(candle.ts, cw_client, sink_label)
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
