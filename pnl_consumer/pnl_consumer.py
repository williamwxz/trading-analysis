"""PnL streaming consumer: Kafka → ClickHouse.

Reads 1-minute closed candles from binance.price.ticks (Redpanda), computes
cumulative PnL per strategy, and writes results to ClickHouse in real-time.

Three independent sink modes: prod, bt, real_trade.
All computation logic lives in libs.computation — this file is pure orchestration.

Price source: candle.open from Redpanda (not ClickHouse futures_price_1min).
Position source: strategy_output_history_* via libs.computation candle lookups.
"""

import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from confluent_kafka import (
    OFFSET_INVALID,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from libs.clickhouse_client import insert_rows
from libs.computation import (
    AnchorRecord,
    AnchorState,
    BootstrapSeed,
    INSERT_COLUMNS,
    WalkRow,
    fetch_bootstrap_seeds,
    fetch_bt_strategies_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
    fetch_walk_rows,
)
from streaming.models import CandleEvent

logger = logging.getLogger(__name__)

TOPIC = "binance.price.ticks"
_DEFAULT_GROUP_ID = "flink-pnl-consumer"
FLUSH_EVERY = 1000

_COLD_START_DAYS = 3
_PNL_WARN_TOLERANCE = 1e-6
_PNL_CRASH_TOLERANCE = 2e-3  # 0.2%

PRICE_COLUMNS = ["exchange", "instrument", "ts", "open", "high", "low", "close", "volume"]

# Table config per mode.
_MODE_CONFIG = {
    "prod": {
        "pnl_table": "analytics.strategy_pnl_1min_prod_v2",
        "history_table": "analytics.strategy_output_history_v2",
        "source_label": "production",
        "real_trade": False,
    },
    "bt": {
        "pnl_table": "analytics.strategy_pnl_1min_bt_v2",
        "history_table": "analytics.strategy_output_history_bt_v2",
        "source_label": "backtest",
        "real_trade": False,
    },
    "real_trade": {
        "pnl_table": "analytics.strategy_pnl_1min_real_trade_v2",
        "history_table": "analytics.strategy_output_history_v2",
        "source_label": "real_trade",
        "real_trade": True,
    },
}


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


def resolve_group_id(env: dict[str, str] | None = None) -> str:
    if env is None:
        env = os.environ
    return env.get("KAFKA_GROUP_ID", _DEFAULT_GROUP_ID)


def peek_reference_ts(
    brokers: str,
    group_id: str,
    topic: str = TOPIC,
    timeout: float = 5.0,
) -> "datetime | None":
    """Return min candle ts at this group's committed offsets. Falls back to latest offset."""
    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    try:
        try:
            meta = consumer.list_topics(topic, timeout=timeout)
            partitions = [TopicPartition(topic, p) for p in meta.topics[topic].partitions] if topic in meta.topics else []
        except Exception:
            partitions = []

        committed = consumer.committed(partitions, timeout=timeout)
        timestamps: list[datetime] = []
        for tp in committed:
            offset = tp.offset
            use_watermark = False
            if offset == OFFSET_INVALID:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=timeout)
                offset = max(high - 1, low)
                if offset < 0:
                    continue
                use_watermark = True
            seek_tp = TopicPartition(topic, tp.partition, offset)
            consumer.assign([seek_tp])
            if use_watermark:
                consumer.seek(seek_tp)
            msg = consumer.poll(timeout=timeout)
            if msg is None or msg.error():
                continue
            raw = msg.value()
            data = json.loads(raw.decode() if raw is not None else "{}")
            timestamps.append(datetime.fromisoformat(data["ts"]))

        return min(timestamps) if timestamps else None
    except Exception:
        logger.warning("peek_reference_ts failed — falling back to now()", exc_info=True)
        return None
    finally:
        consumer.close()


def _bootstrap_state(
    mode: str,
    reference_ts: "datetime | None",
) -> tuple[AnchorState, set[tuple[str, datetime]]]:
    """Seed AnchorState for one mode. Returns (state, seen_set).

    seen_set is populated for prod/bt (dedup guard). Empty for real_trade (uses
    AnchorState revision guard instead).
    """
    cfg = _MODE_CONFIG[mode]
    now = datetime.now(UTC).replace(tzinfo=None)
    ref_ts = reference_ts if reference_ts is not None else now
    start_ts = ref_ts - timedelta(days=_COLD_START_DAYS)

    seeds: list[BootstrapSeed] = fetch_bootstrap_seeds(
        pnl_table=cfg["pnl_table"],
        history_table=cfg["history_table"],
        start_ts=start_ts,
        real_trade=cfg["real_trade"],
    )

    state = AnchorState()
    for seed in seeds:
        state.set(
            seed.strategy_table_name,
            AnchorRecord(
                pnl=seed.pnl,
                price=seed.price,
                position=seed.position,
                bar_ts=seed.bar_ts,
                revision_ts=seed.revision_ts,
            ),
        )

    walk_rows: list[WalkRow] = fetch_walk_rows(
        pnl_table=cfg["pnl_table"],
        history_table=cfg["history_table"],
        start_ts=start_ts,
        reference_ts=ref_ts,
        real_trade=cfg["real_trade"],
    )

    seen: set[tuple[str, datetime]] = set()
    prev_pnl: dict[str, float] = {s.strategy_table_name: s.pnl for s in seeds}
    prev_price: dict[str, float] = {s.strategy_table_name: s.price for s in seeds}

    for wr in walk_rows:
        stn = wr.strategy_table_name
        pp = prev_price.get(stn, 0.0)
        pp_pnl = prev_pnl.get(stn, 0.0)

        if pp != 0.0:
            recomputed = pp_pnl + wr.position * (wr.price - pp) / pp
            deviation = abs(recomputed - wr.cumulative_pnl)
            if deviation > _PNL_CRASH_TOLERANCE:
                raise RuntimeError(
                    f"Cold-start PnL mismatch for {stn} at {wr.ts}: "
                    f"stored={wr.cumulative_pnl:.8f} recomputed={recomputed:.8f} "
                    f"delta={deviation:.2e} > {_PNL_CRASH_TOLERANCE:.1e}"
                )
            if deviation > _PNL_WARN_TOLERANCE:
                logger.warning(
                    "Cold-start PnL drift %s ts=%s stored=%.8f recomputed=%.8f delta=%.2e",
                    stn, wr.ts, wr.cumulative_pnl, recomputed, deviation,
                )

        prev_pnl[stn] = wr.cumulative_pnl
        prev_price[stn] = wr.price
        state.set(stn, AnchorRecord(
            pnl=wr.cumulative_pnl,
            price=wr.price,
            position=wr.position,
            bar_ts=wr.bar_ts,
            revision_ts=wr.revision_ts,
        ))
        if not cfg["real_trade"]:
            seen.add((wr.strategy_instance_id, wr.bar_ts))

    logger.info(
        "Bootstrap [%s]: seeded %d strategies, walked %d rows",
        mode, len(state), len(walk_rows),
    )
    return state, seen


def _compute_pnl_row(
    state: AnchorState,
    strategy_table_name: str,
    candle: CandleEvent,
    bar,
    source_label: str,
    now: datetime,
    bar_ts: "datetime | None" = None,
    revision_ts: "datetime | None" = None,
) -> list:
    """Compute one PnL row. Lazy-seeds from zero if strategy not yet in state."""
    if not state.has(strategy_table_name):
        logger.info("New strategy '%s' — seeding from zero at price=%.4f ts=%s",
                    strategy_table_name, candle.open, candle.ts)
        state.set(strategy_table_name, AnchorRecord(pnl=0.0, price=candle.open, position=0.0))
    pnl = state.compute_pnl(
        strategy_table_name,
        candle.open,
        bar.position,
        bar_ts=bar_ts or datetime.min,
        revision_ts=revision_ts or datetime.min,
    )
    return [
        strategy_table_name,
        bar.strategy_id,
        bar.strategy_name,
        bar.underlying,
        bar.config_timeframe,
        source_label,
        "v2",
        candle.ts,
        pnl,
        bar.benchmark,
        bar.position,
        candle.open,
        bar.final_signal,
        bar.weighting,
        now,
        bar.strategy_instance_id,
    ]


def process_candle(
    candle: CandleEvent,
    state_prod: AnchorState,
    state_real_trade: AnchorState,
    state_bt: AnchorState,
    seen_prod: set[tuple[str, datetime]],
    seen_bt: set[tuple[str, datetime]],
    cfg: SinkConfig,
) -> tuple[list[dict], int, int, int]:
    """Compute all output rows for one candle.

    Returns (rows, prod_fetched, bt_fetched, real_trade_fetched) where the
    fetched counts are the number of strategy instances returned by each
    candle lookup query — used by the caller to validate flush completeness.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []
    prod_fetched = bt_fetched = real_trade_fetched = 0

    if cfg.price:
        rows.append({
            "_sink": "price",
            "exchange": candle.exchange,
            "instrument": candle.instrument,
            "ts": candle.ts,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
        })

    if cfg.prod:
        prod_bars = fetch_strategies_for_candle(candle.instrument, candle.ts)
        prod_fetched = len(prod_bars)
        for bar in prod_bars:
            key = (bar.strategy_instance_id, bar.bar_ts)
            if key in seen_prod:
                continue
            seen_prod.add(key)
            rows.append({"_sink": "pnl_prod", "_row": _compute_pnl_row(
                state_prod, bar.strategy_table_name, candle, bar, "production", now,
            )})

    if cfg.bt:
        bt_bars = fetch_bt_strategies_for_candle(candle.instrument, candle.ts)
        bt_fetched = len(bt_bars)
        for bar in bt_bars:
            key = (bar.strategy_instance_id, bar.bar_ts)
            if key in seen_bt:
                continue
            seen_bt.add(key)
            rows.append({"_sink": "pnl_bt", "_row": _compute_pnl_row(
                state_bt, bar.strategy_table_name, candle, bar, "backtest", now,
            )})

    if cfg.real_trade:
        rt_revs = fetch_real_trade_for_candle(candle.instrument, candle.ts)
        real_trade_fetched = len(rt_revs)
        for rev in rt_revs:
            if not state_real_trade.should_apply_revision(
                rev.strategy_table_name, rev.bar_ts, rev.revision_ts
            ):
                continue
            rows.append({"_sink": "pnl_real_trade", "_row": _compute_pnl_row(
                state_real_trade, rev.strategy_table_name, candle, rev,
                "real_trade", now, rev.bar_ts, rev.revision_ts,
            )})

    return rows, prod_fetched, bt_fetched, real_trade_fetched


_SIID_COL = INSERT_COLUMNS.index("strategy_instance_id")  # 15


def _flush(
    consumer: Consumer,
    price_batch: list[list],
    pnl_prod_batch: list[list],
    pnl_real_trade_batch: list[list],
    pnl_bt_batch: list[list],
    expected_prod: int = 0,
    expected_bt: int = 0,
    expected_real_trade: int = 0,
) -> None:
    # Validate completeness before writing anything.
    # The expected counts come from the last fetch_*_for_candle result — the
    # ground truth for how many strategy instances should be in the output.
    # If the batch has fewer distinct instances, crash without sinking a single row.
    for batch, expected, label in (
        (pnl_prod_batch, expected_prod, "prod"),
        (pnl_bt_batch, expected_bt, "bt"),
        (pnl_real_trade_batch, expected_real_trade, "real_trade"),
    ):
        if not batch or not expected:
            continue
        actual = len({row[_SIID_COL] for row in batch})
        if actual < expected:
            raise RuntimeError(
                f"Flush completeness check failed [{label}]: "
                f"{actual} distinct strategy_instance_ids in batch < "
                f"{expected} fetched from history table. "
                "Refusing to sink partial data."
            )

    if price_batch:
        insert_rows("analytics.futures_price_1min", PRICE_COLUMNS, price_batch)
    if pnl_prod_batch:
        insert_rows("analytics.strategy_pnl_1min_prod_v2", INSERT_COLUMNS, pnl_prod_batch)
    if pnl_real_trade_batch:
        insert_rows("analytics.strategy_pnl_1min_real_trade_v2", INSERT_COLUMNS, pnl_real_trade_batch)
    if pnl_bt_batch:
        insert_rows("analytics.strategy_pnl_1min_bt_v2", INSERT_COLUMNS, pnl_bt_batch)
    price_batch.clear()
    pnl_prod_batch.clear()
    pnl_real_trade_batch.clear()
    pnl_bt_batch.clear()
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise


def emit_candle_lag(candle_ts: datetime, cw_client: Any, sink: str) -> None:
    try:
        lag = (datetime.now(UTC).replace(tzinfo=None) - candle_ts).total_seconds()
        dims = [{"Name": "Sink", "Value": sink}]
        cw_client.put_metric_data(
            Namespace="trading-analysis",
            MetricData=[
                {"MetricName": "CandleLagSeconds", "Value": lag, "Unit": "Seconds", "Dimensions": dims},
                {"MetricName": "CandleProcessingTs", "Value": candle_ts.timestamp(), "Unit": "None", "Dimensions": dims},
            ],
        )
    except Exception:
        logger.warning("Failed to emit candle metrics", exc_info=True)


def run() -> None:
    logging.basicConfig(level=logging.INFO)
    sink_cfg = SinkConfig.from_env()
    logger.info("Sink config: price=%s prod=%s real_trade=%s bt=%s",
                sink_cfg.price, sink_cfg.prod, sink_cfg.real_trade, sink_cfg.bt)

    reference_ts = peek_reference_ts(os.environ["REDPANDA_BROKERS"], resolve_group_id())
    if reference_ts is not None:
        logger.info("Cold-start reference_ts from committed offset: %s", reference_ts)
    else:
        logger.info("No committed offset — using now() as reference_ts")

    state_prod = AnchorState()
    state_bt = AnchorState()
    state_real_trade = AnchorState()
    seen_prod: set[tuple[str, datetime]] = set()
    seen_bt: set[tuple[str, datetime]] = set()

    try:
        if sink_cfg.prod:
            state_prod, seen_prod = _bootstrap_state("prod", reference_ts)
        if sink_cfg.bt:
            state_bt, seen_bt = _bootstrap_state("bt", reference_ts)
        if sink_cfg.real_trade:
            state_real_trade, _ = _bootstrap_state("real_trade", reference_ts)
    except Exception:
        logger.exception("Fatal error during bootstrap — exiting")
        sys.exit(1)

    cw_client = boto3.client("cloudwatch", region_name=os.environ.get("AWS_REGION", "ap-northeast-1"))
    group_id = resolve_group_id()
    sink_label = group_id.removeprefix("pnl-consumer-") or group_id

    consumer = Consumer({
        "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    logger.info("Subscribed to %s as group %s", TOPIC, group_id)

    price_batch: list[list] = []
    pnl_prod_batch: list[list] = []
    pnl_real_trade_batch: list[list] = []
    pnl_bt_batch: list[list] = []
    last_prod_fetched = last_bt_fetched = last_rt_fetched = 0

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received — flushing and closing")
        try:
            _flush(consumer, price_batch, pnl_prod_batch, pnl_real_trade_batch, pnl_bt_batch)
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
            logger.info("Candle %s open=%.2f ts=%s", candle.instrument, candle.open, candle.ts)

            rows, prod_fetched, bt_fetched, rt_fetched = process_candle(
                candle, state_prod, state_real_trade, state_bt,
                seen_prod, seen_bt, sink_cfg,
            )
            if prod_fetched:
                last_prod_fetched = prod_fetched
            if bt_fetched:
                last_bt_fetched = bt_fetched
            if rt_fetched:
                last_rt_fetched = rt_fetched
            logger.info(
                "Candle processed %s ts=%s prod=%d bt=%d rt=%d batch=%d",
                candle.instrument, candle.ts,
                prod_fetched, bt_fetched, rt_fetched,
                len(pnl_prod_batch) + len(pnl_bt_batch) + len(pnl_real_trade_batch),
            )

            for row in rows:
                sink = row["_sink"]
                if sink == "price":
                    price_batch.append([row["exchange"], row["instrument"], row["ts"],
                                        row["open"], row["high"], row["low"], row["close"], row["volume"]])
                elif sink == "pnl_prod":
                    pnl_prod_batch.append(row["_row"])
                elif sink == "pnl_real_trade":
                    pnl_real_trade_batch.append(row["_row"])
                elif sink == "pnl_bt":
                    pnl_bt_batch.append(row["_row"])

            total = len(price_batch) + len(pnl_prod_batch) + len(pnl_real_trade_batch) + len(pnl_bt_batch)
            if total >= FLUSH_EVERY:
                n = (len(price_batch), len(pnl_prod_batch), len(pnl_real_trade_batch), len(pnl_bt_batch))
                _flush(
                    consumer, price_batch, pnl_prod_batch, pnl_real_trade_batch, pnl_bt_batch,
                    expected_prod=last_prod_fetched,
                    expected_bt=last_bt_fetched,
                    expected_real_trade=last_rt_fetched,
                )
                emit_candle_lag(candle.ts, cw_client, sink_label)
                logger.info("Flushed %d price + %d prod + %d real_trade + %d bt rows", *n)

    except Exception:
        logger.exception("Fatal error in consumer loop")
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    run()
