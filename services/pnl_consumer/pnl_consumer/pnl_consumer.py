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

from libs.clickhouse_client import insert_rows, query_dicts
from libs.computation import (
    INSERT_COLUMNS,
    AnchorRecord,
    AnchorState,
    BootstrapSeed,
    WalkRow,
    build_carry_forward_row,
    build_pnl_row,
    fetch_bootstrap_seeds,
    fetch_bt_strategies_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
    fetch_walk_rows,
)
from streaming.binance_ws_consumer import INSTRUMENTS
from streaming.models import CandleEvent

logger = logging.getLogger(__name__)

TOPIC = "binance.price.ticks"
_UNDERLYINGS = [inst.removesuffix("USDT") for inst in INSTRUMENTS]
_DEFAULT_GROUP_ID = "flink-pnl-consumer"

_SEED_HOURS = 48  # seed anchor from last row before ref_ts - 48h (covers all timeframes incl 1d bars)
_WALK_HOURS = (
    2  # walk [ref_ts - 2h, ref_ts) to advance anchor price/position to present
)
_PNL_WARN_TOLERANCE = 1e-6
_PNL_CRASH_TOLERANCE = 2e-3  # 0.2%

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
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    try:
        try:
            meta = consumer.list_topics(topic, timeout=timeout)
            partitions = (
                [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
                if topic in meta.topics
                else []
            )
        except Exception:
            partitions = []

        committed = consumer.committed(partitions, timeout=timeout)
        timestamps: list[datetime] = []
        for tp in committed:
            offset = tp.offset
            use_watermark = False
            if offset == OFFSET_INVALID:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, tp.partition), timeout=timeout
                )
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
        logger.warning(
            "peek_reference_ts failed — falling back to now()", exc_info=True
        )
        return None
    finally:
        consumer.close()


def _fetch_walk_anchors(
    pnl_table: str,
    walk_ts: datetime,
    underlyings: "list[str]",
) -> "tuple[dict[str, float], dict[str, float]]":
    """Return (pnl, price) dicts keyed by strategy_table_name for the last row before walk_ts.

    Queries per underlying so ClickHouse prunes to one instrument's rows at a time
    (each scan ~30-50K rows, well under 50MB). The 2-day window covers 1d-bar strategies
    whose last bar may be up to 24h+ ago. underlyings comes from the seeds already
    discovered by fetch_bootstrap_seeds so no extra discovery query is needed.
    """
    from libs.clickhouse_client import query_dicts

    ts_str = walk_ts.strftime("%Y-%m-%d %H:%M:%S")
    window_start_str = (walk_ts - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    pnl_map: dict[str, float] = {}
    price_map: dict[str, float] = {}
    for underlying in underlyings:
        sql = f"""\
SELECT
    strategy_table_name,
    argMax(cumulative_pnl, (ts, updated_at)) AS cumulative_pnl,
    argMax(price,          (ts, updated_at)) AS price
FROM {pnl_table}
WHERE underlying = '{underlying}'
  AND ts >= '{window_start_str}' AND ts < '{ts_str}'
GROUP BY strategy_table_name
"""
        for row in query_dicts(sql):
            pnl_map[row["strategy_table_name"]] = float(row["cumulative_pnl"] or 0.0)
            price_map[row["strategy_table_name"]] = float(row["price"])
    return pnl_map, price_map


def _bootstrap_bt_state(reference_ts: "datetime | None") -> AnchorState:
    """Seed AnchorState for bt directly from row_json.cumulative_pnl.

    BT bars carry an authoritative cumulative_pnl in row_json (set by the backtest
    engine), so there is no need to walk the pnl table or fetch price history.
    We seed each strategy from its latest bar before ref_ts: pnl from row_json,
    price from futures_price_1min at that bar's closing_ts, position from row_json.

    A single scoped query per underlying keeps memory usage small.
    """
    cfg = _MODE_CONFIG["bt"]
    now = datetime.now(UTC).replace(tzinfo=None)
    ref_ts = (reference_ts if reference_ts is not None else now).replace(tzinfo=None)
    lookback_str = (ref_ts - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    ref_str = ref_ts.strftime("%Y-%m-%d %H:%M:%S")

    state = AnchorState()
    for underlying in _UNDERLYINGS:
        # Latest first-revision bar per strategy whose closing_ts <= ref_ts.
        tf_expr = (
            "multiIf(config_timeframe='1m',1,config_timeframe='3m',3,"
            "config_timeframe='5m',5,config_timeframe='15m',15,"
            "config_timeframe='30m',30,config_timeframe='1h',60,"
            "config_timeframe='4h',240,config_timeframe='1d',1440,5)"
        )
        bars_sql = f"""\
SELECT
    strategy_table_name,
    strategy_instance_id,
    strategy_id,
    strategy_name,
    underlying,
    config_timeframe,
    weighting,
    max(ts) AS latest_ts,
    JSONExtractFloat(argMin(row_json, revision_ts), 'cumulative_pnl') AS cumulative_pnl,
    JSONExtractFloat(argMin(row_json, revision_ts), 'position')       AS position,
    JSONExtractFloat(argMin(row_json, revision_ts), 'final_signal')   AS final_signal,
    JSONExtractFloat(argMin(row_json, revision_ts), 'benchmark')      AS benchmark,
    toString(max(ts) + toIntervalMinute({tf_expr}))                   AS closing_ts
FROM {cfg['history_table']}
WHERE underlying = '{underlying}'
  AND ts >= '{lookback_str}'
  AND ts + toIntervalMinute({tf_expr}) <= '{ref_str}'
GROUP BY strategy_table_name, strategy_instance_id, strategy_id,
         strategy_name, underlying, config_timeframe, weighting
ORDER BY strategy_instance_id, latest_ts DESC
LIMIT 1 BY strategy_instance_id
"""
        bars = query_dicts(bars_sql)
        if not bars:
            continue

        # Fetch prices at each bar's closing_ts in one query.
        closing_ts_list = ", ".join(f"'{r['closing_ts']}'" for r in bars)
        instrument = underlying + "USDT"
        price_sql = f"""\
SELECT toString(ts) AS ts, open
FROM analytics.futures_price_1min
WHERE exchange = 'binance'
  AND instrument = '{instrument}'
  AND ts IN ({closing_ts_list})
"""
        price_map = {r["ts"]: float(r["open"]) for r in query_dicts(price_sql)}

        for r in bars:
            stn = r["strategy_table_name"]
            price = price_map.get(r["closing_ts"], 0.0)
            state.set(
                stn,
                AnchorRecord(
                    pnl=float(r["cumulative_pnl"] or 0.0),
                    price=price,
                    position=float(r["position"] or 0.0),
                    strategy_instance_id=str(r["strategy_instance_id"]),
                    underlying=r["underlying"],
                    strategy_id=int(r["strategy_id"]),
                    strategy_name=r["strategy_name"],
                    config_timeframe=r["config_timeframe"],
                    weighting=float(r["weighting"]),
                    final_signal=float(r["final_signal"] or 0.0),
                    benchmark=float(r["benchmark"] or 0.0),
                ),
            )

    logger.info("Bootstrap [bt]: seeded %d strategies from row_json", len(state))
    return state


def _bootstrap_state(
    mode: str,
    reference_ts: "datetime | None",
) -> AnchorState:
    """Seed AnchorState for prod/real_trade modes."""
    cfg = _MODE_CONFIG[mode]
    is_bt = False
    now = datetime.now(UTC).replace(tzinfo=None)
    ref_ts = reference_ts if reference_ts is not None else now
    # seed_ts: far enough back (48h) that all strategies have a position row before
    # this cutoff, including 4h/1d timeframe strategies inactive for 24h+.
    # walk_ts: start of the walk window (2h). Walk covers [walk_ts, ref_ts).
    # walk_anchor_ts: the pnl/price baseline for the walk — last pnl row just before
    # walk_ts. This must come from walk_ts, NOT seed_ts, because Dagster may have
    # refreshed rows in the 46h gap between seed_ts and walk_ts, producing a different
    # PnL chain than the old consumer output at seed_ts.
    seed_ts = ref_ts - timedelta(hours=_SEED_HOURS)
    walk_ts = ref_ts - timedelta(hours=_WALK_HOURS)

    # Position seeds: read from seed_ts so 4h/1d strategies with no recent bar are found.
    seeds: list[BootstrapSeed] = fetch_bootstrap_seeds(
        pnl_table=cfg["pnl_table"],
        history_table=cfg["history_table"],
        start_ts=seed_ts,
        real_trade=cfg["real_trade"],
    )

    # Walk pnl/price anchors: last pnl row per strategy just before walk_ts.
    # This ensures prev_pnl and prev_price agree with the stored rows in [walk_ts, ref_ts),
    # preventing mismatch when Dagster refreshed rows in the 46h gap between seed_ts and walk_ts.
    walk_anchor_pnl, walk_anchor_price = _fetch_walk_anchors(cfg["pnl_table"], walk_ts, _UNDERLYINGS)

    state = AnchorState()
    for seed in seeds:
        # Use walk anchor pnl/price if available; fall back to seed pnl/price for strategies
        # with no pnl row before walk_ts (e.g. brand-new or long-inactive strategies).
        state.set(
            seed.strategy_table_name,
            AnchorRecord(
                pnl=walk_anchor_pnl.get(seed.strategy_table_name, seed.pnl),
                price=walk_anchor_price.get(seed.strategy_table_name, seed.price),
                position=seed.position,
                bar_ts=seed.bar_ts,
                revision_ts=seed.revision_ts,
                strategy_instance_id=seed.strategy_instance_id,
                underlying=seed.underlying,
                strategy_id=seed.strategy_id,
                strategy_name=seed.strategy_name,
                config_timeframe=seed.config_timeframe,
                weighting=seed.weighting,
                final_signal=seed.final_signal,
                benchmark=seed.benchmark,
            ),
        )

    walk_rows: list[WalkRow] = fetch_walk_rows(
        pnl_table=cfg["pnl_table"],
        history_table=cfg["history_table"],
        start_ts=walk_ts,
        reference_ts=ref_ts,
        real_trade=cfg["real_trade"],
    )

    logger.info(
        "Bootstrap [%s]: seed_ts=%s walk_ts=%s ref_ts=%s seeds=%d walk_anchors=%d walk_rows=%d",
        mode,
        seed_ts,
        walk_ts,
        ref_ts,
        len(seeds),
        len(walk_anchor_pnl),
        len(walk_rows),
    )

    prev_pnl: dict[str, float] = {
        s.strategy_table_name: walk_anchor_pnl.get(s.strategy_table_name, s.pnl)
        for s in seeds
    }
    prev_price: dict[str, float] = {
        s.strategy_table_name: walk_anchor_price.get(s.strategy_table_name, s.price)
        for s in seeds
    }

    for wr in walk_rows:
        stn = wr.strategy_table_name
        pp = prev_price.get(stn, 0.0)
        pp_pnl = prev_pnl.get(stn, 0.0)
        # Use previous price as fallback when current price is missing (0.0 from coalesce).
        effective_price = wr.price if wr.price != 0.0 else pp

        if not is_bt and pp != 0.0 and effective_price != 0.0 and wr.cumulative_pnl != 0.0:
            recomputed = pp_pnl + wr.position * (effective_price - pp) / pp
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
                    stn,
                    wr.ts,
                    wr.cumulative_pnl,
                    recomputed,
                    deviation,
                )

        prev_pnl[stn] = wr.cumulative_pnl
        prev_price[stn] = effective_price
        state.set(
            stn,
            AnchorRecord(
                pnl=wr.cumulative_pnl,
                price=effective_price,
                position=wr.position,
                bar_ts=wr.bar_ts,
                revision_ts=wr.revision_ts,
            ),
        )

    logger.info(
        "Bootstrap [%s]: seeded %d strategies, walked %d rows",
        mode,
        len(state),
        len(walk_rows),
    )
    return state


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
    """Compute one PnL row. Lazy-seeds from zero if strategy not yet in state.

    Always stores bar metadata on the anchor so carry-forward rows can be emitted
    if the next bar arrives late.
    """
    if not state.has(strategy_table_name):
        logger.info(
            "New strategy '%s' — seeding from zero at price=%.4f ts=%s",
            strategy_table_name,
            candle.open,
            candle.ts,
        )
        state.set(
            strategy_table_name, AnchorRecord(pnl=0.0, price=candle.open, position=0.0)
        )
    meta = AnchorRecord(
        strategy_id=bar.strategy_id,
        strategy_name=bar.strategy_name,
        underlying=bar.underlying,
        config_timeframe=bar.config_timeframe,
        weighting=bar.weighting,
        strategy_instance_id=bar.strategy_instance_id,
        final_signal=bar.final_signal,
        benchmark=bar.benchmark,
    )
    pnl = state.compute_pnl(
        strategy_table_name,
        candle.open,
        bar.position,
        bar_ts=bar_ts or datetime.min,
        revision_ts=revision_ts or datetime.min,
        meta=meta,
    )
    return build_pnl_row(
        strategy_table_name,
        {
            "strategy_id": bar.strategy_id,
            "strategy_name": bar.strategy_name,
            "underlying": bar.underlying,
            "config_timeframe": bar.config_timeframe,
            "weighting": bar.weighting,
            "strategy_instance_id": bar.strategy_instance_id,
            "final_signal": bar.final_signal,
            "bar_benchmark": bar.benchmark,
            "position": bar.position,
        },
        candle.open,
        pnl,
        source_label,
        candle.ts,
        now,
    )


def _carry_forward_row(
    state: AnchorState,
    strategy_table_name: str,
    candle: CandleEvent,
    source_label: str,
    now: datetime,
) -> "list | None":
    """Emit a PnL row using the last known position when no bar is active.

    Called for strategies that were active last candle but absent from the current
    candle lookup — their next bar arrived late. Holds the previous position and
    advances the PnL chain until a new bar appears.

    Returns None if the anchor has no metadata (strategy was never seen with a bar).
    """
    rec = state.get(strategy_table_name)
    if not rec.strategy_instance_id:
        return None
    pnl = state.compute_pnl(
        strategy_table_name,
        candle.open,
        rec.position,
        bar_ts=rec.bar_ts,
        revision_ts=rec.revision_ts,
    )
    return build_carry_forward_row(
        strategy_table_name,
        rec,
        candle.open,
        pnl,
        source_label,
        candle.ts,
        now,
    )


def process_candle(
    candle: CandleEvent,
    state_prod: AnchorState,
    state_real_trade: AnchorState,
    state_bt: AnchorState,
    cfg: SinkConfig,
) -> tuple[list[dict], int, int, int]:
    """Compute all output rows for one candle.

    Returns (rows, prod_fetched, bt_fetched, real_trade_fetched) where the
    fetched counts are the number of strategy instances returned by each
    candle lookup query — used by the caller to validate flush completeness.

    For prod/bt: strategies in state that return no active bar receive a
    carry-forward row using their last known position. For real_trade:
    strategies whose revision guard returns False (revision already applied)
    also get a carry-forward row, ensuring continuous per-minute coverage.
    Strategies in state that weren't returned by the candle query also get
    carry-forward rows (for their matching underlying).
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    rows: list[dict] = []
    prod_fetched = bt_fetched = real_trade_fetched = 0
    candle_underlying = candle.instrument.removesuffix("USDT")

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

    if cfg.prod:
        prod_bars = fetch_strategies_for_candle(candle.instrument, candle.ts)
        prod_fetched = len(prod_bars)
        fetched_prod_stns: set[str] = set()
        for bar in prod_bars:
            fetched_prod_stns.add(bar.strategy_table_name)
            rows.append(
                {
                    "_sink": "pnl_prod",
                    "_row": _compute_pnl_row(
                        state_prod,
                        bar.strategy_table_name,
                        candle,
                        bar,
                        "production",
                        now,
                    ),
                }
            )
        # Carry-forward: hold position for strategies in state that returned no bar.
        # Only fire for strategies whose underlying matches this candle's instrument —
        # otherwise a SOL candle would carry-forward FET/ETH strategies at SOL's price.
        for stn in list(state_prod.keys()):
            if (
                stn not in fetched_prod_stns
                and state_prod.get(stn).underlying == candle_underlying
            ):
                row = _carry_forward_row(state_prod, stn, candle, "production", now)
                if row is not None:
                    rows.append({"_sink": "pnl_prod", "_row": row})

    if cfg.bt:
        bt_bars = fetch_bt_strategies_for_candle(candle.instrument, candle.ts)
        bt_fetched = len(bt_bars)
        fetched_bt_stns: set[str] = set()
        for bar in bt_bars:
            fetched_bt_stns.add(bar.strategy_table_name)
            rows.append(
                {
                    "_sink": "pnl_bt",
                    "_row": _compute_pnl_row(
                        state_bt,
                        bar.strategy_table_name,
                        candle,
                        bar,
                        "backtest",
                        now,
                    ),
                }
            )
        for stn in list(state_bt.keys()):
            if (
                stn not in fetched_bt_stns
                and state_bt.get(stn).underlying == candle_underlying
            ):
                row = _carry_forward_row(state_bt, stn, candle, "backtest", now)
                if row is not None:
                    rows.append({"_sink": "pnl_bt", "_row": row})

    if cfg.real_trade:
        rt_revs = fetch_real_trade_for_candle(candle.instrument, candle.ts)
        fetched_rt_stns: set[str] = set()
        for rev in rt_revs:
            fetched_rt_stns.add(rev.strategy_table_name)
            if not state_real_trade.should_apply_revision(
                rev.strategy_table_name, rev.bar_ts, rev.revision_ts
            ):
                # Revision already applied — carry forward with same position/anchor.
                row = _carry_forward_row(
                    state_real_trade, rev.strategy_table_name, candle, "real_trade", now
                )
                if row is not None:
                    rows.append({"_sink": "pnl_real_trade", "_row": row})
                continue
            real_trade_fetched += 1
            rows.append(
                {
                    "_sink": "pnl_real_trade",
                    "_row": _compute_pnl_row(
                        state_real_trade,
                        rev.strategy_table_name,
                        candle,
                        rev,
                        "real_trade",
                        now,
                        rev.bar_ts,
                        rev.revision_ts,
                    ),
                }
            )
        # Carry-forward for real_trade strategies not returned by the candle query
        # (e.g. strategies whose lookback window expired or had no recent bar).
        for stn in list(state_real_trade.keys()):
            if (
                stn not in fetched_rt_stns
                and state_real_trade.get(stn).underlying == candle_underlying
            ):
                row = _carry_forward_row(
                    state_real_trade, stn, candle, "real_trade", now
                )
                if row is not None:
                    rows.append({"_sink": "pnl_real_trade", "_row": row})

    return rows, prod_fetched, bt_fetched, real_trade_fetched


_SIID_COL = INSERT_COLUMNS.index("strategy_instance_id")  # 15


def _flush_candle(
    consumer: Consumer,
    price_row: "list | None",
    pnl_prod_rows: list[list],
    pnl_real_trade_rows: list[list],
    pnl_bt_rows: list[list],
    expected_prod: int = 0,
    expected_bt: int = 0,
    expected_real_trade: int = 0,
) -> None:
    """Write one candle's rows to ClickHouse, then commit its Kafka offset.

    Called once per candle. The offset is committed only after every ClickHouse
    insert for this candle succeeds. If any write fails, the exception propagates
    to the main loop which exits without committing — the consumer restarts from
    the same Kafka position so the candle price is replayed from the original message.
    """
    for rows, expected, label in (
        (pnl_prod_rows, expected_prod, "prod"),
        (pnl_bt_rows, expected_bt, "bt"),
        (pnl_real_trade_rows, expected_real_trade, "real_trade"),
    ):
        if not rows or not expected:
            continue
        actual = len({row[_SIID_COL] for row in rows})
        if actual < expected:
            raise RuntimeError(
                f"Flush completeness check failed [{label}]: "
                f"{actual} distinct strategy_instance_ids < "
                f"{expected} fetched from history table. "
                "Refusing to sink partial data."
            )

    # Insert all sinks. Any failure here raises before the commit below.
    if price_row:
        insert_rows("analytics.futures_price_1min", PRICE_COLUMNS, [price_row])
    if pnl_prod_rows:
        insert_rows(
            "analytics.strategy_pnl_1min_prod_v2", INSERT_COLUMNS, pnl_prod_rows
        )
    if pnl_real_trade_rows:
        insert_rows(
            "analytics.strategy_pnl_1min_real_trade_v2",
            INSERT_COLUMNS,
            pnl_real_trade_rows,
        )
    if pnl_bt_rows:
        insert_rows("analytics.strategy_pnl_1min_bt_v2", INSERT_COLUMNS, pnl_bt_rows)

    # Commit the offset only after all inserts confirmed.
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        if e.args[0].code() != KafkaError._NO_OFFSET:
            raise


def emit_candle_metrics(
    candle_ts: datetime,
    cw_client: Any,
    sink: str,
    messages_received: int,
    prod_rows: int,
    real_trade_rows: int,
    bt_rows: int,
) -> None:
    try:
        lag = (datetime.now(UTC).replace(tzinfo=None) - candle_ts).total_seconds()
        dims = [{"Name": "Sink", "Value": sink}]
        metric_data = [
            {
                "MetricName": "CandleLagSeconds",
                "Value": lag,
                "Unit": "Seconds",
                "Dimensions": dims,
            },
            {
                "MetricName": "CandleProcessingTs",
                "Value": candle_ts.timestamp(),
                "Unit": "None",
                "Dimensions": dims,
            },
            {
                "MetricName": "MessagesReceived",
                "Value": messages_received,
                "Unit": "Count",
                "Dimensions": dims,
            },
        ]
        if prod_rows:
            metric_data.append(
                {
                    "MetricName": "ClickHouseSinkProd",
                    "Value": prod_rows,
                    "Unit": "Count",
                    "Dimensions": dims,
                }
            )
        if real_trade_rows:
            metric_data.append(
                {
                    "MetricName": "ClickHouseSinkRealTrade",
                    "Value": real_trade_rows,
                    "Unit": "Count",
                    "Dimensions": dims,
                }
            )
        if bt_rows:
            metric_data.append(
                {
                    "MetricName": "ClickHouseSinkBt",
                    "Value": bt_rows,
                    "Unit": "Count",
                    "Dimensions": dims,
                }
            )
        cw_client.put_metric_data(Namespace="trading-analysis", MetricData=metric_data)
    except Exception:
        logger.warning("Failed to emit candle metrics", exc_info=True)


def run() -> None:
    logging.basicConfig(level=logging.INFO)
    sink_cfg = SinkConfig.from_env()
    logger.info(
        "Sink config: price=%s prod=%s real_trade=%s bt=%s",
        sink_cfg.price,
        sink_cfg.prod,
        sink_cfg.real_trade,
        sink_cfg.bt,
    )

    reference_ts = peek_reference_ts(os.environ["REDPANDA_BROKERS"], resolve_group_id())
    if reference_ts is not None:
        logger.info("Cold-start reference_ts from committed offset: %s", reference_ts)
    else:
        logger.info("No committed offset — using now() as reference_ts")

    state_prod = AnchorState()
    state_bt = AnchorState()
    state_real_trade = AnchorState()

    try:
        if sink_cfg.prod:
            state_prod = _bootstrap_state("prod", reference_ts)
        if sink_cfg.bt:
            state_bt = _bootstrap_bt_state(reference_ts)
        if sink_cfg.real_trade:
            state_real_trade = _bootstrap_state("real_trade", reference_ts)
    except Exception:
        logger.exception("Fatal error during bootstrap — exiting")
        sys.exit(1)

    cw_client = boto3.client(
        "cloudwatch", region_name=os.environ.get("AWS_REGION", "ap-northeast-1")
    )
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

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received — closing consumer")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    messages_received = 0

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

            messages_received += 1
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
                "Candle %s open=%.2f ts=%s", candle.instrument, candle.open, candle.ts
            )

            # Compute all rows for this candle. Any exception here (ClickHouse fetch
            # failure, computation error) leaves the offset uncommitted — the consumer
            # restarts from the same Kafka position and replays with the original price.
            rows, prod_fetched, bt_fetched, rt_fetched = process_candle(
                candle,
                state_prod,
                state_real_trade,
                state_bt,
                sink_cfg,
            )

            price_row: list | None = None
            pnl_prod_rows: list[list] = []
            pnl_real_trade_rows: list[list] = []
            pnl_bt_rows: list[list] = []

            for row in rows:
                sink = row["_sink"]
                if sink == "price":
                    price_row = [
                        row["exchange"],
                        row["instrument"],
                        row["ts"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                    ]
                elif sink == "pnl_prod":
                    pnl_prod_rows.append(row["_row"])
                elif sink == "pnl_real_trade":
                    pnl_real_trade_rows.append(row["_row"])
                elif sink == "pnl_bt":
                    pnl_bt_rows.append(row["_row"])

            # Write all rows for this candle to ClickHouse, then commit the offset.
            # If any insert fails the exception propagates here — consumer exits and
            # restarts from this candle's offset, replaying with the live Kafka price.
            _flush_candle(
                consumer,
                price_row,
                pnl_prod_rows,
                pnl_real_trade_rows,
                pnl_bt_rows,
                expected_prod=prod_fetched,
                expected_bt=bt_fetched,
                expected_real_trade=rt_fetched,
            )
            emit_candle_metrics(
                candle.ts,
                cw_client,
                sink_label,
                messages_received=messages_received,
                prod_rows=len(pnl_prod_rows),
                real_trade_rows=len(pnl_real_trade_rows),
                bt_rows=len(pnl_bt_rows),
            )
            messages_received = 0
            logger.info(
                "Flushed candle %s ts=%s price=%s prod=%d rt=%d bt=%d",
                candle.instrument,
                candle.ts,
                price_row is not None,
                len(pnl_prod_rows),
                len(pnl_real_trade_rows),
                len(pnl_bt_rows),
            )

    except Exception:
        logger.exception("Fatal error in consumer loop")
        consumer.close()
        sys.exit(1)


if __name__ == "__main__":
    run()
