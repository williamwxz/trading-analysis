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
from streaming.binance_ws_consumer import INSTRUMENTS
from streaming.models import CandleEvent

from libs.clickhouse_client import insert_rows, query_dicts
from libs.computation import (
    INSERT_COLUMNS,
    AnchorRecord,
    AnchorState,
    BootstrapSeed,
    BtLiveAnchor,
    WalkRow,
    build_carry_forward_row,
    build_pnl_row,
    fetch_bootstrap_seeds,
    fetch_bt_anchors_for_candle,
    fetch_last_pnl_anchors,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
    fetch_walk_rows,
)

logger = logging.getLogger(__name__)

TOPIC = "binance.price.ticks"
_UNDERLYINGS = [inst.removesuffix("USDT") for inst in INSTRUMENTS]
_DEFAULT_GROUP_ID = "flink-pnl-consumer"

_WALK_HOURS = (
    2  # walk [ref_ts - 2h, ref_ts) to advance anchor price/position to present
)
_PNL_WARN_TOLERANCE = 1e-6
# Abort the cold-start bootstrap only on a GROSS cumulative_pnl deviation that
# signals real corruption. Was 2e-3 (0.2%), which crash-looped every restart:
# late-arriving revisions routinely shift recent cpnl by ~0.3-1% now that the
# Dagster batch recompute (which reconciled them) is deprecated, so the walk found
# small drift on data the consumer itself had just written and aborted. The walk
# already resumes from the STORED values regardless of this check (see the walk
# loop), so aborting never repaired anything — it only blocked recovery. bt skips
# the check entirely; prod/rt now warn on drift and abort only when clearly broken.
_PNL_ABORT_TOLERANCE = 0.5


def _walk_deviation_action(deviation: float) -> str:
    """Classify a cold-start walk-verify cpnl deviation: "ok" | "warn" | "abort".

    abort only above _PNL_ABORT_TOLERANCE (gross/corruption-scale); warn above
    _PNL_WARN_TOLERANCE (routine late-revision drift); otherwise ok.
    """
    if deviation > _PNL_ABORT_TOLERANCE:
        return "abort"
    if deviation > _PNL_WARN_TOLERANCE:
        return "warn"
    return "ok"


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

# Bootstrap config for the prod/real_trade modes (seed position + revision guard
# from the history table) — read only by _bootstrap_state. BT is absent BY DESIGN
# even though its live path is now stateful (minute-chaining): bt seeds only
# (pnl, price) from its own pnl table via _bootstrap_bt_state and gets position
# per-candle from the cum table, so it needs no history-shaped entry here.
_MODE_CONFIG = {
    "prod": {
        "pnl_table": "analytics.strategy_pnl_1min_prod_v2",
        "history_table": "analytics.strategy_output_history_v2",
        "source_label": "production",
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
    """Return min candle ts at this group's committed offsets.

    Behavior:
      - All partitions have committed offsets and we can read at least one
        candle → returns min(candle.ts).
      - All partitions are OFFSET_INVALID (genuinely fresh group: no prior
        commits) → falls back to latest watermark; returns the most recent
        candle's ts, or None if topic is empty.
      - Any error talking to Kafka (timeout, broker unreachable, etc.) →
        raises, so the caller exits without committing anything. We never
        silently fall back to now() because that would let a subsequent
        successful commit advance the offset past unprocessed messages,
        permanently losing the backlog.

    Raises:
        RuntimeError: wraps any underlying Kafka exception encountered while
        listing topics, fetching committed offsets, or polling.
    """
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
        except Exception as exc:
            raise RuntimeError(
                f"peek_reference_ts: list_topics({topic!r}) failed; refusing "
                f"to fall back to now() — see commit message for rationale"
            ) from exc
        partitions = (
            [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
            if topic in meta.topics
            else []
        )

        try:
            committed = consumer.committed(partitions, timeout=timeout)
        except Exception as exc:
            raise RuntimeError(
                f"peek_reference_ts: committed() failed for {len(partitions)} "
                f"partitions of {topic!r}; refusing to fall back to now()"
            ) from exc

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


def _bootstrap_state(
    mode: str,
    reference_ts: "datetime | None",
) -> AnchorState:
    """Seed AnchorState for prod/real_trade modes."""
    cfg = _MODE_CONFIG[mode]
    now = datetime.now(UTC).replace(tzinfo=None)
    ref_ts = reference_ts if reference_ts is not None else now
    pnl_table = str(cfg["pnl_table"])
    history_table = str(cfg["history_table"])
    real_trade = bool(cfg["real_trade"])
    # walk_ts: start of the verification-only walk window (2h). The walk replays the
    # last 2h of stored rows to catch upstream corruption — it does NOT seed the live
    # anchor (continuous seeding from the last stored row does, below).
    walk_ts = ref_ts - timedelta(hours=_WALK_HOURS)

    # Position + revision guard: the latest accepted revision as of ref_ts.
    seeds: list[BootstrapSeed] = fetch_bootstrap_seeds(
        pnl_table=pnl_table,
        history_table=history_table,
        start_ts=ref_ts,
        real_trade=real_trade,
    )
    seed_by_stn: dict[str, BootstrapSeed] = {s.strategy_table_name: s for s in seeds}

    # Continuous PnL/price anchor: each strategy's ACTUAL last stored pnl row. The
    # universe is every strategy in the pnl table, with an unbounded fallback for the
    # quiet ones, so the first post-restart write chains exactly from the stored
    # value — no re-anchor step.
    last_anchors = fetch_last_pnl_anchors(pnl_table, ref_ts)

    state = AnchorState()
    for stn, anchor in last_anchors.items():
        seed = seed_by_stn.get(stn)
        if seed is not None:
            state.set(
                stn,
                AnchorRecord(
                    pnl=anchor.pnl,
                    price=anchor.price,
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
        else:
            # PnL chain exists but no current revision (retired/quiet) — seed flat;
            # guard stays at datetime.min so any future revision is accepted.
            state.set(
                stn, AnchorRecord(pnl=anchor.pnl, price=anchor.price, position=0.0)
            )

    # Brand-new strategies: a current revision but no stored pnl row yet.
    for seed in seeds:
        if seed.strategy_table_name in last_anchors:
            continue
        state.set(
            seed.strategy_table_name,
            AnchorRecord(
                pnl=seed.pnl,
                price=seed.price,
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

    # ── Verification-only walk: replay the last 2h, abort on gross corruption ──
    walk_anchor_pnl, walk_anchor_price = _fetch_walk_anchors(
        pnl_table, walk_ts, _UNDERLYINGS
    )
    walk_rows: list[WalkRow] = fetch_walk_rows(
        pnl_table=pnl_table,
        history_table=history_table,
        start_ts=walk_ts,
        reference_ts=ref_ts,
        real_trade=real_trade,
    )

    bounded = sum(1 for a in last_anchors.values() if a.ts >= walk_ts)
    oldest_min = (
        int(max((ref_ts - a.ts).total_seconds() // 60 for a in last_anchors.values()))
        if last_anchors
        else 0
    )
    logger.info(
        "Bootstrap [%s]: ref_ts=%s anchored=%d (bounded=%d fallback=%d oldest=%dmin) "
        "seeds=%d walk_rows=%d (verify-only)",
        mode,
        ref_ts,
        len(last_anchors),
        bounded,
        len(last_anchors) - bounded,
        oldest_min,
        len(seeds),
        len(walk_rows),
    )

    prev_pnl: dict[str, float] = dict(walk_anchor_pnl)
    prev_price: dict[str, float] = dict(walk_anchor_price)

    for wr in walk_rows:
        stn = wr.strategy_table_name
        pp = prev_price.get(stn, 0.0)
        pp_pnl = prev_pnl.get(stn, 0.0)
        # Use previous price as fallback when current price is missing (0.0 from coalesce).
        effective_price = wr.price if wr.price != 0.0 else pp

        if pp != 0.0 and effective_price != 0.0 and wr.cumulative_pnl != 0.0:
            recomputed = pp_pnl + wr.position * (effective_price - pp) / pp
            deviation = abs(recomputed - wr.cumulative_pnl)
            action = _walk_deviation_action(deviation)
            if action == "abort":
                raise RuntimeError(
                    f"Cold-start PnL mismatch for {stn} at {wr.ts}: "
                    f"stored={wr.cumulative_pnl:.8f} recomputed={recomputed:.8f} "
                    f"delta={deviation:.2e} > {_PNL_ABORT_TOLERANCE:.1e}"
                )
            if action == "warn":
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
        # verification-only: the walk no longer seeds the live AnchorState.

    return state


_BT_PNL_TABLE = "analytics.strategy_pnl_1min_bt_v2"


def _bootstrap_bt_state(reference_ts: "datetime | None") -> AnchorState:
    """Seed bt AnchorState from the last stored 1-min row per strategy.

    Continuous seeding: each strategy's last (cumulative_pnl, price) becomes the
    chain anchor so the first post-restart candle chains exactly from the stored
    value. Position is NOT seeded here — the live loop passes the active cum bar's
    pos_first into compute_pnl each candle. Brand-new strategies (no stored row)
    lazy-seed from cum_pnl_first on first appearance. Metadata (instance_id,
    underlying, ...) is populated on the first live candle. No history-shaped seeds
    and no walk-verify (bt's source is the authoritative cum table).
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    ref_ts = reference_ts if reference_ts is not None else now
    anchors = fetch_last_pnl_anchors(_BT_PNL_TABLE, ref_ts)
    state = AnchorState()
    for stn, anchor in anchors.items():
        state.set(stn, AnchorRecord(pnl=anchor.pnl, price=anchor.price, position=0.0))
    logger.info("Bootstrap [bt]: ref_ts=%s anchored=%d", ref_ts, len(anchors))
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


def _build_bt_live_row(
    state: AnchorState, a: BtLiveAnchor, candle: CandleEvent, now: datetime
) -> list:
    """Stateful BT row: chain cpnl via AnchorState; position is the active cum
    bar's pos_first. A brand-new strategy lazy-seeds cum_pnl_first with price=0,
    so its first minute holds cum_pnl_first and establishes the price reference."""
    if not state.has(a.strategy_table_name):
        state.set(
            a.strategy_table_name,
            AnchorRecord(pnl=a.cum_pnl_first, price=0.0, position=a.pos_first),
        )
    meta = AnchorRecord(
        strategy_id=a.strategy_id,
        strategy_name=a.strategy_name,
        underlying=a.underlying,
        config_timeframe=a.config_timeframe,
        weighting=a.weighting,
        strategy_instance_id=a.strategy_instance_id,
        final_signal=0.0,
        benchmark=a.benchmark,
    )
    cpnl = state.compute_pnl(a.strategy_table_name, candle.open, a.pos_first, meta=meta)
    return build_pnl_row(
        a.strategy_table_name,
        {
            "strategy_id": a.strategy_id,
            "strategy_name": a.strategy_name,
            "underlying": a.underlying,
            "config_timeframe": a.config_timeframe,
            "weighting": a.weighting,
            "strategy_instance_id": a.strategy_instance_id,
            "final_signal": 0.0,
            "bar_benchmark": a.benchmark,
            "position": a.pos_first,
        },
        candle.open,
        cpnl,
        "backtest",
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
    cfg: SinkConfig,
    state_bt: "AnchorState | None" = None,
) -> tuple[list[dict], int, int, int]:
    """Compute all output rows for one candle.

    Returns (rows, prod_fetched, bt_fetched, real_trade_fetched) where the
    fetched counts are the number of strategy instances returned by each
    candle lookup query — used by the caller to validate flush completeness.

    For prod: strategies in state that return no active bar receive a
    carry-forward row using their last known position. For real_trade:
    strategies whose revision guard returns False (revision already applied)
    also get a carry-forward row, ensuring continuous per-minute coverage.
    Strategies in state that weren't returned by the candle query also get
    carry-forward rows (for their matching underlying).

    BT chains minute-to-minute via state_bt: fetch_bt_anchors_for_candle supplies
    the active cum bar's pos_first, the cpnl chains from the prior minute, and
    strategies absent from the lookup carry forward — same shape as prod.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    if state_bt is None:  # only used when cfg.bt; fresh state is fine for tests
        state_bt = AnchorState()
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
        bt_anchors = fetch_bt_anchors_for_candle(candle.instrument, candle.ts)
        bt_fetched = len(bt_anchors)
        fetched_bt_stns: set[str] = set()
        for a in bt_anchors:
            fetched_bt_stns.add(a.strategy_table_name)
            rows.append(
                {
                    "_sink": "pnl_bt",
                    "_row": _build_bt_live_row(state_bt, a, candle, now),
                }
            )
        # Carry-forward bt strategies in state but absent from this candle's lookup
        # (retired / no cum bar in lookback), matching this candle's underlying.
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

    # peek_reference_ts now raises on Kafka errors instead of falling back to
    # now(), so a broker outage doesn't allow a later successful commit to
    # advance past unprocessed messages. None is only returned when the topic
    # is genuinely empty (true fresh deploy).
    reference_ts = peek_reference_ts(os.environ["REDPANDA_BROKERS"], resolve_group_id())
    if reference_ts is not None:
        logger.info("Cold-start reference_ts from committed offset: %s", reference_ts)
    else:
        logger.info(
            "No reference_ts (empty topic, fresh group) — bootstrap will seed "
            "from now() with no walk-verify"
        )

    state_prod = AnchorState()
    state_real_trade = AnchorState()
    state_bt = AnchorState()

    try:
        if sink_cfg.prod:
            state_prod = _bootstrap_state("prod", reference_ts)
        if sink_cfg.real_trade:
            state_real_trade = _bootstrap_state("real_trade", reference_ts)
        if sink_cfg.bt:
            state_bt = _bootstrap_bt_state(reference_ts)
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
                sink_cfg,
                state_bt,
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
