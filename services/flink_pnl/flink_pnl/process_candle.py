"""Pure per-candle PnL computation for the Flink streaming path.

process_candle() is intentionally free of I/O side effects beyond the fetch_*
calls — all state mutation happens on the in-memory StateMap dicts passed in
by the caller (prod, real_trade, and bt; bt chains via state_bt).

Output format: each element is a dict with a ``_sink`` key that routes the
row to the appropriate ClickHouse table:

    {"_sink": "pnl_prod",       "_row": <INSERT_COLUMNS list>}
    {"_sink": "pnl_bt",         "_row": <INSERT_COLUMNS list>}
    {"_sink": "pnl_real_trade", "_row": <INSERT_COLUMNS list>}
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from streaming.models import CandleEvent

from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap
from libs.computation.anchor_state import AnchorRecord
from libs.computation.candle_lookup import (
    BtLiveAnchor,
    StrategyBar,
    StrategyRevision,
    fetch_bt_anchors_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
)
from libs.computation.pnl_formula import (
    build_carry_forward_row,
    build_pnl_row,
)


def _bar_to_dict(bar: StrategyBar) -> dict[str, Any]:
    """Convert a StrategyBar to the dict shape expected by build_pnl_row."""
    return {
        "strategy_id": bar.strategy_id,
        "strategy_name": bar.strategy_name,
        "underlying": bar.underlying,
        "config_timeframe": bar.config_timeframe,
        "weighting": bar.weighting,
        "strategy_instance_id": bar.strategy_instance_id,
        "final_signal": bar.final_signal,
        "bar_benchmark": bar.benchmark,
        "position": bar.position,
    }


def _revision_to_dict(rev: StrategyRevision) -> dict[str, Any]:
    """Convert a StrategyRevision to the dict shape expected by build_pnl_row."""
    return {
        "strategy_id": rev.strategy_id,
        "strategy_name": rev.strategy_name,
        "underlying": rev.underlying,
        "config_timeframe": rev.config_timeframe,
        "weighting": rev.weighting,
        "strategy_instance_id": rev.strategy_instance_id,
        "final_signal": rev.final_signal,
        "bar_benchmark": rev.benchmark,
        "position": rev.position,
    }


def _bt_anchor_to_dict(a: BtLiveAnchor) -> dict[str, Any]:
    """Convert a BtLiveAnchor to the dict shape expected by build_pnl_row."""
    return {
        "strategy_id": a.strategy_id,
        "strategy_name": a.strategy_name,
        "underlying": a.underlying,
        "config_timeframe": a.config_timeframe,
        "weighting": a.weighting,
        "strategy_instance_id": a.strategy_instance_id,
        "final_signal": 0.0,
        "bar_benchmark": a.benchmark,
        "position": a.pos_first,
    }


def _process_bt(
    candle: CandleEvent,
    underlying: str,
    state_bt: StateMap,
    anchors_by_stn: dict[str, BtLiveAnchor],
    now: datetime,
) -> list[dict[str, Any]]:
    """Emit BT rows with minute-chaining via state_bt (mirrors _process_mode).

    Position each minute is the active cum bar's pos_first. A brand-new strategy
    lazy-seeds cum_pnl_first with price=0, so its first minute holds cum_pnl_first
    and establishes the price reference; thereafter cpnl chains. Strategies in
    state with no anchor this candle carry forward at the last position.
    """
    output: list[dict[str, Any]] = []
    seen_stns: set[str] = set()
    if underlying not in state_bt:
        state_bt[underlying] = {}

    for stn, a in anchors_by_stn.items():
        seen_stns.add(stn)
        if stn not in state_bt[underlying]:
            # cold-start: seed cum_pnl_first with price=0 (first minute holds it)
            state_bt[underlying][stn] = AnchorRecord(
                pnl=a.cum_pnl_first, price=0.0, position=a.pos_first
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
        new_pnl, _ = _advance_anchor(
            state_bt, underlying, stn, candle.open, a.pos_first,
            datetime.min, datetime.min, meta,
        )
        row = build_pnl_row(
            stn, _bt_anchor_to_dict(a), candle.open, new_pnl, "backtest", candle.ts, now
        )
        output.append({"_sink": "pnl_bt", "_row": row})

    # Carry-forward: bt strategies in state with no anchor this candle.
    for stn, rec in state_bt.get(underlying, {}).items():
        if stn in seen_stns or not rec.strategy_instance_id:
            continue
        meta = AnchorRecord(
            strategy_id=rec.strategy_id,
            strategy_name=rec.strategy_name,
            underlying=rec.underlying,
            config_timeframe=rec.config_timeframe,
            weighting=rec.weighting,
            strategy_instance_id=rec.strategy_instance_id,
            final_signal=rec.final_signal,
            benchmark=rec.benchmark,
        )
        new_pnl, updated_rec = _advance_anchor(
            state_bt, underlying, stn, candle.open, rec.position, rec.bar_ts,
            rec.revision_ts, meta,
        )
        row = build_carry_forward_row(
            stn, updated_rec, candle.open, new_pnl, "backtest", candle.ts, now
        )
        output.append({"_sink": "pnl_bt", "_row": row})

    return output


def _advance_anchor(
    state_map: StateMap,
    underlying: str,
    stn: str,
    candle_open: float,
    position: float,
    bar_ts: datetime,
    revision_ts: datetime,
    meta: AnchorRecord,
) -> tuple[float, AnchorRecord]:
    """Advance anchor chain for one strategy. Returns (new_pnl, updated_record).

    Mutates state_map in-place. If no prior anchor exists for (underlying, stn),
    initialises with zero pnl and zero price (first-seen strategy).
    """
    if underlying not in state_map:
        state_map[underlying] = {}

    rec = state_map[underlying].get(
        stn, AnchorRecord(pnl=0.0, price=candle_open, position=0.0)
    )

    if rec.price == 0.0:
        new_pnl = rec.pnl
    else:
        new_pnl = rec.pnl + position * (candle_open - rec.price) / rec.price

    updated = AnchorRecord(
        pnl=new_pnl,
        price=candle_open,
        position=position,
        bar_ts=bar_ts,
        revision_ts=revision_ts,
        strategy_id=meta.strategy_id,
        strategy_name=meta.strategy_name,
        underlying=meta.underlying,
        config_timeframe=meta.config_timeframe,
        weighting=meta.weighting,
        strategy_instance_id=meta.strategy_instance_id,
        final_signal=meta.final_signal,
        benchmark=meta.benchmark,
    )
    state_map[underlying][stn] = updated
    return new_pnl, updated


def _process_mode(
    candle: CandleEvent,
    underlying: str,
    state_map: StateMap,
    bars_by_stn: dict[str, StrategyBar | StrategyRevision],
    sink_key: str,
    source_label: str,
    now: datetime,
    is_real_trade: bool = False,
) -> list[dict[str, Any]]:
    """Emit PnL rows for one mode (prod/bt/real_trade).

    For strategies with a new bar: compute updated PnL, emit a new-bar row.
    For strategies in state that have no new bar (carry-forward): advance anchor
    with the same position, emit a carry-forward row. Carry-forward only fires
    for strategies whose underlying matches the current candle's underlying.
    """
    output: list[dict[str, Any]] = []
    seen_stns: set[str] = set()

    # --- New bars / revisions ---
    for stn, bar_or_rev in bars_by_stn.items():
        seen_stns.add(stn)

        if is_real_trade:
            rev: StrategyRevision = bar_or_rev  # type: ignore[assignment]

            # Revision guard: apply only if (bar_ts, revision_ts) > anchor's pair.
            existing_rec = (state_map.get(underlying) or {}).get(stn, AnchorRecord())
            should_apply = (rev.bar_ts, rev.revision_ts) > (
                existing_rec.bar_ts,
                existing_rec.revision_ts,
            )

            if should_apply:
                # Apply the new revision.
                meta = AnchorRecord(
                    strategy_id=rev.strategy_id,
                    strategy_name=rev.strategy_name,
                    underlying=rev.underlying,
                    config_timeframe=rev.config_timeframe,
                    weighting=rev.weighting,
                    strategy_instance_id=rev.strategy_instance_id,
                    final_signal=rev.final_signal,
                    benchmark=rev.benchmark,
                )
                new_pnl, updated_rec = _advance_anchor(
                    state_map,
                    underlying,
                    stn,
                    candle.open,
                    rev.position,
                    rev.bar_ts,
                    rev.revision_ts,
                    meta,
                )
                row = build_pnl_row(
                    stn,
                    _revision_to_dict(rev),
                    candle.open,
                    new_pnl,
                    source_label,
                    candle.ts,
                    now,
                )
            else:
                # Guard blocked — carry-forward with existing anchor.
                rec = existing_rec
                meta = AnchorRecord(
                    strategy_id=rec.strategy_id,
                    strategy_name=rec.strategy_name,
                    underlying=rec.underlying,
                    config_timeframe=rec.config_timeframe,
                    weighting=rec.weighting,
                    strategy_instance_id=rec.strategy_instance_id,
                    final_signal=rec.final_signal,
                    benchmark=rec.benchmark,
                )
                new_pnl, updated_rec = _advance_anchor(
                    state_map,
                    underlying,
                    stn,
                    candle.open,
                    rec.position,
                    rec.bar_ts,
                    rec.revision_ts,
                    meta,
                )
                row = build_carry_forward_row(
                    stn,
                    updated_rec,
                    candle.open,
                    new_pnl,
                    source_label,
                    candle.ts,
                    now,
                )
        else:
            bar: StrategyBar = bar_or_rev  # type: ignore[assignment]
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
            from datetime import datetime as _dt

            _epoch = _dt.min
            new_pnl, _ = _advance_anchor(
                state_map,
                underlying,
                stn,
                candle.open,
                bar.position,
                _epoch,
                _epoch,
                meta,
            )
            row = build_pnl_row(
                stn,
                _bar_to_dict(bar),
                candle.open,
                new_pnl,
                source_label,
                candle.ts,
                now,
            )

        output.append({"_sink": sink_key, "_row": row})

    # --- Carry-forward: strategies in state with no new bar this candle ---
    existing_underlying_state = state_map.get(underlying, {})
    for stn, rec in existing_underlying_state.items():
        if stn in seen_stns:
            continue
        # Only carry-forward if the record has metadata (was ever seen).
        if not rec.strategy_instance_id:
            continue

        meta = AnchorRecord(
            strategy_id=rec.strategy_id,
            strategy_name=rec.strategy_name,
            underlying=rec.underlying,
            config_timeframe=rec.config_timeframe,
            weighting=rec.weighting,
            strategy_instance_id=rec.strategy_instance_id,
            final_signal=rec.final_signal,
            benchmark=rec.benchmark,
        )
        new_pnl, updated_rec = _advance_anchor(
            state_map,
            underlying,
            stn,
            candle.open,
            rec.position,
            rec.bar_ts,
            rec.revision_ts,
            meta,
        )
        row = build_carry_forward_row(
            stn,
            updated_rec,
            candle.open,
            new_pnl,
            source_label,
            candle.ts,
            now,
        )
        output.append({"_sink": sink_key, "_row": row})

    return output


def process_candle(
    candle: CandleEvent,
    state_prod: StateMap,
    state_rt: StateMap,
    cfg: SinkConfig,
    state_bt: StateMap | None = None,
) -> tuple[list[dict[str, Any]], int, int, int]:
    """Process one closed candle and return all rows to be sunk.

    Returns (rows, prod_fetched, bt_fetched, real_trade_fetched) where the
    fetched counts are the number of strategy instances returned by each
    candle lookup query — used by the caller to validate flush completeness.

    BT chains minute-to-minute via state_bt (optional trailing arg; a fresh map
    is used if omitted): position comes from the active strategy_cum_pnl_bt_v2
    bar's pos_first and cpnl chains from the prior minute.
    """
    if state_bt is None:
        state_bt = {}
    if not (cfg.prod or cfg.bt or cfg.real_trade):
        return [], 0, 0, 0

    output: list[dict[str, Any]] = []
    now = datetime.now(UTC).replace(tzinfo=None)
    prod_fetched = bt_fetched = rt_fetched = 0

    # underlying used for StateMap lookup only — NOT passed to fetch_* functions.
    underlying = candle.instrument.removesuffix("USDT")

    # --- Prod sink ---
    if cfg.prod:
        bars = fetch_strategies_for_candle(candle.instrument, candle.ts)
        prod_fetched = len(bars)
        bars_by_stn: dict[str, StrategyBar] = {b.strategy_table_name: b for b in bars}
        output.extend(
            _process_mode(
                candle,
                underlying,
                state_prod,
                bars_by_stn,
                sink_key="pnl_prod",
                source_label="production",
                now=now,
                is_real_trade=False,
            )
        )

    # --- Backtest sink (minute-chaining via state_bt) ---
    if cfg.bt:
        bt_anchors = fetch_bt_anchors_for_candle(candle.instrument, candle.ts)
        bt_fetched = len(bt_anchors)
        anchors_by_stn: dict[str, BtLiveAnchor] = {
            a.strategy_table_name: a for a in bt_anchors
        }
        output.extend(
            _process_bt(candle, underlying, state_bt, anchors_by_stn, now)
        )

    # --- Real-trade sink ---
    if cfg.real_trade:
        revisions = fetch_real_trade_for_candle(candle.instrument, candle.ts)
        rt_fetched = len(revisions)
        revs_by_stn: dict[str, StrategyRevision] = {
            r.strategy_table_name: r for r in revisions
        }
        output.extend(
            _process_mode(
                candle,
                underlying,
                state_rt,
                revs_by_stn,
                sink_key="pnl_real_trade",
                source_label="real_trade",
                now=now,
                is_real_trade=True,
            )
        )

    return output, prod_fetched, bt_fetched, rt_fetched
