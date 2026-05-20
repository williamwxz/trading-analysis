"""Pure per-candle PnL computation for the Flink streaming path.

process_candle() is intentionally free of I/O side effects beyond the three
fetch_* calls — all state mutation happens on the in-memory StateMap dicts
passed in by the caller.

Output format: each element is a dict with a ``_sink`` key that routes the
row to the appropriate ClickHouse table or price topic:

    {"_sink": "price", "instrument": ..., "open": ..., "ts": ..., ...}
    {"_sink": "pnl_prod",       "_row": <INSERT_COLUMNS list>}
    {"_sink": "pnl_bt",         "_row": <INSERT_COLUMNS list>}
    {"_sink": "pnl_real_trade", "_row": <INSERT_COLUMNS list>}
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from libs.computation.anchor_state import AnchorRecord
from libs.computation.candle_lookup import (
    StrategyBar,
    StrategyRevision,
    fetch_bt_strategies_for_candle,
    fetch_real_trade_for_candle,
    fetch_strategies_for_candle,
)
from libs.computation.pnl_formula import (
    build_carry_forward_row,
    build_pnl_row,
)
from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap
from streaming.models import CandleEvent


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

    rec = state_map[underlying].get(stn, AnchorRecord(pnl=0.0, price=candle_open, position=0.0))

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
            from libs.computation.anchor_state import AnchorState as _AS

            _tmp = _AS()
            _tmp.set(stn, existing_rec)
            should_apply = _tmp.should_apply_revision(stn, rev.bar_ts, rev.revision_ts)

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
                    state_map, underlying, stn,
                    candle.open, rev.position,
                    rev.bar_ts, rev.revision_ts,
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
                    state_map, underlying, stn,
                    candle.open, rec.position,
                    rec.bar_ts, rec.revision_ts,
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
                state_map, underlying, stn,
                candle.open, bar.position,
                _epoch, _epoch,
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
            state_map, underlying, stn,
            candle.open, rec.position,
            rec.bar_ts, rec.revision_ts,
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
    state_bt: StateMap,
    state_rt: StateMap,
    cfg: SinkConfig,
) -> list[dict[str, Any]]:
    """Process one closed candle and return all rows to be sunk.

    Parameters
    ----------
    candle:
        The closed 1-minute candle from Binance.
    state_prod, state_bt, state_rt:
        Per-mode anchor maps (underlying → stn → AnchorRecord). Mutated in-place.
    cfg:
        Which sinks are enabled.

    Returns
    -------
    List of sink-tagged dicts. Each dict has ``_sink`` (str) and either direct
    candle fields (for price sink) or ``_row`` (INSERT_COLUMNS list).
    """
    if not (cfg.price or cfg.prod or cfg.bt or cfg.real_trade):
        return []

    output: list[dict[str, Any]] = []
    now = datetime.utcnow()

    # underlying used for StateMap lookup only — NOT passed to fetch_* functions.
    underlying = candle.instrument.removesuffix("USDT")

    # --- Price sink ---
    if cfg.price:
        output.append({
            "_sink": "price",
            "instrument": candle.instrument,
            "ts": candle.ts,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
        })

    # --- Prod sink ---
    if cfg.prod:
        bars = fetch_strategies_for_candle(candle.instrument, candle.ts)
        bars_by_stn: dict[str, StrategyBar] = {b.strategy_table_name: b for b in bars}
        output.extend(_process_mode(
            candle, underlying, state_prod, bars_by_stn,
            sink_key="pnl_prod",
            source_label="production",
            now=now,
            is_real_trade=False,
        ))

    # --- Backtest sink ---
    if cfg.bt:
        bt_bars = fetch_bt_strategies_for_candle(candle.instrument, candle.ts)
        bt_by_stn: dict[str, StrategyBar] = {b.strategy_table_name: b for b in bt_bars}
        output.extend(_process_mode(
            candle, underlying, state_bt, bt_by_stn,
            sink_key="pnl_bt",
            source_label="backtest",
            now=now,
            is_real_trade=False,
        ))

    # --- Real-trade sink ---
    if cfg.real_trade:
        revisions = fetch_real_trade_for_candle(candle.instrument, candle.ts)
        revs_by_stn: dict[str, StrategyRevision] = {r.strategy_table_name: r for r in revisions}
        output.extend(_process_mode(
            candle, underlying, state_rt, revs_by_stn,
            sink_key="pnl_real_trade",
            source_label="real_trade",
            now=now,
            is_real_trade=True,
        ))

    return output
