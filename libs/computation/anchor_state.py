"""Per-strategy running state for the PnL streaming consumer.

AnchorRecord stores (pnl, price, position) plus the revision guard fields
(bar_ts, revision_ts) used by the real_trade mode.

AnchorState wraps a dict of AnchorRecord keyed by strategy_table_name and
provides:
  - compute_pnl(): advance the chain by one minute
  - should_apply_revision(): real_trade revision guard — tuple comparison
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import KeysView

_DATETIME_MIN = datetime.min


@dataclass
class AnchorRecord:
    pnl: float = 0.0
    price: float = 0.0
    position: float = 0.0
    # Real-trade revision guard fields. Unused (datetime.min) for prod/bt so any
    # revision passes the guard on first encounter.
    bar_ts: datetime = field(default_factory=lambda: _DATETIME_MIN)
    revision_ts: datetime = field(default_factory=lambda: _DATETIME_MIN)
    # Bar metadata — populated when a bar is seen, used for carry-forward rows.
    # None until first bar is processed for this strategy.
    strategy_id: int = 0
    strategy_name: str = ""
    underlying: str = ""
    config_timeframe: str = ""
    weighting: float = 0.0
    strategy_instance_id: str = ""
    final_signal: float = 0.0
    benchmark: float = 0.0


class AnchorState:
    """Per-strategy running state: last-minute (pnl, price, position, bar_ts, revision_ts)."""

    def __init__(self) -> None:
        self._store: dict[str, AnchorRecord] = {}

    def get(self, strategy_table_name: str) -> AnchorRecord:
        return self._store.get(strategy_table_name, AnchorRecord())

    def set(self, strategy_table_name: str, record: AnchorRecord) -> None:
        self._store[strategy_table_name] = record

    def has(self, strategy_table_name: str) -> bool:
        return strategy_table_name in self._store

    def __len__(self) -> int:
        return len(self._store)

    def keys(self) -> "KeysView[str]":
        return self._store.keys()

    def compute_pnl(
        self,
        strategy_table_name: str,
        current_price: float,
        position: float,
        bar_ts: datetime = _DATETIME_MIN,
        revision_ts: datetime = _DATETIME_MIN,
        meta: "AnchorRecord | None" = None,
    ) -> float:
        """Advance the chain by one minute and return new cumulative_pnl.

        Formula: pnl = prev_pnl + position * (current_price - prev_price) / prev_price

        Raises RuntimeError if the strategy has no existing anchor (call set() first).
        bar_ts and revision_ts are stored on the new record for real_trade guard checks.
        meta: if provided, copies bar metadata fields (strategy_id, name, etc.) onto
        the new record so carry-forward rows can be emitted without re-querying.
        """
        if strategy_table_name not in self._store:
            raise RuntimeError(
                f"No anchor state for '{strategy_table_name}'. "
                "Seed via set() before calling compute_pnl()."
            )
        rec = self._store[strategy_table_name]
        if rec.price == 0.0:
            # No prior price — cannot compute return; hold pnl, advance price.
            new_pnl = rec.pnl
        else:
            new_pnl = rec.pnl + position * (current_price - rec.price) / rec.price
        self._store[strategy_table_name] = AnchorRecord(
            pnl=new_pnl,
            price=current_price,
            position=position,
            bar_ts=bar_ts,
            revision_ts=revision_ts,
            strategy_id=meta.strategy_id if meta else rec.strategy_id,
            strategy_name=meta.strategy_name if meta else rec.strategy_name,
            underlying=meta.underlying if meta else rec.underlying,
            config_timeframe=meta.config_timeframe if meta else rec.config_timeframe,
            weighting=meta.weighting if meta else rec.weighting,
            strategy_instance_id=meta.strategy_instance_id if meta else rec.strategy_instance_id,
            final_signal=meta.final_signal if meta else rec.final_signal,
            benchmark=meta.benchmark if meta else rec.benchmark,
        )
        return new_pnl

    def should_apply_revision(
        self,
        strategy_table_name: str,
        new_bar_ts: datetime,
        new_revision_ts: datetime,
    ) -> bool:
        """Return True iff the revision is newer than the anchor's (bar_ts, revision_ts).

        Guard rule: apply iff (new_bar_ts, new_revision_ts) > (anchor.bar_ts, anchor.revision_ts).

        - new_bar_ts < anchor.bar_ts: stale revision for an old bar — ignore.
        - new_bar_ts == anchor.bar_ts and new_revision_ts <= anchor.revision_ts: already seen — ignore.
        - new_bar_ts > anchor.bar_ts OR same bar with newer revision_ts: apply.
        - No existing anchor (datetime.min defaults): first revision always applies.
        """
        rec = self._store.get(strategy_table_name, AnchorRecord())
        return (new_bar_ts, new_revision_ts) > (rec.bar_ts, rec.revision_ts)
