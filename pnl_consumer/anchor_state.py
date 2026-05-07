from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class AnchorRecord:
    anchor_pnl: float = 0.0
    anchor_price: float = 0.0
    anchor_position: float = 0.0
    # ts of the ClickHouse row this anchor was seeded from; None for live-computed anchors.
    anchor_ts: datetime | None = None


class AnchorState:
    """Dict-backed anchor store."""

    def __init__(self) -> None:
        self._store: dict[str, AnchorRecord] = {}

    def get(self, strategy_table_name: str) -> AnchorRecord:
        return self._store.get(strategy_table_name, AnchorRecord())

    def update(self, strategy_table_name: str, record: AnchorRecord) -> None:
        self._store[strategy_table_name] = record

    def update_if_newer(self, strategy_table_name: str, record: AnchorRecord) -> bool:
        """Overwrite only if record.anchor_ts is strictly newer than the stored anchor_ts.

        Returns True if the anchor was updated, False if skipped.
        Live-computed anchors (anchor_ts=None) are never overwritten by a reseed.
        """
        existing = self._store.get(strategy_table_name)
        if existing is None:
            # No in-memory anchor yet — always seed.
            self._store[strategy_table_name] = record
            return True
        if existing.anchor_ts is None:
            # Live-computed anchor is authoritative; reseed must not overwrite it.
            return False
        if record.anchor_ts is not None and record.anchor_ts > existing.anchor_ts:
            self._store[strategy_table_name] = record
            return True
        return False

    def __len__(self) -> int:
        return len(self._store)

    def compute_pnl(
        self,
        strategy_table_name: str,
        close_price: float,
        position: float,
    ) -> float:
        """Apply anchor-chain formula and update state. Returns new cumulative_pnl.

        Clears anchor_ts after the first live computation so subsequent reseeds
        will not overwrite the in-memory chain.
        """
        rec = self.get(strategy_table_name)
        if strategy_table_name not in self._store:
            raise RuntimeError(
                f"No anchor found for strategy '{strategy_table_name}'. "
                "Bootstrap from ClickHouse returned no data — cannot compute PnL from zero."
            )
        if rec.anchor_price == 0.0:
            raise RuntimeError(
                f"Anchor price is zero for strategy '{strategy_table_name}'. "
                "Stored anchor is corrupt — cannot compute PnL."
            )
        new_pnl = (
            rec.anchor_pnl
            + position * (close_price - rec.anchor_price) / rec.anchor_price
        )
        # anchor_ts=None marks this as a live-computed anchor — reseed will not overwrite.
        self.update(
            strategy_table_name,
            AnchorRecord(
                anchor_pnl=new_pnl,
                anchor_price=close_price,
                anchor_position=position,
                anchor_ts=None,
            ),
        )
        return new_pnl
