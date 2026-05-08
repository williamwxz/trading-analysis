from dataclasses import dataclass
from datetime import datetime


@dataclass
class AnchorRecord:
    pnl: float = 0.0
    price: float = 0.0
    position: float = 0.0


class AnchorState:
    """Per-strategy running state: last-minute (pnl, price, position)."""

    def __init__(self) -> None:
        self._store: dict[str, AnchorRecord] = {}

    def get(self, strategy_table_name: str) -> AnchorRecord:
        return self._store.get(strategy_table_name, AnchorRecord())

    def set(self, strategy_table_name: str, record: AnchorRecord) -> None:
        self._store[strategy_table_name] = record

    def __len__(self) -> int:
        return len(self._store)

    def compute_pnl(
        self,
        strategy_table_name: str,
        current_price: float,
        position: float,
    ) -> float:
        """Advance the chain by one minute. Returns new cumulative_pnl.

        Formula: pnl = prev_pnl + position * (current_price - prev_price) / prev_price
        """
        if strategy_table_name not in self._store:
            raise RuntimeError(
                f"No state found for strategy '{strategy_table_name}'. "
                "Bootstrap from ClickHouse returned no data."
            )
        rec = self._store[strategy_table_name]
        new_pnl = rec.pnl + position * (current_price - rec.price) / rec.price
        self._store[strategy_table_name] = AnchorRecord(
            pnl=new_pnl,
            price=current_price,
            position=position,
        )
        return new_pnl
