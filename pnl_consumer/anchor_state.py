from dataclasses import dataclass


@dataclass
class AnchorRecord:
    anchor_pnl: float = 0.0
    anchor_price: float = 0.0
    anchor_position: float = 0.0


class AnchorState:
    """Dict-backed anchor store."""

    def __init__(self) -> None:
        self._store: dict[str, AnchorRecord] = {}

    def get(self, strategy_table_name: str) -> AnchorRecord:
        return self._store.get(strategy_table_name, AnchorRecord())

    def update(self, strategy_table_name: str, record: AnchorRecord) -> None:
        self._store[strategy_table_name] = record

    def __len__(self) -> int:
        return len(self._store)

    def compute_pnl(
        self,
        strategy_table_name: str,
        close_price: float,
        position: float,
    ) -> float:
        """Apply anchor-chain formula and update state. Returns new cumulative_pnl."""
        rec = self.get(strategy_table_name)
        if rec.anchor_price == 0.0:
            new_pnl = rec.anchor_pnl
        else:
            new_pnl = (
                rec.anchor_pnl
                + position * (close_price - rec.anchor_price) / rec.anchor_price
            )
        self.update(strategy_table_name, AnchorRecord(
            anchor_pnl=new_pnl,
            anchor_price=close_price,
            anchor_position=position,
        ))
        return new_pnl
