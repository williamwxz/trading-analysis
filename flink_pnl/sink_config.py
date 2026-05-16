from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class SinkConfig:
    price: bool
    prod: bool
    bt: bool
    real_trade: bool

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> SinkConfig:
        if env is None:
            env = os.environ  # type: ignore[assignment]

        def _flag(key: str) -> bool:
            return env.get(key, "false").lower() == "true"

        return cls(
            price=_flag("ENABLE_PRICE_SINK"),
            prod=_flag("ENABLE_PROD_SINK"),
            bt=_flag("ENABLE_BT_SINK"),
            real_trade=_flag("ENABLE_REAL_TRADE_SINK"),
        )
