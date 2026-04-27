from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass
class CandleEvent:
    exchange: str
    instrument: str
    ts: datetime  # candle open time (minute boundary), naive UTC
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_json(self) -> str:
        import json

        return json.dumps(
            {
                "exchange": self.exchange,
                "instrument": self.instrument,
                "ts": self.ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
                "volume": self.volume,
            }
        )

    @classmethod
    def from_binance_kline(cls, stream_data: dict) -> "CandleEvent":
        """Parse a Binance combined stream kline message.

        stream_data is the full WS message dict, e.g.:
        {"stream": "btcusdt@kline_1m", "data": {"e": "kline", "k": {...}}}
        """
        k = stream_data["data"]["k"]
        # Convert to naive UTC datetime (strip timezone info)
        ts_utc = datetime.fromtimestamp(k["t"] / 1000, tz=UTC)
        return cls(
            exchange="binance",
            instrument=k["s"],  # e.g. "BTCUSDT"
            ts=ts_utc.replace(tzinfo=None),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
        )
