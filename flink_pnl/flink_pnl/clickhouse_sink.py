"""ClickHouse sink helper for the Flink PnL job.

Not a Flink SinkFunction subclass — this is a plain Python helper called directly
from PnlProcessFunction. It buffers rows in memory and flushes to ClickHouse when
triggered (e.g. on checkpoint).

Set DRY_RUN=true to skip ClickHouse writes and print rows to stdout instead.
Useful for verifying output correctness without touching production tables.
"""

from __future__ import annotations

import logging
import os

import libs.clickhouse_client as ch
from flink_pnl.metrics import rows_flushed as _metric_rows_flushed
from flink_pnl.sink_config import SinkConfig
from libs.computation.pnl_formula import INSERT_COLUMNS

logger = logging.getLogger("flink_pnl.clickhouse_sink")

_DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

_PRICE_COLUMNS = ["instrument", "ts", "open", "high", "low", "close", "volume_base"]

_PNL_SINK_TABLE: dict[str, str] = {
    "pnl_prod": "analytics.strategy_pnl_1min_prod_v2",
    "pnl_bt": "analytics.strategy_pnl_1min_bt_v2",
    "pnl_real_trade": "analytics.strategy_pnl_1min_real_trade_v2",
}


class ClickHouseSinkFunction:
    """Buffer rows from process_candle() and flush them to ClickHouse in bulk."""

    def __init__(self, cfg: SinkConfig) -> None:
        self._cfg = cfg
        self._price_buf: list[dict] = []
        self._prod_buf: list[dict] = []
        self._bt_buf: list[dict] = []
        self._rt_buf: list[dict] = []

    def invoke(self, row: dict) -> None:
        """Buffer one output row from process_candle()."""
        sink = row.get("_sink")
        if sink == "price":
            self._price_buf.append(row)
        elif sink == "pnl_prod":
            self._prod_buf.append(row)
        elif sink == "pnl_bt":
            self._bt_buf.append(row)
        elif sink == "pnl_real_trade":
            self._rt_buf.append(row)
        else:
            logger.warning("Unknown _sink value %r — row ignored", sink)

    def flush(self) -> None:
        """Flush all buffered rows to ClickHouse (or stdout in DRY_RUN mode)."""
        has_data = self._price_buf or self._prod_buf or self._bt_buf or self._rt_buf
        if not has_data:
            return

        if _DRY_RUN:
            self._dry_run_flush()
            return

        client = ch.get_client()
        self._flush_price(client)
        self._flush_pnl(
            "pnl_prod",
            "analytics.strategy_pnl_1min_prod_v2",
            self._prod_buf,
            client,
        )
        self._flush_pnl(
            "pnl_bt", "analytics.strategy_pnl_1min_bt_v2", self._bt_buf, client
        )
        self._flush_pnl(
            "pnl_real_trade",
            "analytics.strategy_pnl_1min_real_trade_v2",
            self._rt_buf,
            client,
        )

    def _dry_run_flush(self) -> None:
        """Print one summary line per sink per candle, with up to 3 sample rows."""
        total = 0
        for sink_key, buf in [
            ("price", self._price_buf),
            ("pnl_prod", self._prod_buf),
            ("pnl_bt", self._bt_buf),
            ("pnl_real_trade", self._rt_buf),
        ]:
            if not buf:
                continue
            n = len(buf)
            total += n
            samples = []
            for r in buf[:3]:
                if sink_key == "price":
                    samples.append(
                        f"inst={r['instrument']} ts={r['ts']} open={r['open']}"
                    )
                else:
                    c = dict(zip(INSERT_COLUMNS, r["_row"]))
                    samples.append(
                        f"underlying={c.get('underlying')} name={c.get('strategy_name')} "
                        f"ts={c.get('ts')} pnl={c.get('cumulative_pnl'):.6f} "
                        f"pos={c.get('position')} price={c.get('price')}"
                    )
            suffix = f" (+{n - 3} more)" if n > 3 else ""
            print(
                f"[DRY_RUN {sink_key}] {n} rows | "
                + " | ".join(samples)
                + suffix,
                flush=True,
            )
        _metric_rows_flushed("dry_run", total)
        self._price_buf.clear()
        self._prod_buf.clear()
        self._bt_buf.clear()
        self._rt_buf.clear()

    def _flush_price(self, client) -> None:
        if not self._price_buf:
            return
        rows = [
            [
                r["instrument"],
                r["ts"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
            ]
            for r in self._price_buf
        ]
        ch.insert_rows(
            "analytics.futures_price_1min", _PRICE_COLUMNS, rows, client=client
        )
        n = len(rows)
        self._price_buf.clear()
        _metric_rows_flushed("price", n)
        logger.debug("Flushed %d price rows", n)

    def _flush_pnl(self, sink_key: str, table: str, buf: list[dict], client) -> None:
        if not buf:
            return
        rows = [r["_row"] for r in buf]
        n = len(rows)
        ch.insert_rows(table, INSERT_COLUMNS, rows, client=client)
        buf.clear()
        _metric_rows_flushed(sink_key, n)
        logger.debug("Flushed %d rows to %s", n, table)
