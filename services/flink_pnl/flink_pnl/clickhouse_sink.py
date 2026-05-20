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

_SIID_COL = INSERT_COLUMNS.index("strategy_instance_id")


class ClickHouseSinkFunction:
    """Buffer rows from process_candle() and flush them to ClickHouse in bulk."""

    def __init__(self, cfg: SinkConfig) -> None:
        self._cfg = cfg
        self._prod_buf: list[list] = []
        self._bt_buf: list[list] = []
        self._rt_buf: list[list] = []

    def invoke(self, row: dict) -> None:
        """Buffer one output row from process_candle()."""
        sink = row.get("_sink")
        if sink == "pnl_prod":
            self._prod_buf.append(row["_row"])
        elif sink == "pnl_bt":
            self._bt_buf.append(row["_row"])
        elif sink == "pnl_real_trade":
            self._rt_buf.append(row["_row"])
        elif sink == "price":
            pass  # flink_pnl does not write the price sink
        else:
            logger.warning("Unknown _sink value %r — row ignored", sink)

    def flush(
        self,
        expected_prod: int = 0,
        expected_bt: int = 0,
        expected_real_trade: int = 0,
    ) -> None:
        """Flush all buffered rows to ClickHouse (or stdout in DRY_RUN mode).

        Raises RuntimeError if any sink has fewer distinct strategy_instance_ids
        than the number fetched from the history table — mirrors the completeness
        check in pnl_consumer._flush_candle().
        """
        has_data = self._prod_buf or self._bt_buf or self._rt_buf
        if not has_data:
            return

        self._check_completeness(self._prod_buf, expected_prod, "prod")
        self._check_completeness(self._bt_buf, expected_bt, "bt")
        self._check_completeness(self._rt_buf, expected_real_trade, "real_trade")

        if _DRY_RUN:
            self._dry_run_flush()
            return

        client = ch.get_client()
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

    def _check_completeness(
        self, buf: list[list], expected: int, label: str
    ) -> None:
        if not buf or not expected:
            return
        actual = len({row[_SIID_COL] for row in buf})
        if actual < expected:
            raise RuntimeError(
                f"Flush completeness check failed [{label}]: "
                f"{actual} distinct strategy_instance_ids < "
                f"{expected} fetched from history table. "
                "Refusing to sink partial data."
            )

    def _dry_run_flush(self) -> None:
        """Print one summary line per sink per candle, with up to 3 sample rows."""
        total = 0
        for sink_key, buf in [
            ("pnl_prod", self._prod_buf),
            ("pnl_bt", self._bt_buf),
            ("pnl_real_trade", self._rt_buf),
        ]:
            if not buf:
                continue
            n = len(buf)
            total += n
            samples = []
            for row in buf[:3]:
                c = dict(zip(INSERT_COLUMNS, row))
                samples.append(
                    f"underlying={c.get('underlying')} name={c.get('strategy_name')} "
                    f"ts={c.get('ts')} pnl={c.get('cumulative_pnl'):.6f} "
                    f"pos={c.get('position')} price={c.get('price')}"
                )
            suffix = f" (+{n - 3} more)" if n > 3 else ""
            print(
                f"[DRY_RUN {sink_key}] {n} rows | " + " | ".join(samples) + suffix,
                flush=True,
            )
        _metric_rows_flushed("dry_run", total)
        self._prod_buf.clear()
        self._bt_buf.clear()
        self._rt_buf.clear()

    def _flush_pnl(self, sink_key: str, table: str, buf: list[list], client) -> None:
        if not buf:
            return
        n = len(buf)
        ch.insert_rows(table, INSERT_COLUMNS, buf, client=client)
        buf.clear()
        _metric_rows_flushed(sink_key, n)
        logger.debug("Flushed %d rows to %s", n, table)
