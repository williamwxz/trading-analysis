"""Flink ProcessFunction: bootstraps anchor state then processes candles."""

from __future__ import annotations

import json
import logging
import os

from pyflink.datastream import ProcessFunction, RuntimeContext

from flink_pnl.clickhouse_sink import ClickHouseSinkFunction
from flink_pnl.metrics import bootstrap_complete, emit_candle_lag
from flink_pnl.process_candle import process_candle
from flink_pnl.sink_config import SinkConfig
from flink_pnl.state import StateMap, build_state_from_bootstrap
from pnl_consumer.pnl_consumer import _bootstrap_state, peek_reference_ts
from streaming.models import CandleEvent

logger = logging.getLogger(__name__)


class PnlProcessFunction(ProcessFunction):
    """Flink ProcessFunction that runs bootstrap on open and computes PnL per candle."""

    def open(self, ctx: RuntimeContext) -> None:
        cfg = SinkConfig.from_env()
        self._cfg = cfg

        brokers = os.environ["REDPANDA_BROKERS"]
        group_id = os.environ.get("KAFKA_GROUP_ID", "flink-pnl-consumer-v2")
        self._sink_label = group_id.removeprefix("flink-pnl-consumer-") or group_id

        reference_ts = peek_reference_ts(brokers, group_id)

        if cfg.prod:
            anchor_prod = _bootstrap_state("prod", reference_ts)
            self._state_prod: StateMap = build_state_from_bootstrap(anchor_prod)
            bootstrap_complete("prod", sum(len(v) for v in self._state_prod.values()))
        else:
            self._state_prod = {}

        if cfg.bt:
            anchor_bt = _bootstrap_state("bt", reference_ts)
            self._state_bt: StateMap = build_state_from_bootstrap(anchor_bt)
            bootstrap_complete("bt", sum(len(v) for v in self._state_bt.values()))
        else:
            self._state_bt = {}

        if cfg.real_trade:
            anchor_rt = _bootstrap_state("real_trade", reference_ts)
            self._state_rt: StateMap = build_state_from_bootstrap(anchor_rt)
            bootstrap_complete("real_trade", sum(len(v) for v in self._state_rt.values()))
        else:
            self._state_rt = {}

        self._sink = ClickHouseSinkFunction(cfg)

    def process_element(self, value: str, ctx: ProcessFunction.Context) -> None:
        candle = CandleEvent.from_dict(json.loads(value))
        rows, prod_fetched, bt_fetched, rt_fetched = process_candle(
            candle, self._state_prod, self._state_bt, self._state_rt, self._cfg
        )
        for row in rows:
            self._sink.invoke(row)
        self._sink.flush(
            expected_prod=prod_fetched,
            expected_bt=bt_fetched,
            expected_real_trade=rt_fetched,
        )
        emit_candle_lag(candle.ts, self._sink_label)

    def snapshot_state(self, context) -> None:
        self._sink.flush()
        logger.info("snapshot_state: flushed buffered rows to ClickHouse")
