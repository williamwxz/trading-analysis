"""Fire-and-forget CloudWatch metrics for the Flink PnL job.

All put_metric calls swallow exceptions — a metrics failure must never
crash the job. The namespace and metric names match pnl_consumer so both
appear on the same Grafana dashboard.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_NAMESPACE = "trading-analysis"
_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")

_client = None


def _cw():
    global _client
    if _client is None:
        import boto3
        _client = boto3.client("cloudwatch", region_name=_REGION)
    return _client


def _put(metric_data: list[dict]) -> None:
    try:
        _cw().put_metric_data(Namespace=_NAMESPACE, MetricData=metric_data)
    except Exception:
        logger.debug("CloudWatch put_metric_data failed", exc_info=True)


def bootstrap_complete(mode: str, strategy_count: int) -> None:
    """Emitted once per mode when bootstrap finishes."""
    _put([
        {
            "MetricName": "FlinkBootstrapComplete",
            "Dimensions": [{"Name": "Mode", "Value": mode}],
            "Value": 1,
            "Unit": "Count",
        },
        {
            "MetricName": "FlinkBootstrapStrategyCount",
            "Dimensions": [{"Name": "Mode", "Value": mode}],
            "Value": float(strategy_count),
            "Unit": "Count",
        },
    ])


def emit_candle_lag(candle_ts: datetime, sink: str) -> None:
    """Emitted once per candle. Matches pnl_consumer.emit_candle_lag metric shape."""
    try:
        lag = (datetime.now(timezone.utc).replace(tzinfo=None) - candle_ts).total_seconds()
        dims = [{"Name": "Sink", "Value": sink}]
        _put([
            {
                "MetricName": "CandleLagSeconds",
                "Value": lag,
                "Unit": "Seconds",
                "Dimensions": dims,
            },
            {
                "MetricName": "CandleProcessingTs",
                "Value": candle_ts.timestamp(),
                "Unit": "None",
                "Dimensions": dims,
            },
        ])
    except Exception:
        logger.warning("Failed to emit candle metrics", exc_info=True)


def rows_flushed(sink: str, count: int) -> None:
    """Emitted on each ClickHouse flush per sink."""
    _put([
        {
            "MetricName": "FlinkRowsFlushed",
            "Dimensions": [{"Name": "Sink", "Value": sink}],
            "Value": float(count),
            "Unit": "Count",
        }
    ])
