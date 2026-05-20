"""Fire-and-forget CloudWatch metrics for the Flink PnL job.

All put_metric calls swallow exceptions — a metrics failure must never
crash the job. The namespace matches the existing streaming-infra dashboard.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Sequence

logger = logging.getLogger(__name__)

_NAMESPACE = "trading-analysis"
_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-1")

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


def candle_processed(candle_ts: datetime, rows_emitted: int) -> None:
    """Emitted once per candle with the candle's own timestamp as a metric value."""
    now = datetime.now(timezone.utc)
    lag_s = (now - candle_ts.replace(tzinfo=timezone.utc)).total_seconds()
    candle_epoch = float(candle_ts.timestamp())
    _put([
        {
            "MetricName": "FlinkCandleTimestamp",
            "Dimensions": [],
            "Value": candle_epoch,
            "Unit": "None",
        },
        {
            "MetricName": "FlinkCandleLagSeconds",
            "Dimensions": [],
            "Value": max(0.0, lag_s),
            "Unit": "Seconds",
        },
        {
            "MetricName": "FlinkRowsEmittedPerCandle",
            "Dimensions": [],
            "Value": float(rows_emitted),
            "Unit": "Count",
        },
    ])


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
