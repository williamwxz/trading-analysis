"""Tests for PnlProcessFunction.

PyFlink is not installed in the dev venv (it ships inside the Docker image only).
We patch the entire pyflink module tree via sys.modules before importing pnl_job
so that the class definition and inheritance succeed without the real package.
"""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Patch pyflink before any import of flink_pnl.pnl_job
# ---------------------------------------------------------------------------
for _mod in [
    "pyflink",
    "pyflink.datastream",
    "pyflink.datastream.state",
]:
    sys.modules.setdefault(_mod, MagicMock())

# Make ProcessFunction a real base class so PnlProcessFunction can inherit.
_pyflink_ds = sys.modules["pyflink.datastream"]
_pyflink_ds.ProcessFunction = type("ProcessFunction", (), {"Context": MagicMock()})
_pyflink_ds.RuntimeContext = MagicMock

from flink_pnl.pnl_job import PnlProcessFunction  # noqa: E402  (must be after patching)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fn(process_candle_return=None) -> PnlProcessFunction:
    """Return a PnlProcessFunction with internals wired up directly (no open())."""
    fn = PnlProcessFunction.__new__(PnlProcessFunction)
    fn._cfg = MagicMock()
    fn._state_prod = {}
    fn._state_bt = {}
    fn._state_rt = {}
    fn._sink = MagicMock()
    fn._process_candle_return = process_candle_return or []
    return fn


_SAMPLE_CANDLE_DICT = {
    "exchange": "binance",
    "instrument": "BTCUSDT",
    "ts": "2026-05-15T10:00:00",
    "open": 93000.0,
    "high": 93100.0,
    "low": 92900.0,
    "close": 93050.0,
    "volume": 12.5,
}
_SAMPLE_JSON = json.dumps(_SAMPLE_CANDLE_DICT)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_process_element_calls_process_candle():
    """process_element() deserializes the JSON and forwards to process_candle()."""
    fn = _make_fn()
    mock_candle = MagicMock()
    mock_ctx = MagicMock()

    with (
        patch("flink_pnl.pnl_job.CandleEvent") as mock_ce,
        patch("flink_pnl.pnl_job.process_candle", return_value=[]) as mock_pc,
    ):
        mock_ce.from_dict.return_value = mock_candle

        fn.process_element(_SAMPLE_JSON, mock_ctx)

        mock_pc.assert_called_once_with(
            mock_candle,
            fn._state_prod,
            fn._state_bt,
            fn._state_rt,
            fn._cfg,
        )


@pytest.mark.unit
def test_process_element_invokes_sink_for_each_row():
    """process_element() calls sink.invoke() once per row from process_candle()."""
    fn = _make_fn()
    rows = [{"_sink": "pnl_prod", "_row": []}, {"_sink": "price"}, {"_sink": "pnl_bt"}]
    mock_ctx = MagicMock()

    with (
        patch("flink_pnl.pnl_job.CandleEvent") as mock_ce,
        patch("flink_pnl.pnl_job.process_candle", return_value=rows),
    ):
        mock_ce.from_dict.return_value = MagicMock()

        fn.process_element(_SAMPLE_JSON, mock_ctx)

        assert fn._sink.invoke.call_count == 3


@pytest.mark.unit
def test_snapshot_state_flushes_sink():
    """snapshot_state() must call sink.flush() exactly once."""
    fn = _make_fn()
    mock_ctx = MagicMock()

    fn.snapshot_state(mock_ctx)

    fn._sink.flush.assert_called_once()


@pytest.mark.unit
def test_process_element_parses_json():
    """process_element() calls CandleEvent.from_dict with the parsed dict."""
    fn = _make_fn()
    mock_ctx = MagicMock()

    with (
        patch("flink_pnl.pnl_job.CandleEvent") as mock_ce,
        patch("flink_pnl.pnl_job.process_candle", return_value=[]),
    ):
        mock_ce.from_dict.return_value = MagicMock()

        fn.process_element(_SAMPLE_JSON, mock_ctx)

        mock_ce.from_dict.assert_called_once_with(_SAMPLE_CANDLE_DICT)
