"""Unit tests for binance_futures_backfill_asset skip logic."""
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone, timedelta

from dagster import build_asset_context, DailyPartitionsDefinition


def _make_context(partition_key: str):
    return build_asset_context(partition_key=partition_key)


@pytest.mark.unit
def test_skips_instrument_when_full_day_present():
    """If query_scalar returns >= 1440 for an instrument, it is skipped (no DELETE, no fetch)."""
    from trading_dagster.assets.binance_futures_ohlcv import binance_futures_backfill_asset

    ctx = _make_context("2024-03-01")

    with patch("trading_dagster.assets.binance_futures_ohlcv.get_client") as mock_get_client, \
         patch("trading_dagster.assets.binance_futures_ohlcv.query_scalar", return_value=1440) as mock_qs, \
         patch("trading_dagster.assets.binance_futures_ohlcv.execute_query") as mock_exec, \
         patch("trading_dagster.assets.binance_futures_ohlcv.insert_rows") as mock_insert, \
         patch("trading_dagster.assets.binance_futures_ohlcv.ExchangePriceDataService") as mock_svc:

        result = binance_futures_backfill_asset(ctx)
        assert result.metadata["total_rows"] == 0

    # query_scalar called once per instrument (10 instruments)
    assert mock_qs.call_count == 10
    # No DELETE and no Binance fetch when all instruments are full
    mock_exec.assert_not_called()
    mock_svc.fetch_ohlcv_times_series_df.assert_not_called()
    mock_insert.assert_not_called()


@pytest.mark.unit
def test_refetches_instrument_when_partial_day():
    """If query_scalar returns < 1440, the instrument is deleted and re-fetched."""
    import pandas as pd
    from trading_dagster.assets.binance_futures_ohlcv import binance_futures_backfill_asset, INSTRUMENTS, TARGET_TABLE

    ctx = _make_context("2024-03-01")

    # Only first instrument is partial; rest are full
    def scalar_side_effect(query, client):
        if INSTRUMENTS[0] in query:
            return 500  # partial
        return 1440  # full

    fake_df = pd.DataFrame({
        "timestamp": [datetime(2024, 3, 1, 0, i, tzinfo=timezone.utc) for i in range(10)],
        "open": [1.0] * 10,
        "high": [1.0] * 10,
        "low": [1.0] * 10,
        "close": [1.0] * 10,
        "volume": [1.0] * 10,
    })

    with patch("trading_dagster.assets.binance_futures_ohlcv.get_client"), \
         patch("trading_dagster.assets.binance_futures_ohlcv.query_scalar", side_effect=scalar_side_effect), \
         patch("trading_dagster.assets.binance_futures_ohlcv.execute_query") as mock_exec, \
         patch("trading_dagster.assets.binance_futures_ohlcv.insert_rows") as mock_insert, \
         patch("trading_dagster.assets.binance_futures_ohlcv.ExchangePriceDataService") as mock_svc:

        # Return data for first instrument, then empty to stop the loop
        mock_svc.fetch_ohlcv_times_series_df.side_effect = [fake_df]
        result = binance_futures_backfill_asset(ctx)

    # DELETE called exactly once (only for partial instrument)
    assert mock_exec.call_count == 1
    assert INSTRUMENTS[0] in mock_exec.call_args[0][0]
    # insert_rows called once for the partial instrument
    assert mock_insert.call_count == 1
    insert_call = mock_insert.call_args
    assert insert_call[0][0] == TARGET_TABLE
    assert len(insert_call[0][2]) == 10  # 10 rows from fake_df
