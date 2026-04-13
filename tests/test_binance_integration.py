import pytest
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from trading_dagster.utils.exchange_price_service import ExchangePriceDataService

@pytest.mark.integration
def test_binance_perp_fetch_live_ohlcv():
    """
    Integration test: Fetches live OHLCV data from Binance USDM Futures (Perps).
    Ensures network connectivity and CCXT integration are working for the primary exchange.
    
    This test is intended to run in the CI pipeline (Tokyo region) where Binance is accessible.
    """
    # Detect CI environment (CodeBuild sets CODEBUILD_BUILD_ID)
    is_ci = os.getenv("CODEBUILD_BUILD_ID") is not None or os.getenv("CI") == "true"
    
    # Use a recent timestamp (1 hour ago)
    since_dt = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    symbol = "BTC/USDT"
    exchange_name = "binance_perp"
    
    print(f"\nAttempting to fetch {symbol} from {exchange_name}...")
    
    df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
        symbol=symbol,
        exchange_name=exchange_name,
        timeframe="1m",
        date_since=since_dt,
        limit=10
    )
    
    if df is None:
        # Check if it was a regional restriction (451 error)
        # In the Tokyo pipeline, this should NOT happen.
        if not is_ci:
            pytest.skip("Binance is restricted in this local environment. Skipping local integration test.")
        else:
            pytest.fail(f"CRITICAL: Failed to fetch {symbol} from {exchange_name} in CI environment. Check connectivity/region.")

    # Validation
    assert df is not None, f"DataFrame should not be None for {exchange_name}"
    assert not df.empty, f"DataFrame should not be empty for {exchange_name}"
    
    # Check essential columns for the analytics pipeline
    expected_columns = ["open", "high", "low", "close", "volume", "timestamp"]
    for col in expected_columns:
        assert col in df.columns, f"Missing column {col} in fetched data"
    
    # Check data types (timestamp must be UTC datetime)
    assert pd.api.types.is_datetime64_any_dtype(df['timestamp']), "timestamp column must be datetime"
    assert df['timestamp'].dt.tz is not None, "timestamp column must be timezone-aware (UTC)"
    
    print(f"Successfully fetched {len(df)} rows. Latest data point: {df['timestamp'].max()}")
