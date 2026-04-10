import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import trading_dagster.utils.exchange_price_service as price_service
from trading_dagster.assets.binance_futures_ohlcv import _get_ccxt_symbol

def test_get_ccxt_symbol():
    assert _get_ccxt_symbol("BTCUSDT") == "BTC/USDT"
    assert _get_ccxt_symbol("ETH/USDT") == "ETH/USDT"
    assert _get_ccxt_symbol("SOLUSDT") == "SOL/USDT"

def test_fetch_ohlcv_logic():
    # Setup mock data (3 minutes of 1-min candles)
    mock_ohlcv = [
        [1712275200000, 60000, 60100, 59900, 60050, 10], # 2024-04-05 00:00:00
        [1712275260000, 60050, 60200, 60000, 60150, 15], # 2024-04-05 00:01:00
        [1712275320000, 60150, 60300, 60100, 60250, 20], # 2024-04-05 00:02:00
    ]
    
    # Configure the mock exchange
    mock_exchange = MagicMock()
    mock_exchange.fetch_ohlcv.return_value = mock_ohlcv
    # Mock parse8601 to simulate CCXT's behavior
    mock_exchange.parse8601.return_value = 1712275200000

    # Inject the mock exchange into the service's dictionary
    with patch.dict('trading_dagster.utils.exchange_price_service.ExchangePriceDataService.exchange_dict', 
                    {'binance_perp': mock_exchange}):
        
        df = price_service.ExchangePriceDataService.fetch_ohlcv_times_series_df(
            symbol="BTC/USDT",
            exchange_name="binance_perp",
            timeframe="1m",
            date_since="2024-04-05T00:00:00Z",
            limit=1000
        )
    
    assert df is not None
    assert not df.empty
    assert len(df) == 3
    # Corrected column order based on implementation
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "timestamp"]
    
    # Verify timestamp conversion (UTC)
    assert df.iloc[0]['timestamp'] == datetime(2024, 4, 5, 0, 0, tzinfo=timezone.utc)
    assert df.iloc[2]['timestamp'] == datetime(2024, 4, 5, 0, 2, tzinfo=timezone.utc)

@pytest.mark.unit
def test_partition_loop_logic():
    partition_date = "2024-04-05"
    start_dt = datetime.strptime(partition_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    from datetime import timedelta
    end_dt = start_dt + timedelta(days=1)
    
    data = {
        'timestamp': [
            start_dt + timedelta(hours=23, minutes=59),
            start_dt + timedelta(days=1) # This is 00:00 on the 6th
        ]
    }
    df = pd.DataFrame(data)
    
    # Filter logic: [start, end)
    filtered_df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] < end_dt)]
    
    assert len(filtered_df) == 1
    assert filtered_df.iloc[0]['timestamp'].day == 5
