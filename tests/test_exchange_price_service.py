import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from trading_dagster.utils.exchange_price_service import ExchangePriceDataService
from trading_dagster.assets.binance_futures_ohlcv import _get_ccxt_symbol

class TestExchangePriceDataServiceUnit:
    """Unit tests for ExchangePriceDataService with mocking."""

    def test_to_iso8601_z(self):
        """Test timestamp conversion to ISO8601 UTC format."""
        # Test basic date
        ts = ExchangePriceDataService._to_iso8601_z("2024-01-01")
        assert ts == "2024-01-01T00:00:00Z"

        # Test datetime string
        ts = ExchangePriceDataService._to_iso8601_z("2024-01-01 12:34:56")
        assert ts == "2024-01-01T12:34:56Z"

        # Test utc_now (mocking to be safe)
        with patch("pandas.Timestamp.now") as mock_now:
            mock_now.return_value = pd.Timestamp("2024-01-01 10:00:00", tz="UTC")
            ts = ExchangePriceDataService._to_iso8601_z("utc_now")
            assert ts == "2024-01-01T10:00:00Z"

    def test_get_ccxt_symbol(self):
        """Test conversion of DB instrument names to CCXT symbols."""
        assert _get_ccxt_symbol("BTCUSDT") == "BTC/USDT"
        assert _get_ccxt_symbol("ETHUSDT") == "ETH/USDT"
        assert _get_ccxt_symbol("XRPUSDT") == "XRP/USDT"
        # Non-USDT should remain unchanged (if any)
        assert _get_ccxt_symbol("BTC/USDT") == "BTC/USDT"

    @patch("trading_dagster.utils.exchange_price_service.ExchangePriceDataService.get_exchange")
    def test_fetch_ohlcv_times_series_df_mocked(self, mock_get_exchange):
        """Test DataFrame construction from mocked CCXT response."""
        # Mock exchange object
        mock_ex = MagicMock()
        mock_ex.parse8601.return_value = 1704067200000  # 2024-01-01 00:00:00
        
        # Mock fetch_ohlcv return [ts, o, h, l, c, v]
        mock_ohlcv = [
            [1704067200000, 42000.0, 42100.0, 41900.0, 42050.0, 100.0],
            [1704067260000, 42050.0, 42150.0, 42000.0, 42100.0, 150.0]
        ]
        mock_ex.fetch_ohlcv.return_value = mock_ohlcv
        mock_get_exchange.return_value = mock_ex

        df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
            symbol="BTC/USDT",
            exchange_name="binance_perp",
            timeframe="1m",
            date_since="2024-01-01T00:00:00Z",
            limit=2
        )

        assert df is not None
        assert len(df) == 2
        assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume', 'timestamp']
        assert df.iloc[0]['close'] == 42050.0
        assert df.iloc[1]['timestamp'] == pd.Timestamp("2024-01-01 00:01:00", tz="UTC")

class TestExchangePriceDataServiceIntegration:
    """Integration tests hitting live API."""

    @pytest.mark.integration
    def test_fetch_ohlcv_bybit_live(self):
        """Test live fetch from Bybit (avoiding Binance 451 restricted location error)."""
        # Fetch actual data (small limit)
        df = ExchangePriceDataService.fetch_ohlcv_times_series_df(
            symbol="BTC/USDT",
            exchange_name="bybit",
            timeframe="1m",
            date_since=(datetime.now(timezone.utc) - pd.Timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            limit=5
        )

        assert df is not None
        assert not df.empty
        assert len(df) <= 5
        assert all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume', 'timestamp'])
        # Check if timestamps are close to now
        last_ts = df.iloc[-1]['timestamp']
        now = datetime.now(timezone.utc)
        # Should be within last 10 mins (allowing for some latency/rounding)
        diff = (now - last_ts).total_seconds()
        assert diff < 600 
