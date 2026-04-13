
from typing import List, Union, Dict, Optional
import ccxt
import time
import pandas as pd
from loguru import logger
from ccxt.base.exchange import Exchange
from datetime import datetime


class ExchangePriceDataService:
    _TIMEFRAME_TIMESTAMP_DICT = {'1d': 86400000, '1h': 3600000, '30m': 1800000,
                                '15m': 900000, '10m': 600000 , '5m': 300000, '1m': 60000}
    _COLUMN_NAME_LIST: List[str] = ['open', 'high', 'low', 'close', 'volume']

    # init exchange
    binance: Exchange = ccxt.binance()
    binanceusdm: Exchange = ccxt.binanceusdm()
    bybit: Exchange = ccxt.bybit()
    okx: Exchange = ccxt.okx()
    exchange_dict: Dict = {'binance': binance, 'binance_perp': binanceusdm,
                           'bybit': bybit, "okx": okx}

    def __init__(self):
        return

    @staticmethod
    def _to_iso8601_z(x: str) -> str:
        """
        Accepts:
          - 'utc_now'
          - 'YYYY-MM-DD'
          - 'YYYY-MM-DD HH:MM:SS'
          - 'YYYY-MM-DDTHH:MM:SSZ'
          - timezone-aware strings
        Returns: 'YYYY-MM-DDTHH:MM:SSZ' (UTC)
        """
        if x == "utc_now":
            ts = pd.Timestamp.now(tz="UTC").floor("s")
        else:
            ts = pd.to_datetime(x, utc=True)
            # pd.to_datetime(..., utc=True) already returns tz-aware UTC

        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    def safe_fetch_ohlcv(
        exchange: Exchange,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
        max_retries: int = 3,
        sleep_seconds: int = 5
    ) -> List[List[Union[int, float]]]:


        """
        Safely fetches OHLCV with up to `max_retries` attempts.
        This helps handle transient errors (network issues, server timeouts, etc.).

        Returns a list of OHLCV candles (list of [timestamp, open, high, low, close, volume]),
        or an empty list if all attempts fail.
        """
        for attempt in range(1, max_retries + 1):
            try:
                if not exchange.markets:
                    exchange.load_markets()
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
                return ohlcv
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                logger.warning(f"safe_fetch_ohlcv => Attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(sleep_seconds)
                else:
                    logger.error(f"All {max_retries} attempts failed for fetch_ohlcv on {symbol}, {timeframe}.")
                    return []

    @staticmethod
    def get_exchange(exchange_name: str,
                     ex_default_type: Optional[str] = None) -> Optional[Exchange]:
        base_exchange = ExchangePriceDataService.exchange_dict.get(exchange_name)
        if base_exchange is None:
            logger.error(f"Exchange '{exchange_name}' is not available!")
            return None

        # If exchange_name == 'binanceusdm', we've already got USDM futures.
        # No need to set defaultType or clone.
        if exchange_name == 'binance_perp':
            logger.info("[FETCH CCXT PRICE] binanceusdm (native USDT futures).")
            return base_exchange

        # For Bybit & OKX, you can still do a defaultType if ex_default_type is not set
        if ex_default_type is None and exchange_name in ['bybit', 'okx']:
            ex_default_type = 'swap'

        if ex_default_type:
            new_exchange = type(base_exchange)({
                'options': {
                    'defaultType': ex_default_type
                }
            })
            logger.info(f"[FETCH CCXT PRICE] {exchange_name}: Using defaultType='{ex_default_type}'.")
            return new_exchange
        else:
            return base_exchange

    @classmethod
    def fetch_ohlcv_times_series_df(
        cls,
        symbol: str = "BTC/USDT",
        exchange_name: str = "binance",
        timeframe: str = "1m",
        date_since: str = "2020-05-01T00:00:00Z",
        ex_default_type: Optional[str] = None,
        limit: int = 1000
    ) -> Optional[pd.DataFrame]:

        try:
            date_since_iso = cls._to_iso8601_z(date_since)
        except Exception as e:
            logger.error(f"Error parsing date_since='{date_since}': {e}")
            return None

        exchange = cls.get_exchange(exchange_name, ex_default_type=ex_default_type)
        if exchange is None:
            logger.error(f"Input exchange is not available! exchange_name={exchange_name}")
            return None

        timestamp_int_since: int = exchange.parse8601(date_since_iso)

        # safe fetch with appropriate limit
        all_candle_list = cls.safe_fetch_ohlcv(
            exchange, symbol, timeframe, since=timestamp_int_since, limit=limit
        )

        if not all_candle_list:
            logger.warning(f"No data from {date_since} for {symbol} on {exchange_name}.")
            return None

        # Build clean dataframe
        candlestick_df = pd.DataFrame(
            all_candle_list, 
            columns=['timestamp_int', 'open', 'high', 'low', 'close', 'volume']
        )
        
        # Convert timestamp to human readable datetime (UTC)
        candlestick_df['timestamp'] = pd.to_datetime(candlestick_df['timestamp_int'], unit='ms', utc=True)
        candlestick_df = candlestick_df.drop(columns=["timestamp_int"])

        return candlestick_df

