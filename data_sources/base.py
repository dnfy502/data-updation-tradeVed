"""
Abstract base class for data sources
This allows easy switching between yfinance, fyers, etc.
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Optional

class BaseDataSource(ABC):
    """
    Abstract base class for all data sources
    """
    
    def __init__(self, rate_limit_delay: float = 1.0):
        self.rate_limit_delay = rate_limit_delay
        self.source_name = self.__class__.__name__
    
    @abstractmethod
    def get_stock_data(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data for a single stock
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE.NS')
            period: Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        
        Returns:
            DataFrame with columns: ['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
            None if no data found or error occurred
        """
        pass
    
    @abstractmethod
    def get_multiple_stocks_data(self, symbols: List[str], period: str, interval: str) -> pd.DataFrame:
        """
        Fetch data for multiple stocks with rate limiting
        
        Args:
            symbols: List of symbols ['RELIANCE.NS', 'TCS.NS', ...]
            period: Data period
            interval: Data interval
        
        Returns:
            Combined DataFrame with all stocks data
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """
        Check if the data source is available and can be used
        
        Returns:
            bool: True if available, False otherwise
        """
        pass
    
    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate if symbol format is correct for this data source
        
        Args:
            symbol: Stock symbol to validate
        
        Returns:
            bool: True if valid, False otherwise
        """
        # Basic validation - override in subclasses for specific rules
        return symbol and isinstance(symbol, str) and len(symbol) > 0
    
    def standardize_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Standardize DataFrame to common format
        
        Args:
            df: Raw DataFrame from data source
            symbol: Stock symbol
        
        Returns:
            Standardized DataFrame
        """
        if df.empty:
            return df
        
        # Ensure required columns exist
        required_columns = ['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # Add Symbol column if missing
        if 'Symbol' not in df.columns:
            df['Symbol'] = symbol.replace('.NS', '')
        
        # Ensure Datetime is properly formatted
        if 'Datetime' in df.columns:
            df['Datetime'] = pd.to_datetime(df['Datetime'])
        
        # Select only required columns (if they exist)
        available_columns = [col for col in required_columns if col in df.columns]
        df = df[available_columns]
        
        return df
    
    def get_supported_intervals(self) -> List[str]:
        """
        Get list of supported intervals for this data source
        
        Returns:
            List of supported interval strings
        """
        # Default intervals - override in subclasses
        return ['1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo']
    
    def get_supported_periods(self) -> List[str]:
        """
        Get list of supported periods for this data source
        
        Returns:
            List of supported period strings
        """
        # Default periods - override in subclasses
        return ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'] 