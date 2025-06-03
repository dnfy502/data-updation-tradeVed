"""
YFinance data source implementation
"""
import yfinance as yf
import pandas as pd
import time
from typing import List, Optional
import logging

from .base import BaseDataSource

logger = logging.getLogger(__name__)

class YFinanceDataSource(BaseDataSource):
    """
    YFinance implementation of BaseDataSource
    """
    
    def __init__(self, rate_limit_delay: float = 2.0):
        super().__init__(rate_limit_delay)
        self.source_name = "YFinance"
    
    def get_stock_data(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        """
        Fetch OHLCV data for a single stock using yfinance
        """
        try:
            # Validate inputs
            if not self.validate_symbol(symbol):
                logger.error(f"Invalid symbol: {symbol}")
                return None
            
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            if data.empty:
                logger.warning(f"No data found for {symbol}")
                return None
            
            # Reset index to get Datetime as column
            data.reset_index(inplace=True)
            
            # Standardize column names
            if 'Date' in data.columns:
                data.rename(columns={'Date': 'Datetime'}, inplace=True)
            
            # Add symbol column
            data['Symbol'] = symbol.replace('.NS', '')
            
            # Rename columns to standard format
            column_mapping = {
                'Dividends': 'Dividends',
                'Stock Splits': 'Stock_Splits'
            }
            data.rename(columns=column_mapping, inplace=True)
            
            # Keep only required columns
            required_columns = ['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
            available_columns = [col for col in required_columns if col in data.columns]
            data = data[available_columns]
            
            # Standardize using parent method
            data = self.standardize_dataframe(data, symbol)
            
            logger.info(f"Fetched {len(data)} records for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None
    
    def get_multiple_stocks_data(self, symbols: List[str], period: str, interval: str) -> pd.DataFrame:
        """
        Fetch data for multiple stocks with rate limiting
        """
        all_data = []
        
        logger.info(f"Fetching data for {len(symbols)} symbols")
        
        for i, symbol in enumerate(symbols):
            logger.info(f"Fetching {symbol} ({i+1}/{len(symbols)})")
            
            data = self.get_stock_data(symbol, period, interval)
            if data is not None:
                all_data.append(data)
            
            # Rate limiting - wait between requests
            if i < len(symbols) - 1:  # Don't wait after last symbol
                time.sleep(self.rate_limit_delay)
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logger.info(f"Combined data: {len(combined_data)} total records")
            return combined_data
        else:
            logger.warning("No data retrieved for any symbols")
            return pd.DataFrame()
    
    def is_available(self) -> bool:
        """
        Check if YFinance is available by testing a simple request
        """
        try:
            # Test with a reliable symbol
            ticker = yf.Ticker("RELIANCE.NS")
            test_data = ticker.history(period="1d", interval="1d")
            return not test_data.empty
        except Exception as e:
            logger.error(f"YFinance availability check failed: {str(e)}")
            return False
    
    def validate_symbol(self, symbol: str) -> bool:
        """
        Validate symbol format for YFinance (NSE symbols should end with .NS)
        """
        if not super().validate_symbol(symbol):
            return False
        
        # For Indian stocks, symbol should end with .NS
        # For US stocks, no suffix needed
        # This is a basic validation - you might want more sophisticated logic
        return True
    
    def get_supported_intervals(self) -> List[str]:
        """
        YFinance supported intervals
        """
        return [
            '1m', '2m', '5m', '15m', '30m', '60m', '90m',
            '1h', '1d', '5d', '1wk', '1mo', '3mo'
        ]
    
    def get_supported_periods(self) -> List[str]:
        """
        YFinance supported periods
        """
        return [
            '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', 
            '5y', '10y', 'ytd', 'max'
        ]
    
    def get_company_info(self, symbol: str) -> dict:
        """
        Get company information (additional feature specific to YFinance)
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            return info
        except Exception as e:
            logger.error(f"Error getting company info for {symbol}: {str(e)}")
            return {}
    
    def get_financials(self, symbol: str) -> dict:
        """
        Get financial statements (additional feature specific to YFinance)
        """
        try:
            ticker = yf.Ticker(symbol)
            financials = {
                'income_stmt': ticker.financials,
                'balance_sheet': ticker.balance_sheet,
                'cash_flow': ticker.cashflow
            }
            return financials
        except Exception as e:
            logger.error(f"Error getting financials for {symbol}: {str(e)}")
            return {} 