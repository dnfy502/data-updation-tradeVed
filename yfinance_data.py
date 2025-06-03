import yfinance as yf
import pandas as pd
import time
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YFinanceDataFetcher:
    def __init__(self, rate_limit_delay=2.0):
        """
        rate_limit_delay: Delay between API calls in seconds
        """
        self.rate_limit_delay = rate_limit_delay
        
    def get_stock_data(self, symbol, period="1y", interval="1d"):
        """
        Fetch OHLCV data for a single stock
        
        symbol: Stock symbol (e.g., 'RELIANCE.NS')
        period: Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
        interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        """
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            if data.empty:
                logger.warning(f"No data found for {symbol}")
                return None
                
            # Add symbol column
            data['Symbol'] = symbol.replace('.NS', '')
            data.reset_index(inplace=True)
            
            # Rename columns to standard format
            data.columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Symbol']
            
            # Keep only OHLCV + Symbol
            data = data[['Datetime', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
            logger.info(f"Fetched {len(data)} records for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None
    
    def get_multiple_stocks_data(self, symbols, period="1y", interval="1d"):
        """
        Fetch data for multiple stocks with rate limiting
        
        symbols: List of symbols ['RELIANCE.NS', 'TCS.NS', ...]
        """
        all_data = []
        
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
            return combined_data
        else:
            return pd.DataFrame()

# Usage Example
def main():
    # NSE stock symbols (add .NS suffix)
    nse_stocks = [
        'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
        'HDFC.NS', 'KOTAKBANK.NS', 'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS',
        'ASIANPAINT.NS', 'ITC.NS', 'AXISBANK.NS', 'LT.NS', 'DMART.NS'
    ]
    
    # Initialize fetcher with 1 second delay between calls
    fetcher = YFinanceDataFetcher(rate_limit_delay=2.0)
    
    # Different timeframes you might need
    timeframes = {
        '15m': {'period': '20d', 'interval': '15m'},   # 15min data for last 20 days
        '1h': {'period': '20d', 'interval': '1h'},     # 1hour data for last 20 days
        '1d': {'period': '20d', 'interval': '1d'},     # Daily data for last 20 days
        '1wk': {'period': '1y', 'interval': '1wk'},    # Weekly data for last 1 year
    }
    
    # Fetch data for different timeframes
    for tf_name, tf_params in timeframes.items():
        print(f"\n=== Fetching {tf_name} data ===")
        
        data = fetcher.get_multiple_stocks_data(
            symbols=nse_stocks[:5],  # Start with first 5 stocks for testing
            period=tf_params['period'],
            interval=tf_params['interval']
        )
        
        if not data.empty:
            print(f"Total records: {len(data)}")
            print(f"Date range: {data['Datetime'].min()} to {data['Datetime'].max()}")
            print(f"Stocks: {data['Symbol'].unique()}")
            
            # Save to CSV for inspection
            filename = f"market_data_{tf_name}.csv"
            data.to_csv(filename, index=False)
            print(f"Saved to {filename}")
        else:
            print("No data retrieved")

# Function for scheduled updates
def scheduled_data_update():
    """
    Function to be called by your scheduler (cron job, celery, etc.)
    """
    fetcher = YFinanceDataFetcher(rate_limit_delay=1.5)  # Slightly higher delay for scheduled jobs
    
    # For production, you'd get this list from your database
    active_stocks = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS']  # Example subset
    
    # Fetch latest data
    latest_data = fetcher.get_multiple_stocks_data(
        symbols=active_stocks,
        period='5d',      # Last 5 days
        interval='15m'    # 15 minute intervals
    )
    
    if not latest_data.empty:
        # Here you would update your database
        print(f"Updated data for {len(latest_data)} records")
        # save_to_database(latest_data)  # Your database function
    
    return latest_data

if __name__ == "__main__":
    main()
