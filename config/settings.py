"""
Main configuration settings for the data fetching system
"""
import os
from datetime import time

# Data source configuration
DEFAULT_DATA_SOURCE = 'yfinance'  # Can be changed to 'fyers' later
RATE_LIMIT_DELAY = 2.0  # Seconds between API calls

# File storage configuration
DATA_STORAGE_PATH = os.path.join(os.getcwd(), 'data')
CSV_FILE_PREFIX = 'market_data'

# Market hours (IST)
MARKET_OPEN_TIME = time(9, 15)   # 9:15 AM
MARKET_CLOSE_TIME = time(15, 30)  # 3:30 PM

# Timeframe configurations
TIMEFRAME_CONFIGS = {
    '15m': {
        'period': '5d',      # Last 5 days for intraday
        'interval': '15m',
        'update_frequency_minutes': 15,
        'active_during_market_hours_only': True
    },
    '1h': {
        'period': '5d',      # Last 5 days for intraday  
        'interval': '1h',
        'update_frequency_minutes': 60,
        'active_during_market_hours_only': True
    },
    '1d': {
        'period': '1y',      # Last 1 year for daily data
        'interval': '1d',
        'update_frequency_minutes': 1440,  # Once daily
        'active_during_market_hours_only': False,
        'preferred_update_time': time(16, 0)  # 4 PM IST
    },
    '1wk': {
        'period': '2y',      # Last 2 years for weekly data
        'interval': '1wk', 
        'update_frequency_minutes': 10080,  # Once weekly (7*24*60)
        'active_during_market_hours_only': False,
        'preferred_update_day': 'saturday',
        'preferred_update_time': time(8, 0)  # 8 AM Saturday
    }
}

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'data_fetcher.log'

# Error handling
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds

# Monitoring
ENABLE_PERFORMANCE_MONITORING = True
ALERT_ON_ERRORS = True 