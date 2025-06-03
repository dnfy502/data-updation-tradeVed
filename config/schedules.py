"""
Scheduling configuration for different timeframes
"""
from datetime import time, datetime

# Cron-like schedule definitions
SCHEDULES = {
    '15m': {
        'description': 'Every 15 minutes during market hours',
        'cron': '*/15 9-15 * * 1-5',  # Every 15 min, 9-15 hours, Mon-Fri
        'enabled': True,
        'market_hours_only': True
    },
    '1h': {
        'description': 'Every hour during market hours',
        'cron': '0 9-15 * * 1-5',     # Every hour at minute 0, 9-15 hours, Mon-Fri
        'enabled': True,
        'market_hours_only': True
    },
    '1d': {
        'description': 'Daily at 4 PM IST after market close',
        'cron': '0 16 * * 1-5',       # At 4 PM, Mon-Fri
        'enabled': True,
        'market_hours_only': False
    },
    '1wk': {
        'description': 'Weekly on Saturday at 8 AM',
        'cron': '0 8 * * 6',          # At 8 AM on Saturday
        'enabled': True,
        'market_hours_only': False
    }
}

# Manual update windows (when scheduler can run outside normal schedule)
MANUAL_UPDATE_WINDOWS = {
    'weekdays': {
        'start_time': time(6, 0),   # 6 AM
        'end_time': time(20, 0)     # 8 PM
    },
    'weekends': {
        'start_time': time(8, 0),   # 8 AM
        'end_time': time(18, 0)     # 6 PM
    }
}

# Holiday configuration (when markets are closed)
MARKET_HOLIDAYS_2024 = [
    '2024-01-26',  # Republic Day
    '2024-03-08',  # Holi
    '2024-03-29',  # Good Friday
    '2024-04-11',  # Ram Navami
    '2024-04-17',  # Mahavir Jayanti
    '2024-05-01',  # Maharashtra Day
    '2024-08-15',  # Independence Day
    '2024-08-26',  # Janmashtami
    '2024-10-02',  # Gandhi Jayanti
    '2024-10-12',  # Dussehra
    '2024-11-01',  # Diwali
    '2024-11-15',  # Guru Nanak Jayanti
    '2024-12-25',  # Christmas
]

def is_market_holiday(date_str=None):
    """
    Check if a given date is a market holiday
    
    Args:
        date_str: Date in 'YYYY-MM-DD' format. If None, uses today.
    
    Returns:
        bool: True if it's a holiday
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    return date_str in MARKET_HOLIDAYS_2024

def is_trading_day(date_obj=None):
    """
    Check if a given date is a trading day (not weekend or holiday)
    
    Args:
        date_obj: datetime object. If None, uses today.
    
    Returns:
        bool: True if it's a trading day
    """
    if date_obj is None:
        date_obj = datetime.now()
    
    # Check if weekend (Saturday=5, Sunday=6)
    if date_obj.weekday() in [5, 6]:
        return False
    
    # Check if holiday
    date_str = date_obj.strftime('%Y-%m-%d')
    return not is_market_holiday(date_str) 