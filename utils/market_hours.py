"""
Market hours and timing utilities for Indian stock market
"""
from datetime import datetime, time, timedelta
import pytz
from typing import Tuple
import logging

from config.settings import MARKET_OPEN_TIME, MARKET_CLOSE_TIME
from config.schedules import is_trading_day

logger = logging.getLogger(__name__)

# Indian Standard Time
IST = pytz.timezone('Asia/Kolkata')

class MarketHours:
    """
    Utility class for market hours and timing operations
    """
    
    def __init__(self):
        self.market_open = MARKET_OPEN_TIME
        self.market_close = MARKET_CLOSE_TIME
        self.timezone = IST
    
    def get_current_ist_time(self) -> datetime:
        """
        Get current time in IST
        
        Returns:
            Current datetime in IST
        """
        utc_now = datetime.now(pytz.UTC)
        ist_now = utc_now.astimezone(IST)
        return ist_now
    
    def is_market_open(self, check_time: datetime = None) -> bool:
        """
        Check if market is currently open
        
        Args:
            check_time: Time to check (defaults to current time)
        
        Returns:
            True if market is open, False otherwise
        """
        if check_time is None:
            check_time = self.get_current_ist_time()
        
        # Check if it's a trading day
        if not is_trading_day(check_time):
            return False
        
        # Check if time is within market hours
        current_time = check_time.time()
        return self.market_open <= current_time <= self.market_close
    
    def get_market_status(self, check_time: datetime = None) -> str:
        """
        Get detailed market status
        
        Args:
            check_time: Time to check (defaults to current time)
        
        Returns:
            Market status string
        """
        if check_time is None:
            check_time = self.get_current_ist_time()
        
        if not is_trading_day(check_time):
            return "Market Closed (Holiday/Weekend)"
        
        current_time = check_time.time()
        
        if current_time < self.market_open:
            return "Pre-Market"
        elif current_time > self.market_close:
            return "Post-Market"
        else:
            return "Market Open"
    
    def time_to_market_open(self, check_time: datetime = None) -> timedelta:
        """
        Get time remaining until market opens
        
        Args:
            check_time: Reference time (defaults to current time)
        
        Returns:
            Timedelta until market opens
        """
        if check_time is None:
            check_time = self.get_current_ist_time()
        
        # If market is open, return zero
        if self.is_market_open(check_time):
            return timedelta(0)
        
        # Find next market open time
        next_open = self.get_next_market_open(check_time)
        return next_open - check_time
    
    def time_to_market_close(self, check_time: datetime = None) -> timedelta:
        """
        Get time remaining until market closes
        
        Args:
            check_time: Reference time (defaults to current time)
        
        Returns:
            Timedelta until market closes (negative if market is closed)
        """
        if check_time is None:
            check_time = self.get_current_ist_time()
        
        # If market is closed, return negative value
        if not self.is_market_open(check_time):
            return timedelta(seconds=-1)
        
        # Calculate time to close
        market_close_today = datetime.combine(check_time.date(), self.market_close)
        market_close_today = IST.localize(market_close_today)
        
        return market_close_today - check_time
    
    def get_next_market_open(self, check_time: datetime = None) -> datetime:
        """
        Get the next market opening time
        
        Args:
            check_time: Reference time (defaults to current time)
        
        Returns:
            Next market opening datetime
        """
        if check_time is None:
            check_time = self.get_current_ist_time()
        
        # Start with today
        candidate_date = check_time.date()
        
        # If today's market has already closed, start with tomorrow
        if check_time.time() > self.market_close:
            candidate_date += timedelta(days=1)
        
        # Find next trading day
        while not is_trading_day(datetime.combine(candidate_date, time())):
            candidate_date += timedelta(days=1)
        
        # Combine with market open time
        next_open = datetime.combine(candidate_date, self.market_open)
        return IST.localize(next_open)
    
    def get_market_session_times(self, date: datetime = None) -> Tuple[datetime, datetime]:
        """
        Get market open and close times for a specific date
        
        Args:
            date: Date to check (defaults to today)
        
        Returns:
            Tuple of (market_open_datetime, market_close_datetime)
        """
        if date is None:
            date = self.get_current_ist_time()
        
        market_open_dt = datetime.combine(date.date(), self.market_open)
        market_close_dt = datetime.combine(date.date(), self.market_close)
        
        market_open_dt = IST.localize(market_open_dt)
        market_close_dt = IST.localize(market_close_dt)
        
        return market_open_dt, market_close_dt
    
    def should_update_timeframe(self, timeframe: str, last_update: datetime = None) -> bool:
        """
        Check if a timeframe should be updated based on market hours and intervals
        
        Args:
            timeframe: Timeframe identifier ('15m', '1h', '1d', '1wk')
            last_update: Last update time (None if never updated)
        
        Returns:
            True if update is needed, False otherwise
        """
        current_time = self.get_current_ist_time()
        
        # If never updated, update now
        if last_update is None:
            return True
        
        # Convert last_update to IST if needed
        if last_update.tzinfo is None:
            last_update = IST.localize(last_update)
        elif last_update.tzinfo != IST:
            last_update = last_update.astimezone(IST)
        
        # Define update logic for each timeframe
        if timeframe == '15m':
            # Update every 15 minutes during market hours
            if not self.is_market_open(current_time):
                return False
            return (current_time - last_update).total_seconds() >= 15 * 60
        
        elif timeframe == '1h':
            # Update every hour during market hours
            if not self.is_market_open(current_time):
                return False
            return (current_time - last_update).total_seconds() >= 60 * 60
        
        elif timeframe == '1d':
            # Update once daily after market close
            if current_time.time() < time(16, 0):  # Before 4 PM
                return False
            return current_time.date() > last_update.date()
        
        elif timeframe == '1wk':
            # Update once weekly on Saturday
            if current_time.weekday() != 5:  # Not Saturday
                return False
            return (current_time - last_update).days >= 7
        
        return False
    
    def get_trading_days_between(self, start_date: datetime, end_date: datetime) -> int:
        """
        Get number of trading days between two dates
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            Number of trading days
        """
        trading_days = 0
        current_date = start_date.date()
        end = end_date.date()
        
        while current_date <= end:
            if is_trading_day(datetime.combine(current_date, time())):
                trading_days += 1
            current_date += timedelta(days=1)
        
        return trading_days

# Global instance for easy access
market_hours = MarketHours()

# Convenience functions
def is_market_open_now() -> bool:
    """Check if market is currently open"""
    return market_hours.is_market_open()

def get_market_status_now() -> str:
    """Get current market status"""
    return market_hours.get_market_status()

def should_update_now(timeframe: str, last_update: datetime = None) -> bool:
    """Check if timeframe should be updated now"""
    return market_hours.should_update_timeframe(timeframe, last_update) 