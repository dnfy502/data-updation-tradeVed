"""
Individual timeframe handlers for data updates
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional

from data_sources.base import BaseDataSource
from storage.file_storage import FileStorageManager
from utils.market_hours import market_hours
from utils.logging_config import log_performance, PerformanceLogger
from config.settings import TIMEFRAME_CONFIGS

logger = logging.getLogger(__name__)

class TimeframeHandler:
    """
    Base class for timeframe-specific update handlers
    """
    
    def __init__(self, data_source: BaseDataSource, storage_manager: FileStorageManager):
        self.data_source = data_source
        self.storage_manager = storage_manager
        
    def should_update(self, timeframe: str, last_update: datetime = None) -> bool:
        """
        Check if this timeframe should be updated
        
        Args:
            timeframe: Timeframe identifier
            last_update: Last update time
        
        Returns:
            True if update is needed
        """
        return market_hours.should_update_timeframe(timeframe, last_update)
    
    def get_symbols_to_update(self, symbols: List[str]) -> List[str]:
        """
        Filter symbols that need updating (can be overridden by subclasses)
        
        Args:
            symbols: List of all symbols
        
        Returns:
            Filtered list of symbols to update
        """
        return symbols
    
    @log_performance
    def update_data(self, timeframe: str, symbols: List[str]) -> bool:
        """
        Update data for a specific timeframe
        
        Args:
            timeframe: Timeframe to update
            symbols: List of symbols to update
        
        Returns:
            True if successful, False otherwise
        """
        try:
            config = TIMEFRAME_CONFIGS.get(timeframe)
            if not config:
                logger.error(f"No configuration found for timeframe: {timeframe}")
                return False
            
            # Filter symbols that need updating
            symbols_to_update = self.get_symbols_to_update(symbols)
            
            if not symbols_to_update:
                logger.info(f"No symbols need updating for {timeframe}")
                return True
            
            with PerformanceLogger(f"Fetching {timeframe} data for {len(symbols_to_update)} symbols"):
                # Fetch data from source
                data = self.data_source.get_multiple_stocks_data(
                    symbols=symbols_to_update,
                    period=config['period'],
                    interval=config['interval']
                )
                
                if data.empty:
                    logger.warning(f"No data retrieved for {timeframe}")
                    return False
                
                # Save data
                success = self.storage_manager.save_data(
                    data=data,
                    timeframe=timeframe,
                    append=self.should_append_data(timeframe)
                )
                
                if success:
                    logger.info(f"Successfully updated {timeframe} data: {len(data)} records")
                    return True
                else:
                    logger.error(f"Failed to save {timeframe} data")
                    return False
                    
        except Exception as e:
            logger.error(f"Error updating {timeframe} data: {str(e)}")
            return False
    
    def should_append_data(self, timeframe: str) -> bool:
        """
        Check if data should be appended (vs overwritten)
        
        Args:
            timeframe: Timeframe identifier
        
        Returns:
            True if data should be appended
        """
        # For intraday data, append new records
        # For daily/weekly data, might want to overwrite to ensure data consistency
        return timeframe in ['15m', '1h']

class IntradayHandler(TimeframeHandler):
    """
    Handler for intraday timeframes (15m, 1h)
    """
    
    def should_append_data(self, timeframe: str) -> bool:
        """Intraday data should be appended"""
        return True
    
    def get_symbols_to_update(self, symbols: List[str]) -> List[str]:
        """
        For intraday data, only update during market hours
        """
        current_time = market_hours.get_current_ist_time()
        
        if not market_hours.is_market_open(current_time):
            logger.info("Market is closed, skipping intraday update")
            return []
        
        return symbols

class DailyHandler(TimeframeHandler):
    """
    Handler for daily timeframe (1d)
    """
    
    def should_append_data(self, timeframe: str) -> bool:
        """Daily data can be appended or overwritten based on strategy"""
        return True
    
    def get_symbols_to_update(self, symbols: List[str]) -> List[str]:
        """
        For daily data, update after market close (4 PM IST)
        """
        current_time = market_hours.get_current_ist_time()
        
        # Only update after 4 PM
        if current_time.time().hour < 16:
            logger.info("Market not yet closed, skipping daily update")
            return []
        
        return symbols

class WeeklyHandler(TimeframeHandler):
    """
    Handler for weekly timeframe (1wk)
    """
    
    def should_append_data(self, timeframe: str) -> bool:
        """Weekly data typically overwrites to maintain clean dataset"""
        return False
    
    def get_symbols_to_update(self, symbols: List[str]) -> List[str]:
        """
        For weekly data, update on weekends
        """
        current_time = market_hours.get_current_ist_time()
        
        # Update on Saturday (weekday = 5)
        if current_time.weekday() != 5:
            logger.info("Not Saturday, skipping weekly update")
            return []
        
        return symbols

class SmartUpdateHandler(TimeframeHandler):
    """
    Smart handler that checks what data is missing and updates accordingly
    """
    
    def get_symbols_to_update(self, symbols: List[str]) -> List[str]:
        """
        Only update symbols that have missing or stale data
        """
        symbols_to_update = []
        
        for symbol in symbols:
            # Get last update time for this symbol
            last_update = self.storage_manager.get_latest_data_time('15m', symbol)  # Use 15m as reference
            
            if last_update is None:
                # Never updated, add to list
                symbols_to_update.append(symbol)
            else:
                # Check if data is stale
                current_time = market_hours.get_current_ist_time()
                time_diff = current_time - last_update
                
                # If more than 1 hour old during market hours, update
                if market_hours.is_market_open() and time_diff.total_seconds() > 3600:
                    symbols_to_update.append(symbol)
        
        return symbols_to_update

def get_handler_for_timeframe(timeframe: str, data_source: BaseDataSource, 
                            storage_manager: FileStorageManager) -> TimeframeHandler:
    """
    Factory function to get appropriate handler for timeframe
    
    Args:
        timeframe: Timeframe identifier
        data_source: Data source instance
        storage_manager: Storage manager instance
    
    Returns:
        Appropriate handler instance
    """
    if timeframe in ['15m', '1h']:
        return IntradayHandler(data_source, storage_manager)
    elif timeframe == '1d':
        return DailyHandler(data_source, storage_manager)
    elif timeframe == '1wk':
        return WeeklyHandler(data_source, storage_manager)
    else:
        # Default handler
        return TimeframeHandler(data_source, storage_manager) 