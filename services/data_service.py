"""
Main data service that orchestrates all components
"""
import logging
from typing import List, Dict, Optional, Union
import pandas as pd
from datetime import datetime

from data_sources.yfinance_source import YFinanceDataSource
from data_sources.fyers_source import FyersDataSource
from storage.file_storage import FileStorageManager
from storage.database import DatabaseManager
from schedulers.data_scheduler import DataScheduler
from utils.logging_config import setup_logging, get_logger
from utils.market_hours import market_hours
from config.settings import DEFAULT_DATA_SOURCE, RATE_LIMIT_DELAY
from config.symbols import get_symbols

logger = get_logger(__name__)

class DataService:
    """
    Main service class that orchestrates data fetching, storage, and scheduling
    """
    
    def __init__(self, data_source_type: str = None, symbol_set: str = 'development',
                 storage_type: str = 'file', auto_start_scheduler: bool = False):
        """
        Initialize the data service
        
        Args:
            data_source_type: 'yfinance' or 'fyers' (defaults to config setting)
            symbol_set: Symbol set to use ('development', 'production', etc.)
            storage_type: 'file' or 'database' (currently only 'file' is implemented)
            auto_start_scheduler: Whether to automatically start the scheduler
        """
        # Setup logging
        setup_logging()
        
        self.data_source_type = data_source_type or DEFAULT_DATA_SOURCE
        self.symbol_set = symbol_set
        self.storage_type = storage_type
        
        # Initialize components
        self.data_source = self._initialize_data_source()
        self.storage_manager = self._initialize_storage()
        self.scheduler = None
        
        # Get symbols
        self.symbols = get_symbols(symbol_set)
        
        logger.info(f"DataService initialized:")
        logger.info(f"  - Data source: {self.data_source_type}")
        logger.info(f"  - Symbol set: {symbol_set} ({len(self.symbols)} symbols)")
        logger.info(f"  - Storage: {storage_type}")
        
        # Start scheduler if requested
        if auto_start_scheduler:
            self.start_scheduler()
    
    def _initialize_data_source(self):
        """Initialize the appropriate data source"""
        if self.data_source_type == 'yfinance':
            return YFinanceDataSource(rate_limit_delay=RATE_LIMIT_DELAY)
        elif self.data_source_type == 'fyers':
            return FyersDataSource(rate_limit_delay=RATE_LIMIT_DELAY)
        else:
            raise ValueError(f"Unsupported data source: {self.data_source_type}")
    
    def _initialize_storage(self):
        """Initialize the appropriate storage manager"""
        if self.storage_type == 'file':
            return FileStorageManager()
        elif self.storage_type == 'database':
            return DatabaseManager()  # Placeholder for now
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
    
    def start_scheduler(self) -> bool:
        """
        Start the automatic data scheduler
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.scheduler and self.scheduler.is_running:
                logger.warning("Scheduler is already running")
                return True
            
            self.scheduler = DataScheduler(
                data_source=self.data_source,
                storage_manager=self.storage_manager,
                symbol_set=self.symbol_set
            )
            
            return self.scheduler.start()
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}")
            return False
    
    def stop_scheduler(self):
        """Stop the automatic data scheduler"""
        if self.scheduler:
            self.scheduler.stop()
            logger.info("Scheduler stopped")
        else:
            logger.warning("No scheduler running")
    
    def fetch_data(self, timeframe: str, symbols: List[str] = None, 
                   save_data: bool = True) -> pd.DataFrame:
        """
        Fetch data for specific timeframe and symbols
        
        Args:
            timeframe: '15m', '1h', '1d', or '1wk'
            symbols: List of symbols (defaults to configured symbols)
            save_data: Whether to save the data to storage
        
        Returns:
            DataFrame with fetched data
        """
        try:
            from config.settings import TIMEFRAME_CONFIGS
            
            symbols_to_fetch = symbols or self.symbols
            config = TIMEFRAME_CONFIGS.get(timeframe)
            
            if not config:
                raise ValueError(f"Invalid timeframe: {timeframe}")
            
            logger.info(f"Fetching {timeframe} data for {len(symbols_to_fetch)} symbols")
            
            data = self.data_source.get_multiple_stocks_data(
                symbols=symbols_to_fetch,
                period=config['period'],
                interval=config['interval']
            )
            
            if data.empty:
                logger.warning(f"No data retrieved for {timeframe}")
                return data
            
            if save_data:
                success = self.storage_manager.save_data(
                    data=data,
                    timeframe=timeframe,
                    append=True
                )
                if success:
                    logger.info(f"Saved {len(data)} records for {timeframe}")
                else:
                    logger.error(f"Failed to save data for {timeframe}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error fetching data for {timeframe}: {str(e)}")
            return pd.DataFrame()
    
    def load_data(self, timeframe: str, symbols: List[str] = None,
                  start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Load data from storage
        
        Args:
            timeframe: Timeframe to load
            symbols: Symbols to filter (None for all)
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)
        
        Returns:
            DataFrame with loaded data
        """
        try:
            data = self.storage_manager.load_data(timeframe, symbol_filter=symbols)
            
            if data.empty:
                return data
            
            # Apply date filters if provided
            if start_date or end_date:
                if 'Datetime' in data.columns:
                    data['Datetime'] = pd.to_datetime(data['Datetime'])
                    
                    if start_date:
                        data = data[data['Datetime'] >= start_date]
                    if end_date:
                        data = data[data['Datetime'] <= end_date]
            
            logger.info(f"Loaded {len(data)} records for {timeframe}")
            return data
            
        except Exception as e:
            logger.error(f"Error loading data for {timeframe}: {str(e)}")
            return pd.DataFrame()
    
    def manual_update(self, timeframe: str = None, symbols: List[str] = None) -> bool:
        """
        Manually trigger a data update
        
        Args:
            timeframe: Specific timeframe to update (None for all)
            symbols: Specific symbols to update (None for all)
        
        Returns:
            True if successful, False otherwise
        """
        if self.scheduler:
            return self.scheduler.manual_update(timeframe, symbols)
        else:
            # Run update directly if no scheduler
            if timeframe:
                data = self.fetch_data(timeframe, symbols)
                return not data.empty
            else:
                # Update all timeframes
                from config.settings import TIMEFRAME_CONFIGS
                success_count = 0
                for tf in TIMEFRAME_CONFIGS.keys():
                    data = self.fetch_data(tf, symbols)
                    if not data.empty:
                        success_count += 1
                
                return success_count == len(TIMEFRAME_CONFIGS)
    
    def get_status(self) -> Dict:
        """
        Get comprehensive service status
        
        Returns:
            Dictionary with status information
        """
        status = {
            'service': {
                'data_source': self.data_source_type,
                'storage_type': self.storage_type,
                'symbol_set': self.symbol_set,
                'symbols_count': len(self.symbols),
                'data_source_available': self.data_source.is_available()
            },
            'market': {
                'status': market_hours.get_market_status(),
                'is_open': market_hours.is_market_open(),
                'next_open': market_hours.get_next_market_open().isoformat()
            },
            'scheduler': {
                'running': False,
                'jobs': []
            }
        }
        
        if self.scheduler:
            scheduler_status = self.scheduler.get_status()
            status['scheduler'] = scheduler_status
        
        # Add storage summary
        if hasattr(self.storage_manager, 'get_data_summary'):
            status['storage'] = self.storage_manager.get_data_summary()
        
        return status
    
    def get_data_summary(self) -> Dict:
        """
        Get summary of available data
        
        Returns:
            Dictionary with data summary by timeframe
        """
        summary = {}
        
        from config.settings import TIMEFRAME_CONFIGS
        
        for timeframe in TIMEFRAME_CONFIGS.keys():
            try:
                data = self.load_data(timeframe)
                if not data.empty:
                    summary[timeframe] = {
                        'records': len(data),
                        'symbols': data['Symbol'].nunique() if 'Symbol' in data.columns else 0,
                        'date_range': {
                            'start': data['Datetime'].min().isoformat() if 'Datetime' in data.columns else None,
                            'end': data['Datetime'].max().isoformat() if 'Datetime' in data.columns else None
                        },
                        'last_update': self.storage_manager.get_latest_data_time(timeframe)
                    }
                else:
                    summary[timeframe] = {
                        'records': 0,
                        'symbols': 0,
                        'date_range': None,
                        'last_update': None
                    }
            except Exception as e:
                logger.error(f"Error getting summary for {timeframe}: {str(e)}")
                summary[timeframe] = {'error': str(e)}
        
        return summary
    
    def switch_data_source(self, new_source: str) -> bool:
        """
        Switch to a different data source
        
        Args:
            new_source: 'yfinance' or 'fyers'
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Stop scheduler if running
            was_running = self.scheduler and self.scheduler.is_running
            if was_running:
                self.stop_scheduler()
            
            # Initialize new data source
            old_source = self.data_source_type
            self.data_source_type = new_source
            self.data_source = self._initialize_data_source()
            
            # Restart scheduler if it was running
            if was_running:
                self.start_scheduler()
            
            logger.info(f"Switched data source from {old_source} to {new_source}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to switch data source to {new_source}: {str(e)}")
            return False
    
    def update_symbols(self, symbol_set: str) -> bool:
        """
        Update the symbol set being tracked
        
        Args:
            symbol_set: New symbol set identifier
        
        Returns:
            True if successful, False otherwise
        """
        try:
            old_count = len(self.symbols)
            self.symbol_set = symbol_set
            self.symbols = get_symbols(symbol_set)
            
            # Update scheduler if running
            if self.scheduler:
                self.scheduler.update_symbol_set(symbol_set)
            
            logger.info(f"Updated symbols from {old_count} to {len(self.symbols)} symbols")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update symbols: {str(e)}")
            return False
    
    def cleanup(self):
        """Clean up resources"""
        try:
            if self.scheduler:
                self.stop_scheduler()
            
            # Cleanup old files
            if hasattr(self.storage_manager, 'cleanup_old_files'):
                self.storage_manager.cleanup_old_files()
            
            logger.info("DataService cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

# Factory function for easy service creation
def create_data_service(data_source: str = None, symbol_set: str = 'development',
                       auto_start: bool = False) -> DataService:
    """
    Factory function to create a configured DataService
    
    Args:
        data_source: Data source type ('yfinance', 'fyers')
        symbol_set: Symbol set to use
        auto_start: Whether to start scheduler automatically
    
    Returns:
        Configured DataService instance
    """
    return DataService(
        data_source_type=data_source,
        symbol_set=symbol_set,
        auto_start_scheduler=auto_start
    ) 