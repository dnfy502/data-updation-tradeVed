"""
Main data scheduler for automated updates
"""
import logging
import schedule
import time
import threading
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from data_sources.base import BaseDataSource
from storage.file_storage import FileStorageManager
from schedulers.timeframe_handlers import get_handler_for_timeframe
from utils.market_hours import market_hours, is_market_open_now
from utils.logging_config import get_logger, PerformanceLogger
from config.settings import TIMEFRAME_CONFIGS
from config.schedules import SCHEDULES
from config.symbols import get_symbols

logger = get_logger(__name__)

class DataScheduler:
    """
    Main scheduler for automated data updates
    """
    
    def __init__(self, data_source: BaseDataSource, storage_manager: FileStorageManager,
                 symbol_set: str = 'development'):
        self.data_source = data_source
        self.storage_manager = storage_manager
        self.symbol_set = symbol_set
        self.symbols = get_symbols(symbol_set)
        
        # APScheduler instance
        self.scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
        
        # Track last update times
        self.last_updates: Dict[str, datetime] = {}
        
        # Running flag
        self.is_running = False
        
        logger.info(f"DataScheduler initialized with {len(self.symbols)} symbols from {symbol_set} set")
    
    def start(self):
        """Start the scheduler"""
        try:
            if self.is_running:
                logger.warning("Scheduler is already running")
                return
            
            # Check data source availability
            if not self.data_source.is_available():
                logger.error("Data source is not available")
                return False
            
            # Setup scheduled jobs
            self._setup_scheduled_jobs()
            
            # Start the scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info("Data scheduler started successfully")
            
            # Run initial update if needed
            self._run_initial_update()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {str(e)}")
            return False
    
    def stop(self):
        """Stop the scheduler"""
        try:
            if not self.is_running:
                logger.warning("Scheduler is not running")
                return
            
            self.scheduler.shutdown(wait=True)
            self.is_running = False
            logger.info("Data scheduler stopped")
            
        except Exception as e:
            logger.error(f"Error stopping scheduler: {str(e)}")
    
    def _setup_scheduled_jobs(self):
        """Setup scheduled jobs for different timeframes"""
        
        for timeframe, config in TIMEFRAME_CONFIGS.items():
            schedule_config = SCHEDULES.get(timeframe)
            
            if not schedule_config or not schedule_config.get('enabled', True):
                logger.info(f"Skipping disabled timeframe: {timeframe}")
                continue
            
            # Add job based on timeframe
            if timeframe == '15m':
                # Every 15 minutes during market hours
                self.scheduler.add_job(
                    func=self._update_timeframe,
                    trigger=IntervalTrigger(minutes=15),
                    args=[timeframe],
                    id=f'update_{timeframe}',
                    name=f'Update {timeframe} data',
                    max_instances=1,
                    coalesce=True
                )
                
            elif timeframe == '1h':
                # Every hour during market hours
                self.scheduler.add_job(
                    func=self._update_timeframe,
                    trigger=IntervalTrigger(hours=1),
                    args=[timeframe],
                    id=f'update_{timeframe}',
                    name=f'Update {timeframe} data',
                    max_instances=1,
                    coalesce=True
                )
                
            elif timeframe == '1d':
                # Daily at 4:30 PM IST (after market close)
                self.scheduler.add_job(
                    func=self._update_timeframe,
                    trigger=CronTrigger(hour=16, minute=30, day_of_week='mon-fri'),
                    args=[timeframe],
                    id=f'update_{timeframe}',
                    name=f'Update {timeframe} data',
                    max_instances=1,
                    coalesce=True
                )
                
            elif timeframe == '1wk':
                # Weekly on Saturday at 8 AM
                self.scheduler.add_job(
                    func=self._update_timeframe,
                    trigger=CronTrigger(hour=8, minute=0, day_of_week='sat'),
                    args=[timeframe],
                    id=f'update_{timeframe}',
                    name=f'Update {timeframe} data',
                    max_instances=1,
                    coalesce=True
                )
            
            logger.info(f"Scheduled job for {timeframe}: {schedule_config['description']}")
        
        # Add cleanup job - daily at 2 AM
        self.scheduler.add_job(
            func=self._cleanup_old_files,
            trigger=CronTrigger(hour=2, minute=0),
            id='cleanup_files',
            name='Cleanup old files',
            max_instances=1,
            coalesce=True
        )
    
    def _update_timeframe(self, timeframe: str):
        """
        Update data for a specific timeframe
        
        Args:
            timeframe: Timeframe to update
        """
        try:
            with PerformanceLogger(f"Scheduled update for {timeframe}"):
                # Get appropriate handler
                handler = get_handler_for_timeframe(
                    timeframe, self.data_source, self.storage_manager
                )
                
                # Check if update is needed based on market hours
                config = TIMEFRAME_CONFIGS.get(timeframe, {})
                if config.get('active_during_market_hours_only', False):
                    if not is_market_open_now():
                        logger.info(f"Market closed, skipping {timeframe} update")
                        return
                
                # Perform update
                success = handler.update_data(timeframe, self.symbols)
                
                if success:
                    self.last_updates[timeframe] = datetime.now()
                    logger.info(f"Successfully completed scheduled update for {timeframe}")
                else:
                    logger.error(f"Failed scheduled update for {timeframe}")
                    
        except Exception as e:
            logger.error(f"Error in scheduled update for {timeframe}: {str(e)}")
    
    def _run_initial_update(self):
        """Run initial update for timeframes that have no data"""
        logger.info("Running initial data check...")
        
        for timeframe in TIMEFRAME_CONFIGS.keys():
            try:
                # Check if we have any data for this timeframe
                existing_data = self.storage_manager.load_data(timeframe)
                
                if existing_data.empty:
                    logger.info(f"No existing data for {timeframe}, running initial update")
                    
                    # Run update in background thread to avoid blocking
                    update_thread = threading.Thread(
                        target=self._update_timeframe,
                        args=[timeframe],
                        name=f'InitialUpdate-{timeframe}'
                    )
                    update_thread.daemon = True
                    update_thread.start()
                    
                    # Small delay between timeframes to avoid overwhelming the API
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Error checking initial data for {timeframe}: {str(e)}")
    
    def _cleanup_old_files(self):
        """Cleanup old backup files"""
        try:
            logger.info("Running file cleanup...")
            self.storage_manager.cleanup_old_files(days_to_keep=30)
            logger.info("File cleanup completed")
        except Exception as e:
            logger.error(f"Error during file cleanup: {str(e)}")
    
    def manual_update(self, timeframe: str = None, symbols: List[str] = None) -> bool:
        """
        Manually trigger an update
        
        Args:
            timeframe: Specific timeframe to update (None for all)
            symbols: Specific symbols to update (None for all configured symbols)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            update_symbols = symbols or self.symbols
            timeframes_to_update = [timeframe] if timeframe else list(TIMEFRAME_CONFIGS.keys())
            
            logger.info(f"Manual update requested for timeframes: {timeframes_to_update}")
            
            success_count = 0
            for tf in timeframes_to_update:
                handler = get_handler_for_timeframe(
                    tf, self.data_source, self.storage_manager
                )
                
                if handler.update_data(tf, update_symbols):
                    success_count += 1
                    self.last_updates[tf] = datetime.now()
                    logger.info(f"Manual update successful for {tf}")
                else:
                    logger.error(f"Manual update failed for {tf}")
            
            return success_count == len(timeframes_to_update)
            
        except Exception as e:
            logger.error(f"Error in manual update: {str(e)}")
            return False
    
    def get_status(self) -> Dict:
        """
        Get scheduler status and statistics
        
        Returns:
            Dictionary with status information
        """
        status = {
            'is_running': self.is_running,
            'data_source': self.data_source.source_name,
            'symbols_count': len(self.symbols),
            'symbol_set': self.symbol_set,
            'market_status': market_hours.get_market_status(),
            'last_updates': self.last_updates.copy(),
            'scheduled_jobs': []
        }
        
        if self.is_running:
            for job in self.scheduler.get_jobs():
                status['scheduled_jobs'].append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None
                })
        
        # Add data summary
        try:
            status['data_summary'] = self.storage_manager.get_data_summary()
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}")
            status['data_summary'] = {}
        
        return status
    
    def update_symbol_set(self, symbol_set: str):
        """
        Update the symbol set being tracked
        
        Args:
            symbol_set: New symbol set identifier
        """
        try:
            new_symbols = get_symbols(symbol_set)
            self.symbols = new_symbols
            self.symbol_set = symbol_set
            logger.info(f"Updated symbol set to {symbol_set} with {len(new_symbols)} symbols")
        except Exception as e:
            logger.error(f"Error updating symbol set: {str(e)}")

# Convenience function to create and start a scheduler
def create_scheduler(data_source: BaseDataSource, storage_manager: FileStorageManager,
                   symbol_set: str = 'development', start_immediately: bool = True) -> DataScheduler:
    """
    Create and optionally start a data scheduler
    
    Args:
        data_source: Data source instance
        storage_manager: Storage manager instance
        symbol_set: Symbol set to use
        start_immediately: Whether to start the scheduler immediately
    
    Returns:
        DataScheduler instance
    """
    scheduler = DataScheduler(data_source, storage_manager, symbol_set)
    
    if start_immediately:
        scheduler.start()
    
    return scheduler 