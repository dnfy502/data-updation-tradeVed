"""
File-based storage for market data (CSV files)
"""
import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Optional, List
import logging

from config.settings import DATA_STORAGE_PATH, CSV_FILE_PREFIX

logger = logging.getLogger(__name__)

class FileStorageManager:
    """
    Manages CSV file storage for market data
    """
    
    def __init__(self, base_path: str = None):
        self.base_path = base_path or DATA_STORAGE_PATH
        self.ensure_directory_exists()
    
    def ensure_directory_exists(self):
        """Create data directory if it doesn't exist"""
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
            logger.info(f"Created data directory: {self.base_path}")
    
    def get_filename(self, timeframe: str, date_suffix: bool = True) -> str:
        """
        Generate filename for a timeframe
        
        Args:
            timeframe: '15m', '1h', '1d', '1wk'
            date_suffix: Whether to add date suffix to filename
        
        Returns:
            Complete filename with path
        """
        if date_suffix:
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"{CSV_FILE_PREFIX}_{timeframe}_{date_str}.csv"
        else:
            filename = f"{CSV_FILE_PREFIX}_{timeframe}.csv"
        
        return os.path.join(self.base_path, filename)
    
    def save_data(self, data: pd.DataFrame, timeframe: str, append: bool = False) -> bool:
        """
        Save DataFrame to CSV file
        
        Args:
            data: DataFrame to save
            timeframe: Timeframe identifier
            append: Whether to append to existing file or overwrite
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if data.empty:
                logger.warning(f"No data to save for timeframe {timeframe}")
                return False
            
            filename = self.get_filename(timeframe, date_suffix=False)
            
            if append and os.path.exists(filename):
                # Load existing data and merge
                existing_data = self.load_data(timeframe)
                if not existing_data.empty:
                    # Combine and remove duplicates
                    combined_data = pd.concat([existing_data, data], ignore_index=True)
                    combined_data = self.remove_duplicates(combined_data)
                    data = combined_data
            
            # Sort by datetime and symbol for better organization
            if 'Datetime' in data.columns:
                data = data.sort_values(['Symbol', 'Datetime']).reset_index(drop=True)
            
            data.to_csv(filename, index=False)
            logger.info(f"Saved {len(data)} records to {filename}")
            
            # Also save with date suffix for backup
            backup_filename = self.get_filename(timeframe, date_suffix=True)
            data.to_csv(backup_filename, index=False)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving data for {timeframe}: {str(e)}")
            return False
    
    def load_data(self, timeframe: str, symbol_filter: List[str] = None) -> pd.DataFrame:
        """
        Load DataFrame from CSV file
        
        Args:
            timeframe: Timeframe identifier
            symbol_filter: Optional list of symbols to filter
        
        Returns:
            DataFrame with loaded data
        """
        try:
            filename = self.get_filename(timeframe, date_suffix=False)
            
            if not os.path.exists(filename):
                logger.warning(f"File not found: {filename}")
                return pd.DataFrame()
            
            data = pd.read_csv(filename)
            
            # Convert datetime column
            if 'Datetime' in data.columns:
                data['Datetime'] = pd.to_datetime(data['Datetime'])
            
            # Apply symbol filter if provided
            if symbol_filter and 'Symbol' in data.columns:
                data = data[data['Symbol'].isin(symbol_filter)]
            
            logger.info(f"Loaded {len(data)} records from {filename}")
            return data
            
        except Exception as e:
            logger.error(f"Error loading data for {timeframe}: {str(e)}")
            return pd.DataFrame()
    
    def remove_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate records based on Symbol and Datetime
        
        Args:
            data: DataFrame to deduplicate
        
        Returns:
            Deduplicated DataFrame
        """
        if data.empty:
            return data
        
        initial_count = len(data)
        
        # Remove duplicates based on Symbol and Datetime
        if 'Symbol' in data.columns and 'Datetime' in data.columns:
            data = data.drop_duplicates(subset=['Symbol', 'Datetime'], keep='last')
        
        final_count = len(data)
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicate records")
        
        return data
    
    def get_latest_data_time(self, timeframe: str, symbol: str = None) -> Optional[datetime]:
        """
        Get the latest data timestamp for a timeframe/symbol
        
        Args:
            timeframe: Timeframe identifier
            symbol: Optional symbol to check (if None, checks all symbols)
        
        Returns:
            Latest datetime or None if no data
        """
        try:
            data = self.load_data(timeframe)
            if data.empty:
                return None
            
            if symbol and 'Symbol' in data.columns:
                symbol_data = data[data['Symbol'] == symbol.replace('.NS', '')]
                if symbol_data.empty:
                    return None
                data = symbol_data
            
            if 'Datetime' in data.columns:
                return data['Datetime'].max()
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest data time for {timeframe}: {str(e)}")
            return None
    
    def cleanup_old_files(self, days_to_keep: int = 30):
        """
        Clean up old backup files (with date suffix)
        
        Args:
            days_to_keep: Number of days of backup files to keep
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            for filename in os.listdir(self.base_path):
                if filename.startswith(CSV_FILE_PREFIX) and '_' in filename:
                    try:
                        # Extract date from filename
                        parts = filename.split('_')
                        if len(parts) >= 3:
                            date_part = parts[-1].replace('.csv', '')
                            file_date = datetime.strptime(date_part, '%Y%m%d')
                            
                            if file_date < cutoff_date:
                                file_path = os.path.join(self.base_path, filename)
                                os.remove(file_path)
                                logger.info(f"Removed old backup file: {filename}")
                    except (ValueError, IndexError):
                        # Skip files that don't match expected format
                        continue
                        
        except Exception as e:
            logger.error(f"Error cleaning up old files: {str(e)}")
    
    def get_data_summary(self) -> dict:
        """
        Get summary of all stored data files
        
        Returns:
            Dictionary with file information
        """
        summary = {}
        
        try:
            for filename in os.listdir(self.base_path):
                if filename.startswith(CSV_FILE_PREFIX) and filename.endswith('.csv'):
                    file_path = os.path.join(self.base_path, filename)
                    
                    # Get file info
                    file_stats = os.stat(file_path)
                    file_size = file_stats.st_size
                    file_modified = datetime.fromtimestamp(file_stats.st_mtime)
                    
                    # Get data info
                    try:
                        data = pd.read_csv(file_path)
                        record_count = len(data)
                        symbols = data['Symbol'].nunique() if 'Symbol' in data.columns else 0
                    except:
                        record_count = 0
                        symbols = 0
                    
                    summary[filename] = {
                        'size_bytes': file_size,
                        'size_mb': round(file_size / (1024 * 1024), 2),
                        'modified': file_modified,
                        'records': record_count,
                        'symbols': symbols
                    }
                    
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}")
        
        return summary 