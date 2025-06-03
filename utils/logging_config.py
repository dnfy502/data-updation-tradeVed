"""
Logging configuration for the data fetching system
"""
import logging
import logging.handlers
import os
from datetime import datetime

from config.settings import LOG_LEVEL, LOG_FORMAT, LOG_FILE

def setup_logging(log_level: str = None, log_file: str = None, console_output: bool = True):
    """
    Setup logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Log file path
        console_output: Whether to output logs to console
    """
    level = log_level or LOG_LEVEL
    log_filename = log_file or LOG_FILE
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_filename) if os.path.dirname(log_filename) else 'logs'
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)
    
    # Setup file handler with rotation
    if log_filename:
        file_handler = logging.handlers.RotatingFileHandler(
            log_filename,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Setup console handler
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Set root logger level
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Log startup message
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized - Level: {level}, File: {log_filename}")

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name (usually __name__)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(name)

def log_performance(func):
    """
    Decorator to log function performance
    
    Args:
        func: Function to decorate
    
    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_time = datetime.now()
        
        try:
            result = func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"{func.__name__} completed in {duration:.2f} seconds")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"{func.__name__} failed after {duration:.2f} seconds: {str(e)}")
            raise
    
    return wrapper

class PerformanceLogger:
    """
    Context manager for logging performance of code blocks
    """
    
    def __init__(self, operation_name: str, logger: logging.Logger = None):
        self.operation_name = operation_name
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(f"Completed {self.operation_name} in {duration:.2f} seconds")
        else:
            self.logger.error(f"Failed {self.operation_name} after {duration:.2f} seconds: {str(exc_val)}")

# Example usage:
"""
# Setup logging at application start
setup_logging()

# Use logger in modules
logger = get_logger(__name__)
logger.info("This is an info message")

# Use performance decorator
@log_performance
def my_function():
    # Function code here
    pass

# Use performance context manager
with PerformanceLogger("Data fetching operation"):
    # Code to monitor
    pass
""" 