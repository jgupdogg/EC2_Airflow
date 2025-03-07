import os
import logging
import logging.handlers
import sys
from datetime import datetime

def setup_logging(log_level=logging.INFO, log_file=None, debug_mode=False):
    """
    Configure logging for the application.
    
    Args:
        log_level: Overall logging level (default: INFO)
        log_file: Optional path to log file
        debug_mode: Enable debug mode with more detailed logging
    """
    # Set default log level
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if debug_mode else log_level)
    
    # Remove existing handlers to avoid duplicate logging
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter with time, level, logger name, and message
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if debug_mode:
        # Add function name and line number in debug mode
        log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    
    formatter = logging.Formatter(log_format)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG if debug_mode else log_level)
    root_logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Use rotating file handler to manage log file size
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG if debug_mode else log_level)
        root_logger.addHandler(file_handler)
    
    # Set specific module log levels
    if debug_mode:
        # More verbose for core modules
        logging.getLogger('core').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)
    else:
        # Less verbose for external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('psycopg2').setLevel(logging.WARNING)
    
    # Log initial message
    logging.info(f"Logging initialized at level: {'DEBUG' if debug_mode else logging.getLevelName(log_level)}")
    if log_file:
        logging.info(f"Log file: {log_file}")

def generate_log_file_name():
    """Generate a log file name with timestamp."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"logs/govt_scraper_{timestamp}.log"