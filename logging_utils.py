import os
import logging
import sys
import json
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import time

class FileHandlerWithFlush(RotatingFileHandler):
    """File handler that flushes after each log write"""
    
    def emit(self, record):
        super().emit(record)
        self.flush()

def verify_logging_paths():
    """Verify the logs directory is properly set and has write permissions"""
    import os
    
    # Get the absolute path to the logs directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    log_file = os.path.join(logs_dir, "nl2sql_processor.log")
    
    # Ensure directory exists
    os.makedirs(logs_dir, exist_ok=True)
    
    # Check if directory is writable
    if not os.access(logs_dir, os.W_OK):
        print(f"ERROR: No write access to logs directory: {logs_dir}")
        return False
    
    # Try creating a test file
    try:
        test_file = os.path.join(logs_dir, "test_log.txt")
        with open(test_file, 'w') as f:
            f.write("Test logging")
        os.remove(test_file)
        print(f"Successfully wrote to logs directory: {logs_dir}")
        return True
    except Exception as e:
        print(f"ERROR writing to logs directory: {str(e)}")
        return False

def cleanup_old_logs(log_dir, max_days=30):
    """Delete log files older than max_days"""
    import os
    import time
    
    try:
        now = time.time()
        count = 0
        for file in os.listdir(log_dir):
            if file.endswith(".log") or file.endswith(".log.gz"):
                file_path = os.path.join(log_dir, file)
                if os.stat(file_path).st_mtime < now - max_days * 86400:
                    os.remove(file_path)
                    count += 1
        print(f"Cleaned up {count} old log files")
        return count
    except Exception as e:
        print(f"Error cleaning up logs: {str(e)}")
        return 0

def init_logging():
    """Initialize logging with both console and file handlers and rotation"""
    import os
    import logging
    import sys
    from pathlib import Path
    
    # Create logs directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    
    log_file = os.path.join(logs_dir, "nl2sql_processor.log")
    
    # Get logger
    logger = logging.getLogger("nl2sql_processor")
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Add file handler with rotation
    try:
        # Use rotating file handler with 100MB max size, keeping 10 backup files
        file_handler = FileHandlerWithFlush(
            log_file,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=10,
            delay=False
        )
        
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        logger.info(f"File logging initialized to {log_file} with rotation")
    except Exception as e:
        print(f"Failed to set up file logging: {str(e)}")
        logger.error(f"Failed to set up file logging: {str(e)}")
        
    return logger


def setup_logging(log_dir, log_name, log_level=logging.INFO, max_size_mb=100, backup_count=5):
    """
    Set up logging with proper error handling and log rotation
    
    Args:
        log_dir (str): Directory path where logs should be stored
        log_name (str): Base name for the log file
        log_level (int): Logging level
        max_size_mb (int): Maximum size of log file in MB before rotation
        backup_count (int): Number of backup files to keep
        
    Returns:
        logger: Configured logger object
    """
    # Create a logger
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    
    # Clear existing handlers to prevent duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Always ensure we have a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Try to set up file logging with rotation
    try:
        # Ensure directory exists
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"{log_name}.log")
        
        # Check if path is writable
        if os.access(log_dir, os.W_OK) or not os.path.exists(log_dir):
            # Size-based rotation - rotate when log reaches max_size_mb
            size_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_size_mb * 1024 * 1024,  # Convert MB to bytes
                backupCount=backup_count,
                delay=True  # Only open file when first record is emitted
            )
            
            # Daily rotation at midnight
            time_handler = TimedRotatingFileHandler(
                log_file,
                when='midnight',
                interval=1,
                backupCount=30,  # Keep a month of daily logs
                delay=True
            )
            
            # Choose which rotation strategy to use (size-based is usually better for active services)
            file_handler = size_handler  # You could use time_handler instead
            
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            logger.info(f"File logging with rotation initialized to {log_file}")
        else:
            logger.warning(f"Cannot write to log directory: {log_dir}. File logging disabled.")
    except Exception as e:
        logger.warning(f"Failed to initialize file logging: {str(e)}. Will log to console only.")
    
    return logger



        
def verify_logging_paths():
    """Verify the logs directory is properly set and has write permissions"""
    import os
    
    # Get the absolute path to the logs directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    log_file = os.path.join(logs_dir, "nl2sql_processor.log")
    
    # Ensure directory exists
    os.makedirs(logs_dir, exist_ok=True)
    
    # Check if directory is writable
    if not os.access(logs_dir, os.W_OK):
        print(f"ERROR: No write access to logs directory: {logs_dir}")
        return False
    
    # Try creating a test file
    try:
        test_file = os.path.join(logs_dir, "test_log.txt")
        with open(test_file, 'w') as f:
            f.write("Test logging")
        os.remove(test_file)
        print(f"Successfully wrote to logs directory: {logs_dir}")
        return True
    except Exception as e:
        print(f"ERROR writing to logs directory: {str(e)}")
        return False

class ContextLogger(logging.Logger):
    """Extended Logger class that supports structured logging with context"""
    
    def log_with_context(self, message, level=logging.INFO, exc_info=False, **context):
        """Log a message with additional context as structured data"""
        if context:
            structured_message = f"{message} | Context: {json.dumps(context)}"
        else:
            structured_message = message
        self.log(level, structured_message, exc_info=exc_info)
    
    def error_with_context(self, message, exc_info=True, **context):
        """Log an error message with additional context"""
        self.log_with_context(message, level=logging.ERROR, exc_info=exc_info, **context)
    
    def warning_with_context(self, message, exc_info=False, **context):
        """Log a warning message with additional context"""
        self.log_with_context(message, level=logging.WARNING, exc_info=exc_info, **context)
    
    def info_with_context(self, message, **context):
        """Log an info message with additional context"""
        self.log_with_context(message, level=logging.INFO, **context)
    
    def debug_with_context(self, message, **context):
        """Log a debug message with additional context"""
        self.log_with_context(message, level=logging.DEBUG, **context)

# Register our custom logger class
logging.setLoggerClass(ContextLogger)

def setup_logging(log_dir, log_name, log_level=logging.INFO):
    """
    Set up logging with proper error handling
    
    Args:
        log_dir (str): Directory path where logs should be stored
        log_name (str): Base name for the log file
        log_level (int): Logging level
        
    Returns:
        logger: Configured logger object
    """
    # Create a logger
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    
    # Clear existing handlers to prevent duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Always ensure we have a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Try to set up file logging
    try:
        # Ensure directory exists
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"{log_name}.log")
        
        # Check if path is writable
        if os.access(log_dir, os.W_OK) or not os.path.exists(log_dir):
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            logger.info(f"File logging initialized to {log_file}")
        else:
            logger.warning(f"Cannot write to log directory: {log_dir}. File logging disabled.")
    except Exception as e:
        logger.warning(f"Failed to initialize file logging: {str(e)}. Will log to console only.")
    
    return logger