import os
import logging
import sys
import json
from pathlib import Path

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