#!/bin/bash

# This script checks if the processor is running and performing its basic functions
# Exit 0 = success (healthy)
# Exit 1 = failure (unhealthy)

# Check if the main process is running
# Use 'ps' instead of 'pgrep' for better container compatibility
if ! ps aux | grep "python[3]* processor.py" > /dev/null; then
    echo "UNHEALTHY: Processor process not found"
    exit 1
fi

# Check if a log file exists and has been updated recently
LOG_FILE="/app/logs/nl2sql_processor.log"
if [ -f "$LOG_FILE" ]; then
    # Get the timestamp of the last modification
    LOG_MTIME=$(stat -c %Y "$LOG_FILE" 2>/dev/null || stat -f %m "$LOG_FILE" 2>/dev/null)
    CURRENT_TIME=$(date +%s)
    
    # If log file exists but is more than 10 minutes old, that's suspicious
    if [ $((CURRENT_TIME - LOG_MTIME)) -gt 600 ]; then
        echo "WARNING: Log file exists but hasn't been updated in 10 minutes"
        # Don't fail just for this, but log the warning
    fi
fi

# If we get here, the health check passes
echo "HEALTHY: Processor is running normally"
exit 0