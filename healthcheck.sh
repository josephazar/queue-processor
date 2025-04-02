#!/bin/bash

# This script checks if the processor is running and performing its basic functions
# Exit 0 = success (healthy)
# Exit 1 = failure (unhealthy)

# Check if the main process is running
if ! pgrep -f "python processor.py" > /dev/null; then
    echo "UNHEALTHY: Processor process not found"
    exit 1
fi

# # Check if the logs directory is accessible
# if [ ! -d "/app/logs" ]; then
#     echo "UNHEALTHY: Logs directory not accessible"
#     exit 1
# fi

# # Check if the log file exists and has been updated in the last 5 minutes
# LOG_FILE="/app/logs/nl2sql_processor.log"
# if [ ! -f "$LOG_FILE" ]; then
#     echo "UNHEALTHY: Log file does not exist"
#     exit 1
# fi

# # Check if log file was updated in the last 10 minutes
# LOG_TIMESTAMP=$(stat -c %Y "$LOG_FILE")
# CURRENT_TIMESTAMP=$(date +%s)
# ELAPSED=$((CURRENT_TIMESTAMP - LOG_TIMESTAMP))

# if [ $ELAPSED -gt 600 ]; then
#     echo "UNHEALTHY: Log file not updated in the last 10 minutes"
#     exit 1
# fi

# If we get here, the health check passes
echo "HEALTHY: Processor is running normally"
exit 0