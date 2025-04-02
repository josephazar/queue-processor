#!/bin/bash
# Script to check and restart the container if needed

RESOURCE_GROUP="exquitech-ai-rg"
CONTAINER_NAME="insightshq-agent-processor"

# Check container status
CONTAINER_STATE=$(az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --query containers[0].instanceView.currentState.state -o tsv)
echo "Container state: $CONTAINER_STATE"

if [ "$CONTAINER_STATE" != "Running" ]; then
    echo "Container is not running. Current state: $CONTAINER_STATE"
    echo "Restarting container..."
    az container restart --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME
    
    # Wait a bit and check again
    sleep 30
    NEW_STATE=$(az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --query containers[0].instanceView.currentState.state -o tsv)
    echo "Container state after restart: $NEW_STATE"
else
    echo "Container is running normally."
fi

# Get recent logs
echo "Recent container logs:"
az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --tail 20