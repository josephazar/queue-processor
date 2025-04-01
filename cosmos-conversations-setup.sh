#!/bin/bash

# Login to Azure (uncomment if not already logged in)
# az login

# Set variables
RESOURCE_GROUP="exquitech-ai-rg"
COSMOS_ACCOUNT_NAME="insightshq-cosmos-db"
DATABASE_NAME="insightshq-db"
COLLECTION_NAME="conversations"

# Check if the collection already exists
COLLECTION_EXISTS=$(az cosmosdb mongodb collection show \
    --account-name $COSMOS_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --database-name $DATABASE_NAME \
    --name $COLLECTION_NAME 2>/dev/null)

if [ -z "$COLLECTION_EXISTS" ]; then
    echo "Creating new 'conversations' collection..."
    
    # Create collection
    az cosmosdb mongodb collection create \
        --account-name $COSMOS_ACCOUNT_NAME \
        --resource-group $RESOURCE_GROUP \
        --database-name $DATABASE_NAME \
        --name $COLLECTION_NAME \
        --shard "request_id"  # Partition key
    
    echo "Collection '$COLLECTION_NAME' created successfully."
else
    echo "Collection '$COLLECTION_NAME' already exists."
fi


echo "Setup completed successfully!"