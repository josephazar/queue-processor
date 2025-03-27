#!/bin/bash

# Login to Azure (uncomment if not already logged in)
# az login

#!/bin/bash

# Login to Azure (uncomment if not already logged in)
# az login

# Set variables
RESOURCE_GROUP="exquitech-ai-rg"
LOCATION="uaenorth"
COSMOS_ACCOUNT_NAME="insightshq-cosmos-db"  # Must be globally unique
DATABASE_NAME="insightshq-db"
COLLECTION_NAME="requests"


# Create Cosmos DB account with MongoDB API in serverless mode (cheapest option)
echo "Creating Cosmos DB account with MongoDB API (serverless)..."
az cosmosdb create \
    --name $COSMOS_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --kind MongoDB \
    --server-version "4.0" \
    --capabilities EnableServerless \
    --default-consistency-level Eventual \
    --locations regionName=$LOCATION

# Rest of the script remains the same
# Create database
echo "Creating database..."
az cosmosdb mongodb database create \
    --account-name $COSMOS_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --name $DATABASE_NAME

# Create collection (equivalent to MongoDB collection)
echo "Creating collection..."
az cosmosdb mongodb collection create \
    --account-name $COSMOS_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --database-name $DATABASE_NAME \
    --name $COLLECTION_NAME \
    --shard "request_id"  # Partition key

# Get connection string
echo "Retrieving connection string..."
CONNECTION_STRING=$(az cosmosdb keys list \
    --name $COSMOS_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --type connection-strings \
    --query "connectionStrings[?description=='Primary MongoDB Connection String'].connectionString" \
    --output tsv)

echo "MongoDB Connection String: $CONNECTION_STRING"
echo "Please save this connection string safely!"
