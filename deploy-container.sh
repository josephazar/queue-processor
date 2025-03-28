#!/bin/bash
set -e

# Configuration
RESOURCE_GROUP="exquitech-ai-rg"
LOCATION="uaenorth"
ACR_NAME="insightshq-queue-acr"  # Must be globally unique
CONTAINER_NAME="nl2sql-processor"
IMAGE_NAME="nl2sql-processor"
IMAGE_TAG="latest"

# Container instance settings
CPU="2"
MEMORY="4"
REGISTRY_USERNAME=""  # Will be set later
REGISTRY_PASSWORD=""  # Will be set later

# Create logs storage
STORAGE_ACCOUNT_NAME="insightshqlogs"
STORAGE_SHARE_NAME="nl2sql-logs"

# Environment variables from .env file
ENV_FILE=".env"

echo "Loading environment variables from $ENV_FILE..."
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "Error: $ENV_FILE not found"
    exit 1
fi

# Check if resource group exists, create if not
echo "Checking resource group..."
az group show --name $RESOURCE_GROUP &>/dev/null || \
    az group create --name $RESOURCE_GROUP --location $LOCATION

# Create Azure Container Registry if it doesn't exist
echo "Creating Azure Container Registry..."
az acr show --name $ACR_NAME --resource-group $RESOURCE_GROUP &>/dev/null || \
    az acr create --resource-group $RESOURCE_GROUP --name $ACR_NAME --sku Standard --admin-enabled true

# Get ACR credentials
echo "Getting ACR credentials..."
REGISTRY_USERNAME=$(az acr credential show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query username -o tsv)
REGISTRY_PASSWORD=$(az acr credential show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query "passwords[0].value" -o tsv)
LOGIN_SERVER=$(az acr show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query loginServer -o tsv)

# Build and push the Docker image to ACR
echo "Building and pushing Docker image to ACR..."
az acr build --registry $ACR_NAME --image $IMAGE_NAME:$IMAGE_TAG .

# Create a storage account for logs if it doesn't exist
echo "Setting up storage for logs..."
az storage account show --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP &>/dev/null || \
    az storage account create --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --location $LOCATION --sku Standard_LRS

# Get storage account key
STORAGE_KEY=$(az storage account keys list --account-name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --query "[0].value" -o tsv)

# Create a file share for logs if it doesn't exist
az storage share exists --name $STORAGE_SHARE_NAME --account-name $STORAGE_ACCOUNT_NAME --account-key "$STORAGE_KEY" | grep -q "exists.*true" || \
    az storage share create --name $STORAGE_SHARE_NAME --account-name $STORAGE_ACCOUNT_NAME --account-key "$STORAGE_KEY"

# Create an array of environment variables for the container from .env file
ENV_VARS=(
    "AZURE_SERVICE_BUS_CONNECTION_STRING=$AZURE_SERVICE_BUS_CONNECTION_STRING"
    "AZURE_SERVICE_BUS_QUEUE_NAME=$AZURE_SERVICE_BUS_QUEUE_NAME"
    "AZURE_OPENAI_ENDPOINT=$AZURE_OPENAI_API_ENDPOINT"
    "AZURE_OPENAI_KEY=$AZURE_OPENAI_API_KEY"
    "AZURE_OPENAI_API_VERSION=$AZURE_OPENAI_API_VERSION"
    "AZURE_OPENAI_MODEL_NAME=$AZURE_OPENAI_MODEL_NAME"
    "AZURE_OPENAI_EMBEDDING_MODEL_NAME=$AZURE_OPENAI_EMBEDDING_MODEL_NAME"
    "MONGODB_CONNECTION_STRING=$MONGODB_CONNECTION_STRING"
    "MONGODB_DATABASE_NAME=$MONGODB_DATABASE_NAME"
    "MONGODB_COLLECTION_NAME=$MONGODB_COLLECTION_NAME"
    "AZURE_OPENAI_API_DEPLOYMENT=$AZURE_OPENAI_API_DEPLOYMENT"
    "AZURE_TENANT_ID=$AZURE_TENANT_ID"
    "AZURE_CLIENT_ID=$AZURE_CLIENT_ID"
    "AZURE_CLIENT_SECRET=$AZURE_CLIENT_SECRET"
    "AZURE_FABRIC_SQL_SERVER=$AZURE_FABRIC_SQL_SERVER"
    "AZURE_FABRIC_SQL_WAREHOUSE=$AZURE_FABRIC_SQL_WAREHOUSE"
    "DATABASE_TYPE=$DB_TYPE"
    "ENVIRONMENT=production"
    "MAX_WORKERS=20"
)

# Convert ENV_VARS array to string for az CLI command
ENV_STRING=""
for var in "${ENV_VARS[@]}"; do
    ENV_STRING="$ENV_STRING --environment-variables $var"
done

# Create the container instance with volume mount
echo "Creating Azure Container Instance..."
az container create \
    --resource-group $RESOURCE_GROUP \
    --name $CONTAINER_NAME \
    --image $LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG \
    --registry-login-server $LOGIN_SERVER \
    --registry-username $REGISTRY_USERNAME \
    --registry-password $REGISTRY_PASSWORD \
    $ENV_STRING \
    --cpu $CPU \
    --memory $MEMORY \
    --restart-policy Always \
    --location $LOCATION \
    --azure-file-volume-account-name $STORAGE_ACCOUNT_NAME \
    --azure-file-volume-account-key "$STORAGE_KEY" \
    --azure-file-volume-share-name $STORAGE_SHARE_NAME \
    --azure-file-volume-mount-path "/app/logs"

echo "Deployment completed successfully!"
echo "Container logs: az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME"
echo "Application logs: Available in Azure Storage File Share '$STORAGE_SHARE_NAME' in account '$STORAGE_ACCOUNT_NAME'"