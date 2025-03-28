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

# Create an array of environment variables for the container
ENV_VARS=(
    "AZURE_SERVICE_BUS_CONNECTION_STRING=$AZURE_SERVICE_BUS_CONNECTION_STRING"
    "AZURE_SERVICE_BUS_QUEUE_NAME=$AZURE_SERVICE_BUS_QUEUE_NAME"
    "AZURE_OPENAI_ENDPOINT=$AZURE_OPENAI_ENDPOINT"
    "AZURE_OPENAI_KEY=$AZURE_OPENAI_KEY"
    "AZURE_OPENAI_API_VERSION=$AZURE_OPENAI_API_VERSION"
    "AZURE_OPENAI_MODEL_NAME=$AZURE_OPENAI_MODEL_NAME"
    "AZURE_OPENAI_EMBEDDING_MODEL_NAME=$AZURE_OPENAI_EMBEDDING_MODEL_NAME"
    "MONGODB_CONNECTION_STRING=$MONGODB_CONNECTION_STRING"
    "MONGODB_DATABASE_NAME=$MONGODB_DATABASE_NAME"
    "MONGODB_COLLECTION_NAME=$MONGODB_COLLECTION_NAME"
    "MAX_WORKERS=20"
    "DATABASE_TYPE=fabric"
)

# Convert ENV_VARS array to string for az CLI command
ENV_STRING=""
for var in "${ENV_VARS[@]}"; do
    ENV_STRING="$ENV_STRING --environment-variables $var"
done

# Create the container instance
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
    --location $LOCATION

echo "Deployment completed successfully!"
echo "Container logs: az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME"