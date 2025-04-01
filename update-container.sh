#!/bin/bash
set -e

# Configuration (copy from your existing script)
RESOURCE_GROUP="exquitech-ai-rg"
CONTAINER_NAME="insightshq-agent-processor"
IMAGE_NAME="insightshq-agent-processor"
IMAGE_TAG="latest"
CPU="2"
MEMORY="4"
ACR_NAME="insightshqagentqueueacr"
STORAGE_ACCOUNT_NAME="insightshqlogs"
STORAGE_SHARE_NAME="nl2sql-logs"

# Get necessary values that were calculated in previous steps
LOGIN_SERVER=$(az acr show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query loginServer -o tsv)
REGISTRY_USERNAME=$(az acr credential show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query username -o tsv)
REGISTRY_PASSWORD=$(az acr credential show --name $ACR_NAME --resource-group $RESOURCE_GROUP --query "passwords[0].value" -o tsv)
STORAGE_KEY=$(az storage account keys list --account-name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --query "[0].value" -o tsv)

# Echo registry username and password
echo "Registry username: $REGISTRY_USERNAME"
echo "Registry password: $REGISTRY_PASSWORD"

# Build and push the Docker image to ACR
echo "Building and pushing Docker image to ACR..."
az acr build --registry $ACR_NAME --image $IMAGE_NAME:$IMAGE_TAG .


# Load .env variables
ENV_FILE=".env"
ENV_VARS_FORMATTED=()
while IFS='=' read -r key value; do
    # Clean up key and value (remove whitespace and carriage returns)
    key=$(echo "$key" | sed 's/^[ \t]*//;s/[ \t\r]*$//')
    echo "Raw value: '$value'"
    value=$(echo "$value" | sed 's/^[ \t]*//;s/[ \t\r]*$//')
    echo "Processed value: '$value'"
    echo "Processing: key='$key', value='$value'"  # Debug output
    
    # Hardcode the embedding model name variable if this is that key
    if [[ "$key" == "AZURE_OPENAI_EMBEDDING_MODEL_NAME" ]]; then
        value="textembedding-test-exquitech"
        echo "Overriding embedding model name to: $value"
    fi
    
    if [[ -n "$key" && ! "$key" =~ ^# && "$key" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
        ENV_VARS_FORMATTED+=("$key=$value")
        echo "Added: $key=$value"  # Debug output
    else
        echo "Skipped: $key"  # Debug output
    fi
done < "$ENV_FILE"

# Print the array contents
echo "Environment variables:"
for var in "${ENV_VARS_FORMATTED[@]}"; do
    echo "$var"
done


# Check if the container group exists and delete it if it does
if az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --query id --output tsv > /dev/null 2>&1; then
    echo "Container group exists, deleting..."
    az container delete --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --yes
fi


# Create or update the container instance
echo "Creating/updating Azure Container Instance..."
az container create \
    --resource-group $RESOURCE_GROUP \
    --name $CONTAINER_NAME \
    --image $LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG \
    --os-type Linux \
    --registry-login-server $LOGIN_SERVER \
    --registry-username $REGISTRY_USERNAME \
    --registry-password $REGISTRY_PASSWORD \
    --environment-variables "${ENV_VARS_FORMATTED[@]}" \
    --cpu $CPU \
    --memory $MEMORY \
    --restart-policy Always \
    --azure-file-volume-account-name $STORAGE_ACCOUNT_NAME \
    --azure-file-volume-account-key "$STORAGE_KEY" \
    --azure-file-volume-share-name $STORAGE_SHARE_NAME \
    --azure-file-volume-mount-path "/app/logs"

echo "Container deployment completed!"