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

# Ensure healthcheck.sh is executable
chmod +x healthcheck.sh

# Build and push the Docker image to ACR
echo "Building and pushing Docker image to ACR..."
az acr build --registry $ACR_NAME --image $IMAGE_NAME:$IMAGE_TAG .

# Load .env variables
ENV_FILE=".env"
ENV_VARS_FORMATTED=()

while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi
    
    # Extract key and value
    if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
        key="${BASH_REMATCH[1]}"
        value="${BASH_REMATCH[2]}"
        
        # Remove any surrounding quotes from the value
        value="${value%\"}"
        value="${value#\"}"
        value="${value%\'}"
        value="${value#\'}"
        
        # Trim any trailing whitespace
        value=$(echo "$value" | sed 's/[[:space:]]*$//')
        
        echo "Processing: key='$key', value='$value'"  # Debug output
        ENV_VARS_FORMATTED+=("$key=$value")
        echo "Added: $key=$value"  # Debug output
    else
        echo "Skipped line: $line"  # Debug output
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

# Create the container instance with volume mount and health probes
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
    --azure-file-volume-mount-path "/app/logs" \
    --dns-name-label $CONTAINER_NAME-dns 
    # --liveness-probe-command "/app/healthcheck.sh" \
    # --liveness-probe-interval 30 \
    # --liveness-probe-failure-threshold 3 \
    # --liveness-probe-initial-delay 30

echo "Container deployment completed!"
echo "Container logs: az container logs --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME"
echo "Application logs: Available in Azure Storage File Share '$STORAGE_SHARE_NAME' in account '$STORAGE_ACCOUNT_NAME'"
echo "Container health status: az container show --resource-group $RESOURCE_GROUP --name $CONTAINER_NAME --query containers[0].instanceView.currentState.state"