# Deployment Guide for NL2SQL Bot with Azure Service Bus

This guide covers the deployment of the asynchronous architecture for the NL2SQL Bot using Azure Service Bus.

## 1. Azure Resources Setup

### Azure Service Bus

1. **Create Azure Service Bus Namespace**:
   ```bash
   az servicebus namespace create --resource-group <resource-group> \
     --name <namespace-name> \
     --location <location> \
     --sku Standard
   ```

2. **Create Queue**:
   ```bash
   az servicebus queue create --resource-group <resource-group> \
     --namespace-name <namespace-name> \
     --name nl2sql-requests \
     --max-size 1024 \
     --default-message-time-to-live P1D
   ```

3. **Get Connection String**:
   ```bash
   az servicebus namespace authorization-rule keys list \
     --resource-group <resource-group> \
     --namespace-name <namespace-name> \
     --name RootManageSharedAccessKey \
     --query primaryConnectionString \
     --output tsv
   ```

### Database Setup

For development purposes, SQLite is used in the code. For production, set up an Azure SQL Database:

1. **Create Azure SQL Database**:
   ```bash
   az sql server create --resource-group <resource-group> \
     --name <server-name> \
     --admin-user <admin-username> \
     --admin-password <admin-password> \
     --location <location>

   az sql db create --resource-group <resource-group> \
     --server <server-name> \
     --name nl2sql-db \
     --edition Standard \
     --capacity 10
   ```

2. **Get Connection String**:
   Format: `mssql+pyodbc://<username>:<password>@<server-name>.database.windows.net:1433/<database>?driver=ODBC+Driver+17+for+SQL+Server`

## 2. Application Components Deployment

### FastAPI Application

Deploy as an Azure App Service:

1. **Create App Service Plan**:
   ```bash
   az appservice plan create --resource-group <resource-group> \
     --name <appservice-plan-name> \
     --sku B1
   ```

2. **Create Web App**:
   ```bash
   az webapp create --resource-group <resource-group> \
     --plan <appservice-plan-name> \
     --name <app-name> \
     --runtime "PYTHON|3.9"
   ```

3. **Configure Settings**:
   ```bash
   az webapp config appsettings set --resource-group <resource-group> \
     --name <app-name> \
     --settings \
     AZURE_SERVICE_BUS_CONNECTION_STRING="<connection-string>" \
     AZURE_SERVICE_BUS_QUEUE_NAME="nl2sql-requests" \
     DATABASE_CONNECTION_STRING="<db-connection-string>" \
     AZURE_OPENAI_API_ENDPOINT="<openai-endpoint>" \
     AZURE_OPENAI_API_KEY="<openai-key>" \
     AZURE_OPENAI_API_VERSION="<openai-version>"
   ```

4. **Deploy Code**:
   ```bash
   az webapp deployment source config-zip --resource-group <resource-group> \
     --name <app-name> \
     --src <zip-file-path>
   ```

### Queue Processor Service

Deploy as an Azure Container Instance:

1. **Containerize the Queue Processor**:
   Create a Dockerfile:
   ```dockerfile
   FROM python:3.9
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   COPY . .
   CMD ["python", "queue_processor.py"]
   ```

2. **Build and Push to Container Registry**:
   ```bash
   az acr build --registry <acr-name> --image nl2sql-processor:latest .
   ```

3. **Deploy Container Instance**:
   ```bash
   az container create \
     --resource-group <resource-group> \
     --name nl2sql-processor \
     --image <acr-name>.azurecr.io/nl2sql-processor:latest \
     --registry-login-server <acr-name>.azurecr.io \
     --registry-username <acr-username> \
     --registry-password <acr-password> \
     --environment-variables \
       AZURE_SERVICE_BUS_CONNECTION_STRING="<connection-string>" \
       AZURE_SERVICE_BUS_QUEUE_NAME="nl2sql-requests" \
       DATABASE_CONNECTION_STRING="<db-connection-string>" \
       AZURE_OPENAI_API_ENDPOINT="<openai-endpoint>" \
       AZURE_OPENAI_API_KEY="<openai-key>" \
       AZURE_OPENAI_API_VERSION="<openai-version>" \
       MAX_WORKERS="10" \
     --cpu 1 \
     --memory 2
   ```

## 3. Scaling Considerations

### FastAPI App Service

For increased load, scale up your App Service Plan:

```bash
az appservice plan update --resource-group <resource-group> \
  --name <appservice-plan-name> \
  --sku P1V2
```

### Queue Processor

To increase worker capacity, options include:

1. **Vertical Scaling**: Increase CPU/memory resources
   ```bash
   az container update --resource-group <resource-group> \
     --name nl2sql-processor \
     --cpu 2 \
     --memory 4
   ```

2. **Horizontal Scaling**: Deploy multiple container instances
   - Each instance will compete for messages from the queue
   - Use environment variables to set worker count per instance (MAX_WORKERS)

3. **Azure Kubernetes Service (AKS)**: For production-grade scaling
   - Provides better control over scaling policies
   - Ideal for larger deployments with 50+ users

## 4. Monitoring and Troubleshooting

1. **Application Insights Integration**:
   ```bash
   az monitor app-insights component create \
     --app <insights-name> \
     --resource-group <resource-group> \
     --location <location>

   az webapp config appsettings set \
     --resource-group <resource-group> \
     --name <app-name> \
     --settings APPLICATIONINSIGHTS_CONNECTION_STRING="<connection-string>"
   ```

2. **Service Bus Metrics**:
   Monitor queue length and processing rates in Azure Portal.

3. **Container Logs**:
   ```bash
   az container logs --resource-group <resource-group> \
     --name nl2sql-processor
   ```

## 5. Security Best Practices

1. **Use Managed Identity** for accessing Service Bus and databases
2. **Store secrets in Azure Key Vault** rather than environment variables
3. **Enable SSL/TLS** for all communication
4. **Implement rate limiting** to prevent abuse

## 6. Maintenance Tasks

Create a maintenance script to run periodically:

```python
from database import cleanup_old_requests

# Clean up requests older than 7 days
cleanup_old_requests(days=7)
```

Deploy this as an Azure Function with a timer trigger to run daily.