import os
from dotenv import load_dotenv
import json
load_dotenv(override=True)


class PGConfig:
    def __init__(self):
        load_dotenv(override=True)
        self.db_params = {
            "dbname": os.getenv("AZURE_POSTGRES_DATABASE"),
            "user": f"{os.getenv('AZURE_POSTGRES_USER')}",
            "password": os.getenv("AZURE_POSTGRES_PASSWORD"),
            "host": f"{os.getenv('AZURE_POSTGRES_SERVER')}"
            + ".postgres.database.azure.com",
            "port": 5432,
            "sslmode": "require",
        }
        self.verbose = True

        
class FabricConfig:
    def __init__(self, datasource_id):
        """
        Loads connection details from `datasources.json` based on the given `datasource_id`.
        """
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
        print(f"Base directory: {base_dir}")
        datasources_file = os.path.join(base_dir, "nl2sql/datasources.json")

        if not os.path.exists(datasources_file):
            print(f"Error: datasources.json not found at {datasources_file}")
            # Fallback to default values if file doesn't exist
            self.tenant_id = os.getenv("AZURE_TENANT_ID")
            self.client_id = os.getenv("AZURE_CLIENT_ID")
            self.server = os.getenv("AZURE_SQL_SERVER")
            self.database = os.getenv("AZURE_SQL_DATABASE")
            self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
            
            # Log values for debugging (mask secrets in production)
            print(f"Using environment variables for database connection:")
            print(f"  Tenant ID: {self.tenant_id}")
            print(f"  Client ID: {self.client_id}")
            print(f"  Server: {self.server}")
            print(f"  Database: {self.database}")
            print(f"  Client Secret: {'*****' if self.client_secret else 'Not set'}")
            
            # Construct the connection string with increased timeout values
            self.connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Authentication=ActiveDirectoryServicePrincipal;"
                f"UID={self.client_id};"
                f"PWD={self.client_secret};"
                f"Authority Id={self.tenant_id};"
                f"Connection Timeout=60;"  # Extended timeout
                f"Command Timeout=60;"     # Extended timeout
            )
            return

        try:
            with open(datasources_file, "r", encoding="utf-8") as file:
                datasources = json.load(file)

            datasource = next((ds for ds in datasources if ds["id"] == datasource_id), None)

            if not datasource:
                print(f"Error: Datasource '{datasource_id}' not found in datasources.json.")
                # Fallback to default values
                self.tenant_id = os.getenv("AZURE_TENANT_ID")
                self.client_id = os.getenv("AZURE_CLIENT_ID")
                self.server = os.getenv("AZURE_SQL_SERVER")
                self.database = os.getenv("AZURE_SQL_DATABASE")
            else:
                # Set values from datasources.json
                self.tenant_id = datasource["tenant_id"]
                self.client_id = datasource["client_id"]
                self.server = datasource["server"]
                self.database = datasource["database"]

            # Retrieve client secret from environment (assumes secret is stored securely)
            self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
            if not self.client_secret:
                print("Warning: AZURE_CLIENT_SECRET environment variable is not set")

            # Construct the connection string with increased timeout values
            self.connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Authentication=ActiveDirectoryServicePrincipal;"
                f"UID={self.client_id};"
                f"PWD={self.client_secret};"
                f"Authority Id={self.tenant_id};"
                f"Connection Timeout=60;"  # Extended timeout
                f"Command Timeout=60;"     # Extended timeout
            )
            
            print(f"Connection string constructed for {self.server}/{self.database}")
            
        except Exception as e:
            print(f"Error loading datasources.json: {e}")
            # Fallback to environment variables
            self.tenant_id = os.getenv("AZURE_TENANT_ID")
            self.client_id = os.getenv("AZURE_CLIENT_ID")
            self.server = os.getenv("AZURE_SQL_SERVER")
            self.database = os.getenv("AZURE_SQL_DATABASE")
            self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
            
            # Construct the connection string with increased timeout values
            self.connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Authentication=ActiveDirectoryServicePrincipal;"
                f"UID={self.client_id};"
                f"PWD={self.client_secret};"
                f"Authority Id={self.tenant_id};"
                f"Connection Timeout=60;"  # Extended timeout
                f"Command Timeout=60;"     # Extended timeout
            )

        self.verbose = True


class BigQueryConfig:
    def __init__(self):
        load_dotenv(override=True)

        # Load BigQuery json directory path from .env file
        secret_name = os.getenv("SERVICE_ACCOUNT_SECRET_NAME")
        if not secret_name:
            raise ValueError(
                "SERVICE_ACCOUNT_SECRET_NAME is not set in the environment variables."
            )

        # Construct the path to the secret file
        project_root = os.path.abspath(os.path.dirname(__file__))
        secret_file_path = os.path.join(
            project_root, "..", "..", "secrets", secret_name
        )

        # Check if the secret file exists
        if not os.path.isfile(secret_file_path):
            raise FileNotFoundError(f"Secret file {secret_file_path} does not exist.")

        self.service_account_json = secret_file_path
        self.dataset_id = os.getenv("BIGQUERY_DATASET_ID")
        self.project_id = json.load(open(self.service_account_json))["project_id"]