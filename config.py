import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Service Bus settings
AZURE_SERVICE_BUS_CONNECTION_STRING = os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING")
AZURE_SERVICE_BUS_QUEUE_NAME = os.getenv("AZURE_SERVICE_BUS_QUEUE_NAME", "nl2sql-requests")

# Azure OpenAI settings
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_API_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
AZURE_OPENAI_MODEL_NAME = os.getenv("AZURE_OPENAI_MODEL_NAME", "gpt-4o-mini")
AZURE_OPENAI_EMBEDDING_MODEL_NAME = os.getenv("AZURE_OPENAI_EMBEDDING_MODEL_NAME", "textembedding-test-exquitech")

# MongoDB (Cosmos DB) settings
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE_NAME = os.getenv("MONGODB_DATABASE_NAME", "insightshq-db")
MONGODB_COLLECTION_NAME = os.getenv("MONGODB_COLLECTION_NAME", "requests")

# Azure Postgres settings (not used in current implementation but kept for future use)
AZURE_POSTGRES_SERVER = os.getenv("AZURE_POSTGRES_SERVER", "")
AZURE_POSTGRES_DATABASE = os.getenv("AZURE_POSTGRES_DATABASE", "")
AZURE_POSTGRES_USER = os.getenv("AZURE_POSTGRES_USER", "")
AZURE_POSTGRES_PASSWORD = os.getenv("AZURE_POSTGRES_PASSWORD", "")

# Queue processor settings
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
MAX_MESSAGE_COUNT = int(os.getenv("MAX_MESSAGE_COUNT", "10"))
MAX_WAIT_TIME = int(os.getenv("MAX_WAIT_TIME", "5"))
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "7"))
CLEANUP_INTERVAL_HOURS = int(os.getenv("CLEANUP_INTERVAL_HOURS", "1"))

# Assistant settings
ASSISTANT_POOL_SIZE = int(os.getenv("ASSISTANT_POOL_SIZE", "5"))
THREAD_LIFETIME_HOURS = int(os.getenv("THREAD_LIFETIME_HOURS", "24"))

# Database type for the assistant
DATABASE_TYPE = os.getenv("DATABASE_TYPE", "fabric")

# Validate required settings
def validate_config():
    """Validate that all required configuration settings are present."""
    required_vars = [
        "AZURE_SERVICE_BUS_CONNECTION_STRING",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_KEY",
        "AZURE_OPENAI_EMBEDDING_MODEL_NAME",
        "AZURE_OPENAI_MODEL_NAME",
        "AZURE_OPENAI_API_VERSION",
        "AZURE_SERVICE_BUS_QUEUE_NAME",
        "MONGODB_DATABASE_NAME",
        "MONGODB_COLLECTION_NAME",
        "MONGODB_CONNECTION_STRING"
    ]
    
    missing_vars = [var for var in required_vars if not globals().get(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return True

# Path configurations
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)