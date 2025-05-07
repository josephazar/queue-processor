import os
import json
import time
import logging
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
from datetime import datetime, timedelta
import backoff
from config import (
    MONGODB_CONNECTION_STRING,
    MONGODB_DATABASE_NAME,
    MONGODB_COLLECTION_NAME
)

# Configure logging
logger = logging.getLogger(__name__)

# Singleton pattern for database connection with enhanced connection pooling
class CosmosDBManager:
    _instance = None
    _client = None
    _db = None
    _collection = None
    _conversation_collection = None
    _health_collection = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = CosmosDBManager()
        return cls._instance
    
    def __init__(self):
        if not MONGODB_CONNECTION_STRING:
            logger.error("MONGODB_CONNECTION_STRING environment variable is not set")
            raise ValueError("MONGODB_CONNECTION_STRING environment variable is not set")
        
        try:
            # Initialize MongoDB connection with optimized connection pooling
            self._client = MongoClient(
                MONGODB_CONNECTION_STRING,
                maxPoolSize=50,  # Increase connection pool size for better concurrency
                minPoolSize=10,  # Maintain minimum connections
                maxIdleTimeMS=45000,  # Close idle connections after 45 seconds
                connectTimeoutMS=5000,  # 5 seconds connection timeout
                socketTimeoutMS=30000,  # 30 seconds socket timeout
                serverSelectionTimeoutMS=5000,  # 5 seconds server selection timeout
                retryWrites=False,  # CHANGE: Disable retry for write operations
                w='majority',  # Write concern for data durability
                waitQueueTimeoutMS=5000  # 5 seconds wait queue timeout
            )
            
            # Test connection
            self._client.admin.command('ping')
            
            self._db = self._client[MONGODB_DATABASE_NAME]
            self._collection = self._db[MONGODB_COLLECTION_NAME]
            self._conversation_collection = self._db["conversations"]
            self._health_collection = self._db["container_health"]
            self._assistant_pool_collection = self._db["assistant_pool"]
            
            # Create indexes if needed for requests collection
            self._collection.create_index("request_id", unique=True)
            self._collection.create_index("user_email")
            self._collection.create_index("status")
            self._collection.create_index("created_at")
            self._collection.create_index("request_type")
            
            # Create indexes for conversation history collection
            self._conversation_collection.create_index("request_id")
            self._conversation_collection.create_index("user_email")
            self._conversation_collection.create_index("assistant_id")
            self._conversation_collection.create_index("thread_id")
            self._conversation_collection.create_index("created_at")
            self._conversation_collection.create_index("updated_at")
            self._conversation_collection.create_index("conversation_id")
            
            # Create compound indexes for common query patterns
            self._conversation_collection.create_index([
                ("user_email", pymongo.ASCENDING), 
                ("created_at", pymongo.DESCENDING)
            ])
            self._conversation_collection.create_index([
                ("assistant_id", pymongo.ASCENDING), 
                ("updated_at", pymongo.DESCENDING)
            ])
            
            # Create indexes for health collection
            self._health_collection.create_index("timestamp")
            self._health_collection.create_index("error_type")
            self._health_collection.create_index("container_id")

            self._assistant_pool_collection.create_index("assistant_id", unique=True)

            logger.info(f"MongoDB connection initialized successfully to database: {MONGODB_DATABASE_NAME}")
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error initializing MongoDB connection: {str(e)}")
            raise
    
    def get_collection(self):
        """Get the MongoDB collection for requests"""
        return self._collection
    
    def get_conversation_collection(self):
        """Get the MongoDB collection for conversation history"""
        return self._conversation_collection
    
    def get_health_collection(self):
        """Get the MongoDB collection for container health logs"""
        return self._health_collection
    
    def close(self):
        """Close the MongoDB connection"""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")
            
    def get_assistant_pool_collection(self):
        """Get the MongoDB collection for assistant pool"""
        return self._assistant_pool_collection

# Retry decorator for database operations with exponential backoff
@backoff.on_exception(
    backoff.expo,
    (ConnectionFailure, ServerSelectionTimeoutError, OperationFailure),
    max_tries=5,
    max_time=30,
    on_backoff=lambda details: logger.warning(
        f"Retrying database operation after {details['wait']:.1f}s due to {details['exception']}"
    )
)
def db_operation_with_retry(func):
    """Decorator for database operations with retry logic"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Database connectivity error in {func.__name__}: {str(e)}")
            raise
        except OperationFailure as e:
            logger.error(f"Database operation failed in {func.__name__}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
            raise
    return wrapper

# New function for container health logging
@db_operation_with_retry
def log_container_health_issue(error_type, details):
    """
    Log container health issues directly to Cosmos DB
    
    Args:
        error_type (str): Type of error or health event
        details (str): Details about the error or event
    """
    try:
        collection = CosmosDBManager.get_instance().get_health_collection()
        
        # Format the document
        document = {
            "type": "container_health",
            "error_type": error_type,
            "details": details,
            "timestamp": int(time.time()),
            "container_id": os.environ.get("HOSTNAME", "unknown")
        }
        
        # Insert the document
        collection.insert_one(document)
        
        logger.debug(f"Logged container health issue: {error_type}")
        return True
        
    except Exception as e:
        # Log to console/file if we can't log to DB
        logger.error(f"Failed to log container health issue to Cosmos DB: {str(e)}")
        return False

# Request data access functions
@db_operation_with_retry
def store_request(request_id, status, request_type, user_email=None, result=None, assistant_id=None, thread_id=None):
    """
    Store a new request in the database with retry logic
    """
    collection = CosmosDBManager.get_instance().get_collection()
    
    # Format the document
    now = int(time.time())
    document = {
        "request_id": request_id,
        "status": status,
        "request_type": request_type,
        "user_email": user_email,
        "assistant_id": assistant_id,
        "thread_id": thread_id,
        "created_at": now,
        "updated_at": now
    }
    
    # Add result if provided
    if result:
        if isinstance(result, dict):
            document["result"] = result
        else:
            document["result"] = json.loads(result) if isinstance(result, str) else result
    
    # Insert document with upsert (update if exists, insert if not)
    collection.update_one(
        {"request_id": request_id},
        {"$set": document},
        upsert=True
    )
    
    logger.debug(f"Stored request {request_id} with status {status}")
    return document

@db_operation_with_retry
def update_request_status(request_id, status, result=None):
    """
    Update the status and result of an existing request with retry logic
    """
    collection = CosmosDBManager.get_instance().get_collection()
    
    # Prepare update data
    update_data = {
        "status": status,
        "updated_at": int(time.time())
    }
    
    # Extract assistant_id and thread_id from result if provided
    if result and isinstance(result, dict):
        # Store the entire result object
        update_data["result"] = result
        
        # Also update specific fields if they exist in the result
        if "assistant_id" in result:
            update_data["assistant_id"] = result["assistant_id"]
        if "thread_id" in result:
            update_data["thread_id"] = result["thread_id"]
    
    # Update the document
    collection.update_one(
        {"request_id": request_id},
        {"$set": update_data}
    )
    
    logger.debug(f"Updated status for request {request_id} to {status}")

@db_operation_with_retry
def get_request_status(request_id):
    """
    Get the status and result of a request with retry logic
    """
    collection = CosmosDBManager.get_instance().get_collection()
    
    # Find document
    document = collection.find_one({"request_id": request_id})
    
    if document:
        # Convert MongoDB document to dict and remove _id field
        document = dict(document)
        if "_id" in document:
            del document["_id"]
            
        return document
    else:
        return None

@db_operation_with_retry
def get_user_requests(user_email, limit=20):
    """
    Get recent requests for a specific user with retry logic
    """
    collection = CosmosDBManager.get_instance().get_collection()
    
    # Find documents
    cursor = collection.find(
        {"user_email": user_email}
    ).sort(
        "created_at", pymongo.DESCENDING
    ).limit(limit)
    
    # Convert to list
    documents = []
    for doc in cursor:
        # Convert MongoDB document to dict and remove _id field
        doc = dict(doc)
        if "_id" in doc:
            del doc["_id"]
        documents.append(doc)
        
    return documents

@db_operation_with_retry
def cleanup_old_requests(days=7):
    """
    Delete requests older than specified days with retry logic
    """
    collection = CosmosDBManager.get_instance().get_collection()
    
    # Calculate cutoff timestamp
    cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)
    
    # Find documents to delete
    docs_to_delete = list(collection.find(
        {"created_at": {"$lt": cutoff_timestamp}}
    ))
    
    # Extract request_id and assistant_id for return value
    deleted_items = [
        {"request_id": doc["request_id"], "assistant_id": doc.get("assistant_id")} 
        for doc in docs_to_delete
    ]
    
    # Delete documents
    if deleted_items:
        result = collection.delete_many({"created_at": {"$lt": cutoff_timestamp}})
        logger.info(f"Cleaned up {result.deleted_count} requests older than {days} days")
    else:
        logger.info(f"No requests older than {days} days found for cleanup")
        
    return deleted_items

# Conversation history functions
@db_operation_with_retry
def store_conversation(request_id, question, answer, user_email=None, assistant_id=None, thread_id=None, 
                      report_name=None, request_type=None, conversation_id=None):
    """
    Store a conversation entry in the database with retry logic
    """
    collection = CosmosDBManager.get_instance().get_conversation_collection()
    
    # Format the document
    now = int(time.time())
    document = {
        "request_id": request_id,
        "question": question,
        "answer": answer,
        "user_email": user_email,
        "assistant_id": assistant_id,
        "thread_id": thread_id,
        "report_name": report_name,
        "request_type": request_type,
        "conversation_id": conversation_id,
        "created_at": now,
        "updated_at": now
    }
    
    # Insert the document
    result = collection.insert_one(document)
    
    logger.debug(f"Stored conversation for request {request_id}")
    return str(result.inserted_id)

@db_operation_with_retry
def get_assistant_last_activity(assistant_id):
    """
    Get the timestamp of the last activity for a given assistant with retry logic
    """
    collection = CosmosDBManager.get_instance().get_conversation_collection()
    
    # Find the most recent conversation with this assistant
    result = collection.find_one(
        {"assistant_id": assistant_id},
        sort=[("updated_at", pymongo.DESCENDING)]
    )
    
    if result:
        return result.get("updated_at")
    else:
        return None

@db_operation_with_retry
def get_conversation_history(user_email=None, assistant_id=None, thread_id=None, conversation_id=None, limit=50):
    """
    Get conversation history based on various filter criteria with retry logic
    """
    collection = CosmosDBManager.get_instance().get_conversation_collection()
    
    # Build filter based on provided parameters
    filter_dict = {}
    if user_email:
        filter_dict["user_email"] = user_email
    if assistant_id:
        filter_dict["assistant_id"] = assistant_id
    if thread_id:
        filter_dict["thread_id"] = thread_id
    if conversation_id:
        filter_dict["conversation_id"] = conversation_id
    
    # Find documents
    cursor = collection.find(filter_dict).sort("created_at", pymongo.DESCENDING).limit(limit)
    
    # Convert to list
    documents = []
    for doc in cursor:
        # Convert MongoDB document to dict and convert ObjectId to string
        doc = dict(doc)
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        documents.append(doc)
        
    return documents

@db_operation_with_retry
def cleanup_old_conversations(days=30):
    """
    Delete conversations older than specified days with retry logic
    """
    collection = CosmosDBManager.get_instance().get_conversation_collection()
    
    # Calculate cutoff timestamp
    cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)
    
    # Delete old conversations
    result = collection.delete_many({"created_at": {"$lt": cutoff_timestamp}})
    
    logger.info(f"Cleaned up {result.deleted_count} conversations older than {days} days")
    return result.deleted_count

@db_operation_with_retry
def cleanup_old_health_logs(days=7):
    """
    Delete health logs older than specified days with retry logic
    """
    collection = CosmosDBManager.get_instance().get_health_collection()
    
    # Calculate cutoff timestamp
    cutoff_timestamp = int(time.time()) - (days * 24 * 60 * 60)
    
    # Delete old health logs
    result = collection.delete_many({"timestamp": {"$lt": cutoff_timestamp}})
    
    logger.info(f"Cleaned up {result.deleted_count} health logs older than {days} days")
    return result.deleted_count

# Bulk operations for better performance
@db_operation_with_retry
def bulk_store_conversations(conversations):
    """
    Store multiple conversation entries in a single database operation
    
    Args:
        conversations: List of conversation documents to insert
    
    Returns:
        int: Number of inserted documents
    """
    if not conversations:
        return 0
        
    collection = CosmosDBManager.get_instance().get_conversation_collection()
    result = collection.insert_many(conversations)
    return len(result.inserted_ids)

# Add methods to query container health
@db_operation_with_retry
def get_container_health_history(container_id=None, error_type=None, limit=50):
    """
    Get container health history with optional filters
    
    Args:
        container_id (str, optional): Filter by container ID
        error_type (str, optional): Filter by error type
        limit (int): Maximum number of records to return
        
    Returns:
        list: List of health records
    """
    collection = CosmosDBManager.get_instance().get_health_collection()
    
    # Build filter
    filter_dict = {}
    if container_id:
        filter_dict["container_id"] = container_id
    if error_type:
        filter_dict["error_type"] = error_type
    
    # Find documents
    cursor = collection.find(filter_dict).sort("timestamp", pymongo.DESCENDING).limit(limit)
    
    # Convert to list
    documents = []
    for doc in cursor:
        # Convert MongoDB document to dict and convert ObjectId to string
        doc = dict(doc)
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        documents.append(doc)
        
    return documents

@db_operation_with_retry
def get_pool_assistants():
    """Get assistants from the pool stored in Cosmos DB"""
    try:
        collection = CosmosDBManager.get_instance().get_assistant_pool_collection()
        cursor = collection.find({})
        
        assistants = []
        for doc in cursor:
            assistants.append(doc["assistant_id"])
            
        return assistants
    except Exception as e:
        logger.error(f"Failed to get pool assistants from Cosmos DB: {str(e)}")
        return []

@db_operation_with_retry
def store_pool_assistant(assistant_id):
    """Store an assistant ID in the pool in Cosmos DB"""
    try:
        collection = CosmosDBManager.get_instance().get_assistant_pool_collection()
        
        # Format the document
        now = int(time.time())
        document = {
            "assistant_id": assistant_id,
            "created_at": now,
            "updated_at": now
        }
        
        # Insert the document
        collection.update_one(
            {"assistant_id": assistant_id},
            {"$set": document},
            upsert=True
        )
        
        logger.debug(f"Stored assistant {assistant_id} in pool")
        return True
    except Exception as e:
        logger.error(f"Failed to store pool assistant in Cosmos DB: {str(e)}")
        return False

@db_operation_with_retry
def remove_pool_assistant(assistant_id):
    """Remove an assistant ID from the pool in Cosmos DB"""
    try:
        collection = CosmosDBManager.get_instance().get_assistant_pool_collection()
        collection.delete_one({"assistant_id": assistant_id})
        
        logger.debug(f"Removed assistant {assistant_id} from pool")
        return True
    except Exception as e:
        logger.error(f"Failed to remove pool assistant from Cosmos DB: {str(e)}")
        return False