import os
import json
import time
import logging
import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
from config import (
    MONGODB_CONNECTION_STRING,
    MONGODB_DATABASE_NAME,
    MONGODB_COLLECTION_NAME
)

# Configure logging
logger = logging.getLogger(__name__)

# Singleton pattern for database connection
class CosmosDBManager:
    _instance = None
    _client = None
    _db = None
    _collection = None
    
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
            # Initialize MongoDB connection
            self._client = MongoClient(MONGODB_CONNECTION_STRING)
            self._db = self._client[MONGODB_DATABASE_NAME]
            self._collection = self._db[MONGODB_COLLECTION_NAME]
            
            # Create indexes if needed
            self._collection.create_index("request_id", unique=True)
            self._collection.create_index("user_email")
            self._collection.create_index("status")
            self._collection.create_index("created_at")
            self._collection.create_index("request_type")
            
            logger.info(f"MongoDB connection initialized successfully to database: {MONGODB_DATABASE_NAME}")
            
        except Exception as e:
            logger.error(f"Error initializing MongoDB connection: {str(e)}")
            raise
    
    def get_collection(self):
        """Get the MongoDB collection"""
        return self._collection
    
    def close(self):
        """Close the MongoDB connection"""
        if self._client:
            self._client.close()

# Request data access functions
def store_request(request_id, status, request_type, user_email=None, result=None, assistant_id=None, thread_id=None):
    """
    Store a new request in the database
    """
    try:
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
        
    except Exception as e:
        logger.error(f"Error storing request {request_id}: {str(e)}")
        raise

def update_request_status(request_id, status, result=None):
    """
    Update the status and result of an existing request
    """
    try:
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
        
    except Exception as e:
        logger.error(f"Error updating request status for {request_id}: {str(e)}")
        raise

def get_request_status(request_id):
    """
    Get the status and result of a request
    """
    try:
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
            
    except Exception as e:
        logger.error(f"Error retrieving request status for {request_id}: {str(e)}")
        return None

def get_user_requests(user_email, limit=20):
    """
    Get recent requests for a specific user
    """
    try:
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
        
    except Exception as e:
        logger.error(f"Error retrieving requests for user {user_email}: {str(e)}")
        return []

def cleanup_old_requests(days=7):
    """
    Delete requests older than specified days
    """
    try:
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
            
    except Exception as e:
        logger.error(f"Error cleaning up old requests: {str(e)}")
        return []