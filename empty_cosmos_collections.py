#!/usr/bin/env python3
"""
Script to empty Cosmos DB collections: requests, conversations, and container_health
"""
import os
from pymongo import MongoClient
from dotenv import load_dotenv

def empty_collections():
    """Empty all three collections in Cosmos DB"""
    # Load environment variables from .env file
    load_dotenv()
    
    # Get connection string and database name from environment
    conn_string = os.getenv('MONGODB_CONNECTION_STRING')
    db_name = os.getenv('MONGODB_DATABASE_NAME', 'insightshq-db')
    
    if not conn_string:
        print("Error: MONGODB_CONNECTION_STRING environment variable not set")
        return
    
    try:
        # Connect to MongoDB/Cosmos DB
        client = MongoClient(conn_string)
        db = client[db_name]
        
        # List of collections to empty
        collections = ['requests', 'conversations', 'container_health']
        
        for collection_name in collections:
            collection = db[collection_name]
            
            # Count documents before deletion
            count_before = collection.count_documents({})
            
            # Delete all documents
            result = collection.delete_many({})
            
            print(f"Collection '{collection_name}': Deleted {result.deleted_count} of {count_before} documents")
        
        print("All collections emptied successfully!")
        
    except Exception as e:
        print(f"Error emptying collections: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()
            print("Connection closed")

if __name__ == "__main__":
    empty_collections()