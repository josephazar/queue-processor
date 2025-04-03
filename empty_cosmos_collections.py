#!/usr/bin/env python3
"""
Script to empty Cosmos DB collections: requests, conversations, and container_health
"""
import os
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from pymongo.errors import BulkWriteError, OperationFailure


def empty_collections():
    """Empty all three collections in Cosmos DB using batched deletions"""
    # Load environment variables from .env file
    load_dotenv()
    
    # Get connection string and database name from environment
    conn_string = os.getenv('MONGODB_CONNECTION_STRING')
    db_name = os.getenv('MONGODB_DATABASE_NAME', 'insightshq-db')
    
    if not conn_string:
        print("Error: MONGODB_CONNECTION_STRING environment variable not set")
        return
    
    # Define batch size - keep this small for Cosmos DB
    BATCH_SIZE = 50
    
    try:
        # Connect to MongoDB/Cosmos DB
        client = MongoClient(conn_string)
        db = client[db_name]
        
        # List of collections to empty
        collections = ['requests', 'conversations', 'container_health']
        
        for collection_name in collections:
            print(f"\nProcessing collection: {collection_name}")
            collection = db[collection_name]
            
            # Count documents
            try:
                total_count = collection.count_documents({})
                print(f"Found {total_count} documents in '{collection_name}'")
                
                if total_count == 0:
                    print("No documents to delete")
                    continue
                
                deleted_total = 0
                
                # Continue deleting in batches until collection is empty
                while True:
                    # Find IDs for a batch of documents
                    cursor = collection.find({}, {'_id': 1}).limit(BATCH_SIZE)
                    ids = [doc['_id'] for doc in cursor]
                    
                    if not ids:
                        print("No more documents to delete")
                        break
                    
                    print(f"Deleting batch of {len(ids)} documents...")
                    
                    try:
                        # Delete the batch
                        result = collection.delete_many({'_id': {'$in': ids}})
                        deleted_count = result.deleted_count
                        deleted_total += deleted_count
                        print(f"Successfully deleted {deleted_count} documents")
                        
                        # Sleep to avoid rate limiting
                        time.sleep(0.5)
                        
                    except (OperationFailure, BulkWriteError) as e:
                        print(f"Error in batch: {str(e)}")
                        
                        # If we hit an error, try a smaller batch size
                        if len(ids) > 10:
                            print("Reducing batch size and retrying...")
                            # Try with smaller batches
                            for i in range(0, len(ids), 10):
                                small_batch = ids[i:i+10]
                                try:
                                    result = collection.delete_many({'_id': {'$in': small_batch}})
                                    deleted_count = result.deleted_count
                                    deleted_total += deleted_count
                                    print(f"Successfully deleted {deleted_count} documents (smaller batch)")
                                    time.sleep(1)  # Longer sleep for retry
                                except Exception as inner_e:
                                    print(f"Error with smaller batch: {str(inner_e)}")
                                    time.sleep(2)  # Even longer sleep
                
                print(f"Collection '{collection_name}': Total deleted {deleted_total} of {total_count} documents")
                
            except Exception as e:
                print(f"Error processing collection {collection_name}: {str(e)}")
    
    except Exception as e:
        print(f"Error emptying collections: {str(e)}")
    
    finally:
        if 'client' in locals():
            client.close()
            print("Connection closed")

if __name__ == "__main__":
    empty_collections()