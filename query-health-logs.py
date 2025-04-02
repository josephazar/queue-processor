#!/usr/bin/env python3
"""
Script to query container health logs from Cosmos DB
"""
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Query container health logs from Cosmos DB')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back (default: 24)')
    parser.add_argument('--container', help='Filter by container ID')
    parser.add_argument('--error-type', help='Filter by error type')
    parser.add_argument('--limit', type=int, default=50, help='Maximum number of logs to display (default: 50)')
    parser.add_argument('--output', choices=['console', 'json'], default='console', help='Output format (default: console)')
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    # Get Cosmos DB connection string
    conn_string = os.getenv('MONGODB_CONNECTION_STRING')
    if not conn_string:
        print("Error: MONGODB_CONNECTION_STRING environment variable not set")
        sys.exit(1)
    
    db_name = os.getenv('MONGODB_DATABASE_NAME', 'insightshq-db')
    
    try:
        # Connect to Cosmos DB
        client = MongoClient(conn_string)
        db = client[db_name]
        collection = db["container_health"]
        
        # Calculate timestamp for hours back
        hours_back = datetime.now() - timedelta(hours=args.hours)
        timestamp = int(hours_back.timestamp())
        
        # Build query
        query = {"timestamp": {"$gte": timestamp}}
        if args.container:
            query["container_id"] = args.container
        if args.error_type:
            query["error_type"] = args.error_type
        
        # Execute query
        cursor = collection.find(query).sort("timestamp", -1).limit(args.limit)
        results = list(cursor)
        
        # Convert ObjectId to string for JSON serialization
        for doc in results:
            doc["_id"] = str(doc["_id"])
        
        # Output results
        if args.output == 'json':
            print(json.dumps(results, indent=2))
        else:
            print(f"Found {len(results)} health log entries from the past {args.hours} hours:\n")
            for doc in results:
                timestamp = datetime.fromtimestamp(doc["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                print(f"[{timestamp}] {doc['error_type']} - Container: {doc['container_id']}")
                print(f"  Details: {doc['details']}")
                print("-" * 80)
        
        # Summary
        error_types = {}
        for doc in results:
            error_type = doc["error_type"]
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        print("\nSummary:")
        for error_type, count in error_types.items():
            print(f"  {error_type}: {count} occurrences")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()