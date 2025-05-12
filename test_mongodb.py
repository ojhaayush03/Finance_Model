import sys
import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Add project root to path to import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import get_config

def test_mongodb_connection():
    """Test MongoDB connection and check stored data"""
    # Get MongoDB connection details from config
    mongodb_uri = get_config('MONGODB_URI', 'mongodb://localhost:27017/')
    db_name = get_config('MONGODB_DB_NAME', 'financial_db')
    
    print(f"Connecting to MongoDB at: {mongodb_uri}")
    print(f"Database name: {db_name}")
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongodb_uri)
        
        # Check if server is available
        server_info = client.server_info()
        print(f"✅ Connected to MongoDB server version: {server_info.get('version', 'unknown')}")
        
        # Get database
        db = client[db_name]
        
        # List all collections
        collections = db.list_collection_names()
        print(f"Collections in database: {len(collections)}")
        for i, collection_name in enumerate(collections, 1):
            print(f"  {i}. {collection_name}")
            
            # Count documents in collection
            count = db[collection_name].count_documents({})
            print(f"     Documents: {count}")
            
            # Show sample document (first one)
            if count > 0:
                sample = db[collection_name].find_one({})
                print(f"     Sample document keys: {list(sample.keys())}")
                
                # If it's a stock collection, show some stats
                if "_stock_data" in collection_name:
                    # Get date range
                    oldest = db[collection_name].find_one({}, sort=[("date", 1)])
                    newest = db[collection_name].find_one({}, sort=[("date", -1)])
                    
                    if oldest and newest and "date" in oldest and "date" in newest:
                        oldest_date = oldest["date"]
                        newest_date = newest["date"]
                        print(f"     Date range: {oldest_date} to {newest_date}")
                        print(f"     Data span: {(newest_date - oldest_date).days} days")
        
        return True
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {str(e)}")
        return False

def insert_test_data():
    """Insert test data into MongoDB to verify it's working"""
    # Get MongoDB connection details from config
    mongodb_uri = get_config('MONGODB_URI', 'mongodb://localhost:27017/')
    db_name = get_config('MONGODB_DB_NAME', 'financial_db')
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        
        # Create test collection
        test_collection = db["test_collection"]
        
        # Create test document
        test_document = {
            "test_id": "test_1",
            "timestamp": datetime.now(),
            "message": "This is a test document to verify MongoDB is working"
        }
        
        # Insert test document
        result = test_collection.insert_one(test_document)
        
        if result.acknowledged:
            print(f"✅ Test document inserted with ID: {result.inserted_id}")
            return True
        else:
            print("❌ Failed to insert test document")
            return False
    except Exception as e:
        print(f"❌ Error inserting test data: {str(e)}")
        return False

if __name__ == "__main__":
    print("Testing MongoDB connection and data...")
    connection_success = test_mongodb_connection()
    
    if connection_success:
        print("\nInserting test data to verify write operations...")
        insert_success = insert_test_data()
        
        if insert_success:
            print("\nVerifying test data was inserted...")
            test_mongodb_connection()
    
    print("\nMongoDB test completed.")
