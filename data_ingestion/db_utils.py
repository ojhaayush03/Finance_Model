from pymongo import MongoClient, UpdateOne
import pandas as pd
from datetime import datetime
import sys
import os

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  
from config import get_config

class MongoDBClient:
    def __init__(self):
        # Get MongoDB connection details from config
        mongodb_uri = get_config('MONGODB_URI', 'mongodb://localhost:27017/')
        db_name = get_config('MONGODB_DB_NAME', 'financial_db')
        
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[db_name]
    
    def store_stock_data(self, ticker: str, data: pd.DataFrame):
        collection = self.db[f'{ticker.lower()}_stock_data']
        
        # Convert DataFrame to list of dictionaries
        records = data.to_dict('records')
        
        # Convert date strings to datetime objects and prepare for upsert
        for record in records:
            if isinstance(record['date'], str):
                record['date'] = datetime.strptime(record['date'], '%Y-%m-%d')
        
        # Create index on date field
        collection.create_index('date')
        
        # Use upsert approach (update if exists, insert if not)
        # First, check if we should replace or append
        existing_count = collection.count_documents({})
        
        if existing_count == 0:
            # If no existing data, just insert all records
            collection.insert_many(records)
            print(f"Inserted {len(records)} new records into MongoDB for {ticker}")
        else:
            # If data exists, use bulk upsert to update existing records and add new ones
            bulk_operations = []
            for record in records:
                # Use date as the unique identifier
                bulk_operations.append(
                    UpdateOne(
                        {'date': record['date']},  # Query to find the document
                        {'$set': record},          # Update operation
                        upsert=True                # Create if doesn't exist
                    )
                )
            
            if bulk_operations:
                result = collection.bulk_write(bulk_operations)
                print(f"MongoDB upsert results for {ticker}: {result.upserted_count} inserted, {result.modified_count} modified")
        
        # Verify data was stored correctly
        stored_count = collection.count_documents({})
        
        return stored_count
    
    def get_stock_data(self, ticker: str) -> pd.DataFrame:
        collection = self.db[f'{ticker.lower()}_stock_data']
        data = list(collection.find({}, {'_id': 0}).sort('date', 1))
        return pd.DataFrame(data)
