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
        
        # Define collection prefixes (folders)
        self.collection_prefixes = {
            'stock_data': 'stock_data.',
            'economic_indicators': 'economic_indicators.',
            'reddit_data': 'reddit_data.',
            'twitter_data': 'twitter_data.',
            'news_data': 'news_data.'
        }
    
    def store_stock_data(self, ticker: str, data: pd.DataFrame):
        # Use the stock_data folder prefix
        collection_name = f"{self.collection_prefixes['stock_data']}{ticker.lower()}"
        collection = self.db[collection_name]
        
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
            print(f"Inserted {len(records)} new records into MongoDB for {ticker} in stock_data folder")
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
                print(f"MongoDB upsert results for {ticker} in stock_data folder: {result.upserted_count} inserted, {result.modified_count} modified")
        
        # Verify data was stored correctly
        stored_count = collection.count_documents({})
        
        return stored_count
    
    def get_stock_data(self, ticker: str) -> pd.DataFrame:
        collection_name = f"{self.collection_prefixes['stock_data']}{ticker.lower()}"
        collection = self.db[collection_name]
        data = list(collection.find({}, {'_id': 0}).sort('date', 1))
        return pd.DataFrame(data)
    
    def store_economic_data(self, ticker: str, data: pd.DataFrame):
        # Store economic indicators data
        collection_name = f"{self.collection_prefixes['economic_indicators']}{ticker.lower()}"
        collection = self.db[collection_name]
        
        # Convert DataFrame to list of dictionaries
        records = data.to_dict('records')
        
        # Convert date strings to datetime objects
        for record in records:
            if isinstance(record.get('date'), str):
                record['date'] = datetime.strptime(record['date'], '%Y-%m-%d')
        
        # Create index on date field
        collection.create_index('date')
        
        # Use upsert approach
        bulk_operations = []
        for record in records:
            bulk_operations.append(
                UpdateOne(
                    {'date': record['date'], 'indicator': record.get('indicator')},
                    {'$set': record},
                    upsert=True
                )
            )
        
        if bulk_operations:
            result = collection.bulk_write(bulk_operations)
            print(f"Economic data stored: {result.upserted_count} inserted, {result.modified_count} modified")
        
        return collection.count_documents({})
    
    def store_reddit_data(self, ticker: str, data_list):
        # Store Reddit data
        collection_name = f"{self.collection_prefixes['reddit_data']}{ticker.lower()}"
        collection = self.db[collection_name]
        
        # Create compound index on permalink to ensure uniqueness
        collection.create_index('permalink', unique=True)
        
        # Use upsert for each post
        bulk_operations = []
        for post in data_list:
            bulk_operations.append(
                UpdateOne(
                    {'permalink': post['permalink']},
                    {'$set': post},
                    upsert=True
                )
            )
        
        if bulk_operations:
            result = collection.bulk_write(bulk_operations)
            print(f"Reddit data stored for {ticker}: {result.upserted_count} inserted, {result.modified_count} modified")
        
        return collection.count_documents({})
    
    def store_twitter_data(self, ticker: str, data_list):
        # Store Twitter data
        collection_name = f"{self.collection_prefixes['twitter_data']}{ticker.lower()}"
        collection = self.db[collection_name]
        
        # Create index on created_at and author_id
        collection.create_index([('author_id', 1), ('created_at', 1)])
        
        # Use upsert for each tweet
        bulk_operations = []
        for tweet in data_list:
            # Ensure tweet has an identifier
            if 'id' not in tweet and 'tweet_id' not in tweet:
                # Use author_id + created_at as a composite key if no ID
                bulk_operations.append(
                    UpdateOne(
                        {'author_id': tweet['author_id'], 'created_at': tweet['created_at']},
                        {'$set': tweet},
                        upsert=True
                    )
                )
            else:
                tweet_id = tweet.get('id') or tweet.get('tweet_id')
                bulk_operations.append(
                    UpdateOne(
                        {'id': tweet_id},
                        {'$set': tweet},
                        upsert=True
                    )
                )
        
        if bulk_operations:
            result = collection.bulk_write(bulk_operations)
            print(f"Twitter data stored for {ticker}: {result.upserted_count} inserted, {result.modified_count} modified")
        
        return collection.count_documents({})
    
    def store_news_data(self, ticker: str, data_list):
        # Store news data
        collection_name = f"{self.collection_prefixes['news_data']}{ticker.lower()}"
        collection = self.db[collection_name]
        
        # Create index on published_at and url
        collection.create_index([('publishedAt', 1), ('url', 1)])
        
        # Use upsert for each article
        bulk_operations = []
        for article in data_list:
            bulk_operations.append(
                UpdateOne(
                    {'url': article['url']},
                    {'$set': article},
                    upsert=True
                )
            )
        
        if bulk_operations:
            result = collection.bulk_write(bulk_operations)
            print(f"News data stored for {ticker}: {result.upserted_count} inserted, {result.modified_count} modified")
        
        return collection.count_documents({})
