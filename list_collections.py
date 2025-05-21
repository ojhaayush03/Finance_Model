"""
Simple script to list MongoDB collections by folder structure
"""

from pymongo import MongoClient
import sys
import os

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from config import get_config

def list_collections():
    """Connect to MongoDB Atlas and list collections by folder"""
    # Get connection details from config
    mongodb_uri = get_config('MONGODB_URI')
    db_name = get_config('MONGODB_DB_NAME')
    
    print(f"Connecting to MongoDB at: {mongodb_uri}")
    print(f"Database name: {db_name}")
    
    try:
        # Create a MongoDB client
        client = MongoClient(mongodb_uri)
        
        # Check connection by getting server info
        server_info = client.server_info()
        print(f"✅ Connected to MongoDB Atlas! Version: {server_info.get('version', 'Unknown')}")
        
        # Get the database
        db = client[db_name]
        
        # List all collections
        collection_names = db.list_collection_names()
        print(f"\nFound {len(collection_names)} collections in total")
        
        # Define folders we're looking for
        folders = {
            'stock_data.': [],
            'economic_indicators.': [],
            'reddit_data.': [],
            'twitter_data.': [],
            'news_data.': [],
            'other': []
        }
        
        # Group collections by folder
        for name in collection_names:
            added = False
            for prefix in list(folders.keys())[:-1]:  # All except 'other'
                if name.startswith(prefix):
                    folders[prefix].append(name)
                    added = True
                    break
            
            if not added:
                folders['other'].append(name)
        
        # Print results by folder
        print("\n=== COLLECTIONS BY FOLDER ===")
        for folder, collections in folders.items():
            folder_name = folder.strip('.') if folder != 'other' else 'UNGROUPED'
            if collections:
                print(f"\n{folder_name.upper()} ({len(collections)} collections):")
                for name in sorted(collections):
                    count = db[name].count_documents({})
                    if folder != 'other':
                        ticker = name.replace(folder, '')
                        print(f"  - {name}: {count} documents (Ticker: {ticker})")
                    else:
                        print(f"  - {name}: {count} documents")
            else:
                print(f"\n{folder_name.upper()}: No collections")
                
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to MongoDB Atlas: {str(e)}")
        return False

if __name__ == "__main__":
    list_collections()
