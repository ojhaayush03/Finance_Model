"""
Script to verify MongoDB Atlas connection and data storage
"""

from pymongo import MongoClient
import sys
import os

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from config import get_config

def verify_atlas_connection():
    """Connect to MongoDB Atlas and verify data storage"""
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
        print(f"\nFound {len(collection_names)} collections:")
        
        # Group collections by folder/prefix
        folders = {
            'stock_data': [],
            'economic_indicators': [],
            'reddit_data': [],
            'twitter_data': [],
            'news_data': [],
            'other': []
        }
        
        # Sort collections into folders
        for name in collection_names:
            count = db[name].count_documents({})
            
            # Determine which folder this collection belongs to
            folder = 'other'
            for prefix in folders.keys():
                if name.startswith(prefix + '.'):
                    folder = prefix
                    break
            
            # Add to appropriate folder list
            folders[folder].append((name, count))
        
        # Print collections organized by folder
        for folder, collections in folders.items():
            if collections:
                print(f"\n{folder.upper()} folder ({len(collections)} collections):")
                for name, count in collections:
                    ticker = name.split('.')[-1] if '.' in name else name
                    print(f"  - {name}: {count} documents (Ticker: {ticker})")
            else:
                if folder != 'other':
                    print(f"\n{folder.upper()} folder: No collections")
        
        # Print ungrouped collections
        if folders['other']:
            print(f"\nUNGROUPED collections ({len(folders['other'])} collections):")
            for name, count in folders['other']:
                print(f"  - {name}: {count} documents")
        
        # Show sample data from each collection (limited to first 3 collections)
        print("\nSample data from collections:")
        for name in collection_names[:3]:
            print(f"\nCollection: {name}")
            docs = list(db[name].find().limit(2))
            
            if not docs:
                print("  No documents found")
                continue
                
            for doc in docs:
                # Remove _id field for cleaner output
                doc.pop('_id', None)
                print(f"  {doc}")
                
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to MongoDB Atlas: {str(e)}")
        return False

if __name__ == "__main__":
    verify_atlas_connection()
