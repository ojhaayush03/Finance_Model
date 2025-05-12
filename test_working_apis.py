"""
Simplified test script to verify working API keys
"""

import logging
from config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('api_tester')

def test_alpha_vantage_api():
    """Test Alpha Vantage API connection"""
    try:
        from alpha_vantage.timeseries import TimeSeries
        
        api_key = get_config('ALPHA_VANTAGE_API_KEY')
        logger.info(f"Testing Alpha Vantage API with key: {api_key[:5]}...")
        
        ts = TimeSeries(key=api_key, output_format='pandas')
        data, _ = ts.get_daily(symbol='MSFT', outputsize='compact')
        
        if data is not None and not data.empty:
            logger.info(f"✅ Alpha Vantage API test successful! Retrieved {len(data)} data points.")
            return True
        else:
            logger.error("❌ Alpha Vantage API returned empty data.")
            return False
    except Exception as e:
        logger.error(f"❌ Alpha Vantage API test failed: {str(e)}")
        return False

def test_twitter_api():
    """Test Twitter/X API connection"""
    try:
        from twitter_api_client import Client
        
        bearer_token = get_config('TWITTER_BEARER_TOKEN')
        logger.info(f"Testing Twitter API with token: {bearer_token[:5]}...")
        
        client = Client(bearer_token=bearer_token)
        
        # Try to fetch some tweets
        response = client.tweets.search_recent(
            query="stock market",
            max_results=10
        )
        
        if hasattr(response, 'data') and response.data:
            logger.info(f"✅ Twitter API test successful! Retrieved {len(response.data)} tweets.")
            return True
        else:
            logger.error("❌ Twitter API returned no data.")
            return False
    except Exception as e:
        logger.error(f"❌ Twitter API test failed: {str(e)}")
        return False

def test_mongodb_connection():
    """Test MongoDB connection"""
    try:
        from pymongo import MongoClient
        
        mongodb_uri = get_config('MONGODB_URI')
        db_name = get_config('MONGODB_DB_NAME')
        
        logger.info(f"Testing MongoDB connection to {mongodb_uri}...")
        
        client = MongoClient(mongodb_uri)
        db = client[db_name]
        
        # Try to list collections
        collections = db.list_collection_names()
        
        logger.info(f"✅ MongoDB connection successful! Found {len(collections)} collections.")
        return True
    except Exception as e:
        logger.error(f"❌ MongoDB connection test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("\n🔍 Testing working API connections...\n")
    
    # Test Alpha Vantage API
    alpha_result = test_alpha_vantage_api()
    
    # Test Twitter API
    twitter_result = test_twitter_api()
    
    # Test MongoDB connection
    mongo_result = test_mongodb_connection()
    
    # Summary
    print("\n📊 API Testing Summary:")
    print(f"Alpha Vantage API: {'✅ Working' if alpha_result else '❌ Failed'}")
    print(f"Twitter API: {'✅ Working' if twitter_result else '❌ Failed'}")
    print(f"MongoDB: {'✅ Working' if mongo_result else '❌ Failed'}")
    
    print("\n✅ API testing completed!")
