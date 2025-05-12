"""
Test script to verify API keys and connections
"""

import logging
from config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('api_tester')

def test_fred_api():
    """Test FRED API connection"""
    try:
        from fredapi import Fred
        
        api_key = get_config('FRED_API_KEY')
        logger.info(f"Testing FRED API with key: {api_key[:5]}...")
        
        fred = Fred(api_key=api_key)
        
        # Try to fetch GDP data
        data = fred.get_series('GDP')
        
        if data is not None and not data.empty:
            logger.info(f"✅ FRED API test successful! Retrieved {len(data)} data points.")
            return True
        else:
            logger.error("❌ FRED API returned empty data.")
            return False
    except Exception as e:
        logger.error(f"❌ FRED API test failed: {str(e)}")
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

def test_reddit_api():
    """Test Reddit API connection"""
    try:
        import praw
        
        client_id = get_config('REDDIT_CLIENT_ID')
        client_secret = get_config('REDDIT_CLIENT_SECRET')
        user_agent = get_config('REDDIT_USER_AGENT')
        
        logger.info(f"Testing Reddit API with client ID: {client_id}...")
        
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        
        # Try to fetch some posts from r/stocks
        subreddit = reddit.subreddit('stocks')
        posts = list(subreddit.hot(limit=5))
        
        if posts:
            logger.info(f"✅ Reddit API test successful! Retrieved {len(posts)} posts.")
            return True
        else:
            logger.error("❌ Reddit API returned no data.")
            return False
    except Exception as e:
        logger.error(f"❌ Reddit API test failed: {str(e)}")
        return False

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
    print("\n🔍 Testing API connections...\n")
    
    # Test Alpha Vantage API
    test_alpha_vantage_api()
    
    # Test FRED API
    test_fred_api()
    
    # Test Twitter API
    test_twitter_api()
    
    # Test Reddit API
    test_reddit_api()
    
    # Test MongoDB connection
    test_mongodb_connection()
    
    print("\n✅ API testing completed!")
