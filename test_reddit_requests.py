"""
Test script for Reddit API using requests approach
"""

import logging
from data_ingestion.reddit_api import RedditAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('reddit_tester')

def test_reddit_api():
    """Test Reddit API using requests approach"""
    try:
        # Initialize Reddit analyzer
        logger.info("Initializing Reddit analyzer...")
        analyzer = RedditAnalyzer()
        
        # Test searching for a popular stock
        ticker = "AAPL"
        logger.info(f"Searching for {ticker} on Reddit...")
        
        # Search for posts
        posts = analyzer.search_subreddit("stocks", ticker, limit=5)
        
        if posts:
            logger.info(f"✅ Successfully retrieved {len(posts)} posts about {ticker}")
            for i, post in enumerate(posts[:3], 1):
                logger.info(f"Post {i}: {post['title']}")
            return True
        else:
            logger.error(f"❌ No posts found for {ticker}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Reddit API test failed with error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    print("\n🔍 Testing Reddit API with requests approach...\n")
    success = test_reddit_api()
    print(f"\n{'✅ Reddit API test succeeded' if success else '❌ Reddit API test failed'}")
