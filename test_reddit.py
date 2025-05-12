"""
Detailed test script for Reddit API to diagnose authentication issues
"""

import praw
import logging
from config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('reddit_tester')

def test_reddit_api_detailed():
    """Test Reddit API connection with detailed error reporting"""
    try:
        # Get credentials from config
        client_id = get_config('REDDIT_CLIENT_ID')
        client_secret = get_config('REDDIT_CLIENT_SECRET')
        user_agent = get_config('REDDIT_USER_AGENT')
        
        # Log credential info (without revealing full values)
        logger.info(f"Client ID: {client_id[:5]}...{client_id[-5:] if len(client_id) > 10 else ''}")
        logger.info(f"Client Secret: {client_secret[:3]}...{client_secret[-3:] if len(client_secret) > 6 else ''}")
        logger.info(f"User Agent: {user_agent}")
        
        # Initialize Reddit client
        logger.info("Initializing Reddit client...")
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        
        # Test authentication by checking read-only status
        logger.info(f"Read-only mode: {reddit.read_only}")
        
        # Try to access public subreddit
        logger.info("Attempting to access r/stocks subreddit...")
        subreddit = reddit.subreddit('stocks')
        
        # Try to get subreddit info
        logger.info(f"Subreddit display name: {subreddit.display_name}")
        logger.info(f"Subreddit title: {subreddit.title}")
        
        # Try to get posts
        logger.info("Fetching posts...")
        posts = list(subreddit.hot(limit=3))
        
        if posts:
            logger.info(f"✅ Successfully retrieved {len(posts)} posts")
            logger.info(f"First post title: {posts[0].title}")
            return True
        else:
            logger.error("❌ No posts retrieved")
            return False
            
    except Exception as e:
        logger.error(f"❌ Reddit API test failed with error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    print("\n🔍 Testing Reddit API in detail...\n")
    success = test_reddit_api_detailed()
    print(f"\n{'✅ Reddit API test succeeded' if success else '❌ Reddit API test failed'}")
