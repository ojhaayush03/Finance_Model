import os
import sys
import time
import tweepy
from datetime import datetime, timedelta

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import get_config

def test_twitter_api():
    """Test the Twitter API connection and rate limits"""
    # Get the API key from config
    bearer_token = get_config('TWITTER_BEARER_TOKEN')
    
    print(f"Twitter Bearer Token: {bearer_token[:15]}...{bearer_token[-15:]} (length: {len(bearer_token)})")
    
    try:
        # Initialize the Twitter client
        client = tweepy.Client(bearer_token=bearer_token)
        
        # Check rate limits
        print("\nChecking Twitter API rate limits...")
        
        # Try a simple search to see if we can connect
        ticker = "AAPL"
        # Don't use the $ (cashtag) operator as it's not available in the free tier
        query = f"{ticker} stock lang:en -is:retweet"
        
        print(f"\nAttempting to search for '{query}'...")
        try:
            # Calculate start time
            start_time = datetime.now() - timedelta(days=7)
            
            # Search tweets
            response = client.search_recent_tweets(
                query=query,
                max_results=10,  # Reduced to minimize rate limit impact
                start_time=start_time,
                tweet_fields=['created_at', 'public_metrics', 'author_id']
            )
            
            if hasattr(response, 'data') and response.data:
                print(f"✅ Successfully retrieved {len(response.data)} tweets")
                print("\nSample tweet:")
                print(f"Text: {response.data[0].text[:100]}...")
                print(f"Created at: {response.data[0].created_at}")
                print(f"Likes: {response.data[0].public_metrics.get('like_count', 0)}")
            else:
                print("⚠️ No tweets found matching the query")
                
            # Check rate limit info from response headers
            if hasattr(response, 'includes') and hasattr(response.includes, 'rate_limit'):
                rate_limit = response.includes.rate_limit
                print(f"\nRate limit information:")
                print(f"Limit: {rate_limit.limit}")
                print(f"Remaining: {rate_limit.remaining}")
                print(f"Reset at: {rate_limit.reset}")
            
            return True
            
        except tweepy.TooManyRequests as e:
            print(f"❌ Rate limit exceeded: {str(e)}")
            print("\nWhen encountering rate limits:")
            print("1. Implement exponential backoff (already done in social_sentiment.py)")
            print("2. Reduce request frequency")
            print("3. Consider upgrading to a paid API tier")
            print("4. Cache results to minimize duplicate requests")
            return False
            
        except Exception as e:
            print(f"❌ Error searching tweets: {str(e)}")
            return False
            
    except Exception as e:
        print(f"❌ Error initializing Twitter API: {str(e)}")
        return False

if __name__ == "__main__":
    print("Testing Twitter API...")
    success = test_twitter_api()
    if success:
        print("\n✅ Twitter API test completed successfully")
    else:
        print("\n❌ Twitter API test failed")
