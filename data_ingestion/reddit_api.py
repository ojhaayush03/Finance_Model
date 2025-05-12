"""
Reddit API module using requests instead of PRAW
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from textblob import TextBlob
import sys
import os

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  
from config import get_config
from data_ingestion.db_utils import MongoDBClient

class RedditAnalyzer:
    def __init__(self):
        """Initialize Reddit API client using requests"""
        self.db_client = MongoDBClient()
        
        # Reddit API credentials
        self.client_id = get_config('REDDIT_CLIENT_ID')
        self.client_secret = get_config('REDDIT_CLIENT_SECRET')
        self.user_agent = get_config('REDDIT_USER_AGENT')
        
        # Initialize session and get token
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        self._get_access_token()
    
    def _get_access_token(self):
        """Get Reddit API access token"""
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {
            'grant_type': 'client_credentials',
            'username': '',
            'password': ''
        }
        
        try:
            response = self.session.post(
                'https://www.reddit.com/api/v1/access_token',
                auth=auth,
                data=data
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.session.headers.update({
                    'Authorization': f"Bearer {token_data['access_token']}"
                })
                print("✅ Reddit API token obtained")
                return True
            else:
                print(f"❌ Failed to get Reddit token: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"❌ Error getting Reddit token: {str(e)}")
            return False
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of text using TextBlob"""
        analysis = TextBlob(text)
        return {
            'polarity': analysis.sentiment.polarity,
            'subjectivity': analysis.sentiment.subjectivity
        }
    
    def search_subreddit(self, subreddit, query, limit=25):
        """Search for posts in a subreddit"""
        url = f"https://oauth.reddit.com/r/{subreddit}/search"
        params = {
            'q': query,
            'restrict_sr': 'on',
            'sort': 'relevance',
            'limit': limit,
            't': 'week'
        }
        
        try:
            response = self.session.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                posts = []
                
                for post in data['data']['children']:
                    post_data = post['data']
                    posts.append({
                        'title': post_data['title'],
                        'selftext': post_data.get('selftext', ''),
                        'score': post_data['score'],
                        'created_utc': datetime.fromtimestamp(post_data['created_utc']),
                        'author': post_data['author'],
                        'num_comments': post_data['num_comments'],
                        'url': post_data['url'],
                        'permalink': f"https://www.reddit.com{post_data['permalink']}"
                    })
                
                return posts
            else:
                print(f"❌ Failed to search subreddit: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"❌ Error searching subreddit: {str(e)}")
            return []
    
    def fetch_stock_sentiment(self, ticker, days_back=7):
        """Fetch and analyze Reddit posts about a stock ticker"""
        subreddits = ['wallstreetbets', 'stocks', 'investing']
        all_posts = []
        
        for subreddit in subreddits:
            try:
                # Search for the ticker
                posts = self.search_subreddit(subreddit, ticker, limit=50)
                
                # Filter by date
                cutoff_date = datetime.now() - timedelta(days=days_back)
                filtered_posts = [p for p in posts if p['created_utc'] >= cutoff_date]
                
                # Add subreddit info
                for post in filtered_posts:
                    post['subreddit'] = subreddit
                    post['ticker'] = ticker
                    
                    # Analyze sentiment
                    text = f"{post['title']} {post['selftext']}"
                    sentiment = self.analyze_sentiment(text)
                    post['sentiment_polarity'] = sentiment['polarity']
                    post['sentiment_subjectivity'] = sentiment['subjectivity']
                
                all_posts.extend(filtered_posts)
                print(f"✅ Retrieved {len(filtered_posts)} posts from r/{subreddit} for {ticker}")
                
            except Exception as e:
                print(f"❌ Error processing r/{subreddit}: {str(e)}")
        
        # Store in MongoDB
        if all_posts:
            collection = self.db_client.db[f'{ticker.lower()}_reddit']
            collection.create_index([('ticker', 1), ('created_utc', 1)])
            
            # Convert to proper format for MongoDB
            for post in all_posts:
                collection.update_one(
                    {'permalink': post['permalink']},
                    {'$set': post},
                    upsert=True
                )
            
            print(f"✅ Stored {len(all_posts)} Reddit posts for {ticker}")
        
        return pd.DataFrame(all_posts)
    
    def get_sentiment_summary(self, ticker, days_back=7):
        """Get sentiment summary for a ticker"""
        collection = self.db_client.db[f'{ticker.lower()}_reddit']
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get data from MongoDB
        posts = list(collection.find({
            'ticker': ticker,
            'created_utc': {'$gte': start_date, '$lte': end_date}
        }))
        
        # If no data in MongoDB, fetch from API
        if not posts:
            self.fetch_stock_sentiment(ticker, days_back)
            posts = list(collection.find({
                'ticker': ticker,
                'created_utc': {'$gte': start_date, '$lte': end_date}
            }))
        
        # Create DataFrame
        df = pd.DataFrame(posts)
        
        if df.empty:
            return pd.DataFrame()
        
        # Group by date and calculate average sentiment
        df['date'] = pd.to_datetime(df['created_utc']).dt.date
        summary = df.groupby(['date', 'subreddit']).agg({
            'sentiment_polarity': 'mean',
            'sentiment_subjectivity': 'mean',
            'score': 'sum',
            'num_comments': 'sum',
            'title': 'count'
        }).rename(columns={'title': 'post_count'}).reset_index()
        
        return summary

if __name__ == "__main__":
    # Example usage
    analyzer = RedditAnalyzer()
    ticker = input("Enter ticker symbol to analyze on Reddit: ").upper()
    
    print(f"\nFetching Reddit sentiment for {ticker}...")
    posts_df = analyzer.fetch_stock_sentiment(ticker)
    
    if not posts_df.empty:
        print(f"\nFound {len(posts_df)} posts about {ticker}")
        
        # Show sentiment summary
        summary = analyzer.get_sentiment_summary(ticker)
        print("\nSentiment Summary by Subreddit and Date:")
        print(summary)
