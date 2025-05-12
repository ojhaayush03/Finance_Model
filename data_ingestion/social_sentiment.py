import os
import pandas as pd
import praw
import tweepy
from datetime import datetime, timedelta
from textblob import TextBlob
import sys
import time
import random
import logging

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  
from config import get_config
from db_utils import MongoDBClient

class SocialMediaSentimentAnalyzer:
    def __init__(self):
        """Initialize social media clients and MongoDB connection"""
        self.db_client = MongoDBClient()
        
        # Twitter/X API credentials
        self.twitter_bearer_token = get_config('TWITTER_BEARER_TOKEN')
        
        # Reddit API credentials
        self.reddit_client_id = get_config('REDDIT_CLIENT_ID')
        self.reddit_client_secret = get_config('REDDIT_CLIENT_SECRET')
        self.reddit_user_agent = get_config('REDDIT_USER_AGENT')
        
        # Initialize clients
        self._init_twitter_client()
        self._init_reddit_client()
    
    def _init_twitter_client(self):
        """Initialize Twitter/X API client using tweepy"""
        try:
            self.twitter_client = tweepy.Client(bearer_token=self.twitter_bearer_token)
            print("✅ Twitter/X API client initialized")
        except Exception as e:
            print(f"❌ Error initializing Twitter/X client: {str(e)}")
            self.twitter_client = None
    
    def _init_reddit_client(self):
        """Initialize Reddit API client"""
        try:
            self.reddit_client = praw.Reddit(
                client_id=self.reddit_client_id,
                client_secret=self.reddit_client_secret,
                user_agent=self.reddit_user_agent
            )
            print("✅ Reddit API client initialized")
        except Exception as e:
            print(f"❌ Error initializing Reddit client: {str(e)}")
            self.reddit_client = None
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of text using TextBlob"""
        analysis = TextBlob(text)
        return {
            'polarity': analysis.sentiment.polarity,
            'subjectivity': analysis.sentiment.subjectivity
        }
    
    def fetch_twitter_sentiment(self, ticker, days_back=7, max_results=100, max_retries=3):
        """Fetch and analyze Twitter/X posts about a stock ticker using tweepy with retry mechanism"""
        if not self.twitter_client:
            print("❌ Twitter/X client not initialized")
            return pd.DataFrame()
        
        # Format query for Twitter/X search
        # Don't use the $ (cashtag) operator as it's not available in the free tier
        query = f"{ticker} stock lang:en -is:retweet"
        
        # Calculate start time
        start_time = datetime.now() - timedelta(days=days_back)
        
        # Implement retry with exponential backoff
        retry_count = 0
        backoff_time = 2  # Initial backoff time in seconds
        response = None
        
        # Try to fetch tweets with retry mechanism
        while retry_count <= max_retries:
            try:
                # Search tweets
                response = self.twitter_client.search_recent_tweets(
                    query=query,
                    max_results=max_results,
                    start_time=start_time,
                    tweet_fields=['created_at', 'public_metrics', 'author_id']
                )
                # If successful, break out of the retry loop
                break
            except tweepy.TooManyRequests:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"❌ Twitter API rate limit exceeded after {max_retries} retries")
                    return pd.DataFrame()
                
                # Calculate backoff time with jitter
                sleep_time = backoff_time + random.uniform(0, 1)
                print(f"⏳ Twitter API rate limit hit. Retrying in {sleep_time:.2f} seconds (attempt {retry_count}/{max_retries})")
                time.sleep(sleep_time)
                
                # Exponential backoff
                backoff_time *= 2
            except Exception as e:
                print(f"❌ Error fetching Twitter data: {str(e)}")
                return pd.DataFrame()
        
        # Process the tweets if we have a response
        try:
            tweets = []
            
            # Process tweets
            if response and hasattr(response, 'data') and response.data:
                for tweet in response.data:
                    # Analyze sentiment
                    sentiment = self.analyze_sentiment(tweet.text)
                    
                    # Create tweet record
                    tweet_data = {
                        'ticker': ticker,
                        'platform': 'twitter',
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'author_id': tweet.author_id,
                        'likes': tweet.public_metrics.get('like_count', 0),
                        'retweets': tweet.public_metrics.get('retweet_count', 0),
                        'sentiment_polarity': sentiment['polarity'],
                        'sentiment_subjectivity': sentiment['subjectivity']
                    }
                    
                    tweets.append(tweet_data)
            
            # Store in MongoDB
            if tweets:
                collection = self.db_client.db[f'{ticker.lower()}_social']
                collection.create_index([('platform', 1), ('created_at', 1)])
                collection.insert_many(tweets)
                
                print(f"✅ Retrieved and stored {len(tweets)} tweets for {ticker}")
                return pd.DataFrame(tweets)
            else:
                print(f"⚠️ No tweets found for {ticker}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error processing Twitter data: {str(e)}")
            return pd.DataFrame()
    
    def fetch_reddit_sentiment(self, ticker, days_back=7, limit=100):
        """Fetch and analyze Reddit posts about a stock ticker"""
        if not self.reddit_client:
            print("❌ Reddit client not initialized")
            return pd.DataFrame()
        
        try:
            # Subreddits to search
            subreddits = ['wallstreetbets', 'stocks', 'investing', f'{ticker}']
            posts = []
            
            for subreddit_name in subreddits:
                try:
                    subreddit = self.reddit_client.subreddit(subreddit_name)
                    
                    # Search for posts containing the ticker
                    search_query = f"{ticker}"
                    search_results = subreddit.search(search_query, time_filter='week', limit=limit)
                    
                    for post in search_results:
                        # Skip posts older than days_back
                        post_date = datetime.fromtimestamp(post.created_utc)
                        if (datetime.now() - post_date).days > days_back:
                            continue
                        
                        # Analyze sentiment
                        title_sentiment = self.analyze_sentiment(post.title)
                        
                        # Create post record
                        post_data = {
                            'ticker': ticker,
                            'platform': 'reddit',
                            'subreddit': subreddit_name,
                            'title': post.title,
                            'created_at': post_date,
                            'author': str(post.author),
                            'score': post.score,
                            'comments': post.num_comments,
                            'sentiment_polarity': title_sentiment['polarity'],
                            'sentiment_subjectivity': title_sentiment['subjectivity']
                        }
                        
                        posts.append(post_data)
                except Exception as e:
                    print(f"⚠️ Error processing subreddit {subreddit_name}: {str(e)}")
                    continue
            
            # Store in MongoDB
            if posts:
                collection = self.db_client.db[f'{ticker.lower()}_social']
                collection.create_index([('platform', 1), ('created_at', 1)])
                collection.insert_many(posts)
                
                print(f"✅ Retrieved and stored {len(posts)} Reddit posts for {ticker}")
                return pd.DataFrame(posts)
            else:
                print(f"⚠️ No Reddit posts found for {ticker}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error fetching Reddit data: {str(e)}")
            return pd.DataFrame()
    
    def get_social_sentiment(self, ticker, days_back=7):
        """Get combined social media sentiment for a ticker"""
        collection = self.db_client.db[f'{ticker.lower()}_social']
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get data from MongoDB
        data = list(collection.find({
            'created_at': {'$gte': start_date, '$lte': end_date}
        }))
        
        # If no data in MongoDB, fetch from APIs
        if not data:
            twitter_df = self.fetch_twitter_sentiment(ticker, days_back)
            reddit_df = self.fetch_reddit_sentiment(ticker, days_back)
            
            # Combine data
            if not twitter_df.empty or not reddit_df.empty:
                combined_df = pd.concat([twitter_df, reddit_df], ignore_index=True)
                data = combined_df.to_dict('records')
        
        # Convert to DataFrame
        if data:
            df = pd.DataFrame(data)
            
            # Group by date and platform
            df['date'] = pd.to_datetime(df['created_at']).dt.date
            grouped = df.groupby(['date', 'platform']).agg({
                'sentiment_polarity': 'mean',
                'sentiment_subjectivity': 'mean',
                'ticker': 'first',
                '_id': 'count'
            }).rename(columns={'_id': 'post_count'}).reset_index()
            
            return grouped
        else:
            return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    analyzer = SocialMediaSentimentAnalyzer()
    ticker = input("Enter ticker symbol to analyze social sentiment: ").upper()
    
    print(f"\nFetching Twitter/X sentiment for {ticker}...")
    twitter_df = analyzer.fetch_twitter_sentiment(ticker)
    
    print(f"\nFetching Reddit sentiment for {ticker}...")
    reddit_df = analyzer.fetch_reddit_sentiment(ticker)
    
    # Get combined sentiment
    sentiment_df = analyzer.get_social_sentiment(ticker)
    if not sentiment_df.empty:
        print(f"\nSocial Media Sentiment for {ticker}:")
        print(sentiment_df)
