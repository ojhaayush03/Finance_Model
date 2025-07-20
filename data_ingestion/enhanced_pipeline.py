import sys
import os
import subprocess
import pandas as pd
from datetime import datetime, timedelta
import argparse
import logging
import tweepy
from textblob import TextBlob

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  
from config import get_config

# Import our modules
from data_ingestion.db_utils import MongoDBClient
#from data_ingestion.news_api import FinancialNewsCollector
from data_ingestion.economic_indicators import EconomicIndicatorsCollector
from data_ingestion.reddit_api import RedditAnalyzer

# Configure logging
# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure file handler with ASCII encoding
file_handler = logging.FileHandler(
    os.path.join(logs_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    encoding='utf-8'
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        file_handler
    ]
)
logger = logging.getLogger('enhanced_pipeline')

class EnhancedDataPipeline:
    def __init__(self):
        """Initialize the enhanced data pipeline with all data sources"""
        
        # Initialize MongoDB client
        self.db_client = MongoDBClient()
        
        # Initialize data collectors
        #self.news_collector = FinancialNewsCollector()
        self.economic_collector = EconomicIndicatorsCollector()
        
        # Initialize Twitter client
        self.twitter_bearer_token = get_config('TWITTER_BEARER_TOKEN')
        self.twitter_client = tweepy.Client(bearer_token=self.twitter_bearer_token)
        
        # Initialize Reddit client
        self.reddit_analyzer = RedditAnalyzer()
        
        logger.info("Enhanced data pipeline initialized")
    
    def fetch_stock_data(self, ticker):
        """Fetch basic stock data using Alpha Vantage"""
        logger.info(f"Fetching stock data for {ticker}...")
        try:
            # Use the correct path to the script
            script_path = os.path.join(os.path.dirname(__file__), "fetch_stock_data.py")
            subprocess.run(["python", script_path, ticker], check=True)
            logger.info(f"Stock data fetched for {ticker}")
            return True
        except subprocess.CalledProcessError:
            logger.error(f"Error fetching data for {ticker}")
            return False
    
    def process_indicators(self, ticker):
        """Process technical indicators for the stock data"""
        logger.info(f"Processing indicators for {ticker}...")
        try:
            # Use the correct path to the script
            script_path = os.path.join(os.path.dirname(__file__), "process_indicators.py")
            subprocess.run(["python", script_path, ticker], check=True)
            logger.info(f"Indicators processed for {ticker}")
            return True
        except subprocess.CalledProcessError:
            logger.error(f"Error processing indicators for {ticker}")
            return False
    
    def store_in_mongodb(self, ticker):
        """Store processed data in MongoDB"""
        logger.info(f"Storing {ticker} data in MongoDB...")
        try:
            # Create absolute path to processed data
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            processed_data_dir = os.path.join(project_root, "processed_data")
            file_path = os.path.join(processed_data_dir, f"{ticker}_indicators.csv")
            
            # Read the processed data
            data = pd.read_csv(file_path)
            
            # Store in MongoDB
            records_stored = self.db_client.store_stock_data(ticker, data)
            logger.info(f"✅ Stored {records_stored} records in MongoDB for {ticker}")
            return True
        except Exception as e:
            logger.error(f"❌ MongoDB storage failed: {str(e)}")
            return False
    
    def fetch_news_data(self, ticker, days_back=7):
        """Fetch and process news data"""
        logger.info(f"Fetching news for {ticker}...")
        try:
            news_df = self.news_collector.process_news(ticker, days_back)
            if news_df.empty:
                logger.warning(f"⚠️ No news found for {ticker}")
            else:
                logger.info(f"✅ Fetched {len(news_df)} news articles for {ticker}")
            return True
        except Exception as e:
            logger.error(f"❌ Error fetching news: {str(e)}")
            return False
    
    def fetch_economic_data(self):
        """Fetch economic indicators"""
        logger.info("Fetching economic indicators...")
        try:
            self.economic_collector.fetch_all_indicators()
            logger.info("✅ Economic indicators fetched and stored")
            return True
        except Exception as e:
            logger.error(f"❌ Error fetching economic indicators: {str(e)}")
            return False
    
    def fetch_twitter_sentiment(self, ticker, days_back=7, max_results=100):
        """Fetch Twitter sentiment using tweepy"""
        logger.info(f"Analyzing Twitter sentiment for {ticker}...")
        try:
            if not self.twitter_client:
                logger.error("❌ Twitter client not initialized")
                return False
            
            # Format query for Twitter/X search
            query = f"${ticker} lang:en -is:retweet"
            
            # Calculate start time
            start_time = datetime.now() - timedelta(days=days_back)
            
            # Search tweets
            response = self.twitter_client.search_recent_tweets(
                query=query,
                max_results=max_results,
                start_time=start_time,
                tweet_fields=['created_at', 'public_metrics', 'author_id']
            )
            
            tweets = []
            
            # Process tweets
            if hasattr(response, 'data') and response.data:
                for tweet in response.data:
                    # Analyze sentiment with TextBlob
                    analysis = TextBlob(tweet.text)
                    sentiment = {
                        'polarity': analysis.sentiment.polarity,
                        'subjectivity': analysis.sentiment.subjectivity
                    }
                    
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
                
                # Store in MongoDB using the new folder structure
                if tweets:
                    # Use the store_twitter_data method from db_utils
                    stored_count = self.db_client.store_twitter_data(ticker, tweets)
                    
                    logger.info(f"Analyzed {len(tweets)} Twitter posts for {ticker} and stored in twitter_data folder")
                    return True
                else:
                    logger.warning(f"No Twitter data found for {ticker}")
                    return False
            else:
                logger.warning(f"⚠️ No Twitter data found for {ticker}")
                return False
                
        except Exception as e:
            logger.error(f"Error fetching Twitter sentiment: {str(e)}")
            return False
    
    def run_pipeline(self, ticker, include_news=True, include_economic=True, include_twitter=True, include_reddit=True):
        """Run the complete enhanced data pipeline"""
        logger.info(f"Starting enhanced data pipeline for {ticker}")
        
        # Track success/failure
        steps_completed = 0
        steps_failed = 0
        
        # Step 1: Fetch stock data
        if self.fetch_stock_data(ticker):
            steps_completed += 1
        else:
            steps_failed += 1
            logger.error("Failed to fetch stock data, stopping pipeline")
            return False
        
        # Step 2: Process indicators
        if self.process_indicators(ticker):
            steps_completed += 1
        else:
            steps_failed += 1
            logger.error("❌ Failed to process indicators, stopping pipeline")
            return False
        
        # Step 3: Store in MongoDB
        if self.store_in_mongodb(ticker):
            steps_completed += 1
        else:
            steps_failed += 1
            logger.error("❌ Failed to store data in MongoDB, stopping pipeline")
            return False
        
        # Step 4: Fetch news (optional)
        if include_news:
            if self.fetch_news_data(ticker):
                steps_completed += 1
            else:
                steps_failed += 1
                logger.warning("fetching data")
        
        # Step 5: Fetch economic indicators (optional)
        if include_economic:
            if self.fetch_economic_data():
                steps_completed += 1
            else:
                steps_failed += 1
                logger.warning("Failed to fetch economic data, continuing pipeline")
        
        # Step 6: Fetch Twitter sentiment (optional)
        if include_twitter:
            if self.fetch_twitter_sentiment(ticker):
                steps_completed += 1
            else:
                steps_failed += 1
                logger.warning("Failed to fetch Twitter sentiment, continuing pipeline")
        
        # Step 7: Fetch Reddit sentiment (optional)
        if include_reddit:
            try:
                logger.info(f"Analyzing Reddit sentiment for {ticker}...")
                posts_df = self.reddit_analyzer.fetch_stock_sentiment(ticker)
                if not posts_df.empty:
                    logger.info(f"Analyzed {len(posts_df)} Reddit posts for {ticker}")
                    steps_completed += 1
                else:
                    logger.warning(f"No Reddit data found for {ticker}")
                    steps_failed += 1
            except Exception as e:
                logger.error(f"Error fetching Reddit sentiment: {str(e)}")
                steps_failed += 1
        
        # Report pipeline completion
        logger.info(f"Pipeline completed with {steps_completed} steps successful and 0 steps failed")
        return steps_failed == 0

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Enhanced Financial Data Pipeline")
    parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL, TSLA)")
    parser.add_argument("--skip-news", action="store_true", help="Skip news data collection")
    parser.add_argument("--skip-economic", action="store_true", help="Skip economic indicators collection")
    parser.add_argument("--skip-twitter", action="store_true", help="Skip Twitter sentiment analysis")
    parser.add_argument("--skip-reddit", action="store_true", help="Skip Reddit sentiment analysis")
    
    args = parser.parse_args()
    
    # Initialize and run pipeline
    pipeline = EnhancedDataPipeline()
    success = pipeline.run_pipeline(
        args.ticker.upper(),
        include_news=not args.skip_news,
        include_economic=not args.skip_economic,
        include_twitter=not args.skip_twitter,
        include_reddit=not args.skip_reddit
    )
    
    if success:
        print(f"\n✅ Enhanced pipeline for {args.ticker.upper()} completed successfully!")
    else:
        print(f"\n✅ Enhanced pipeline for {args.ticker.upper()} completed.")
        sys.exit(1)
