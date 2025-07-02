"""
Correlation Analysis Module

This module analyzes relationships between different data sources:
- Stock price data
- Technical indicators
- Social media sentiment (Twitter, Reddit)
- Economic indicators
- News sentiment

It provides functions to calculate and visualize correlations between these data sources.
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import logging
from datetime import datetime, timedelta

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import get_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('correlation_analysis')

class CorrelationAnalyzer:
    def __init__(self):
        """Initialize the correlation analyzer with MongoDB connection"""
        self.mongodb_uri = get_config('MONGODB_URI')
        self.db_name = get_config('MONGODB_DB_NAME')
        self.client = MongoClient(self.mongodb_uri)
        self.db = self.client[self.db_name]
        logger.info("Correlation analyzer initialized with MongoDB connection")
    
    def fetch_stock_data(self, ticker, days=30):
        """Fetch stock data for a given ticker from MongoDB"""
        try:
            collection = self.db[f"{ticker}_stock_data"]
            
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Query for data within the date range
            query = {
                "date": {
                    "$gte": start_date.strftime("%Y-%m-%d"),
                    "$lte": end_date.strftime("%Y-%m-%d")
                }
            }
            
            # Fetch and convert to DataFrame
            cursor = collection.find(query)
            stock_data = pd.DataFrame(list(cursor))
            
            if stock_data.empty:
                logger.warning(f"No stock data found for {ticker} in the last {days} days")
                return None
            
            logger.info(f"Fetched {len(stock_data)} stock data points for {ticker}")
            return stock_data
        
        except Exception as e:
            logger.error(f"Error fetching stock data: {str(e)}")
            return None
    
    def fetch_sentiment_data(self, ticker, source, days=30):
        """Fetch sentiment data from Twitter or Reddit for a given ticker"""
        try:
            collection_name = f"{ticker}_{source}"
            collection = self.db[collection_name]
            
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Field name for date depends on the source
            date_field = "created_at" if source == "twitter" else "post_date"
            
            # Query for data within the date range
            query = {
                date_field: {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
            
            # Fetch and convert to DataFrame
            cursor = collection.find(query)
            sentiment_data = pd.DataFrame(list(cursor))
            
            if sentiment_data.empty:
                logger.warning(f"No {source} sentiment data found for {ticker} in the last {days} days")
                return None
            
            logger.info(f"Fetched {len(sentiment_data)} {source} sentiment data points for {ticker}")
            return sentiment_data
        
        except Exception as e:
            logger.error(f"Error fetching {source} sentiment data: {str(e)}")
            return None
    
    def fetch_economic_data(self, days=90):
        """Fetch economic indicators data"""
        try:
            collection = self.db["economic_indicators"]
            
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Query for data within the date range
            query = {
                "date": {
                    "$gte": start_date.strftime("%Y-%m-%d"),
                    "$lte": end_date.strftime("%Y-%m-%d")
                }
            }
            
            # Fetch and convert to DataFrame
            cursor = collection.find(query)
            economic_data = pd.DataFrame(list(cursor))
            
            if economic_data.empty:
                logger.warning(f"No economic data found in the last {days} days")
                return None
            
            logger.info(f"Fetched {len(economic_data)} economic data points")
            return economic_data
        
        except Exception as e:
            logger.error(f"Error fetching economic data: {str(e)}")
            return None
    
    def fetch_news_data(self, ticker, days=30):
        """Fetch news sentiment data for a given ticker"""
        try:
            collection = self.db[f"{ticker}_news"]
            
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Query for data within the date range
            query = {
                "published_at": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
            
            # Fetch and convert to DataFrame
            cursor = collection.find(query)
            news_data = pd.DataFrame(list(cursor))
            
            if news_data.empty:
                logger.warning(f"No news data found for {ticker} in the last {days} days")
                return None
            
            logger.info(f"Fetched {len(news_data)} news data points for {ticker}")
            return news_data
        
        except Exception as e:
            logger.error(f"Error fetching news data: {str(e)}")
            return None
    
    def aggregate_daily_sentiment(self, sentiment_data, date_column, sentiment_column):
        """Aggregate sentiment data to daily averages"""
        if sentiment_data is None or sentiment_data.empty:
            return None
        
        # Convert date column to datetime if it's not already
        sentiment_data[date_column] = pd.to_datetime(sentiment_data[date_column])
        
        # Extract date part only
        sentiment_data['date'] = sentiment_data[date_column].dt.date
        
        # Group by date and calculate average sentiment
        daily_sentiment = sentiment_data.groupby('date')[sentiment_column].mean().reset_index()
        daily_sentiment['date'] = pd.to_datetime(daily_sentiment['date'])
        
        return daily_sentiment
    
    def merge_datasets(self, stock_data, twitter_data=None, reddit_data=None, economic_data=None, news_data=None):
        """Merge different datasets on date for correlation analysis"""
        if stock_data is None or stock_data.empty:
            logger.error("Stock data is required for merging datasets")
            return None
        
        # Ensure date column is datetime
        stock_data['date'] = pd.to_datetime(stock_data['date'])
        merged_data = stock_data[['date', 'close', 'volume']].copy()
        
        # Add Twitter sentiment if available
        if twitter_data is not None and not twitter_data.empty:
            twitter_daily = self.aggregate_daily_sentiment(twitter_data, 'created_at', 'sentiment_polarity')
            if twitter_daily is not None:
                merged_data = pd.merge(merged_data, twitter_daily, on='date', how='left')
                merged_data.rename(columns={'sentiment_polarity': 'twitter_sentiment'}, inplace=True)
        
        # Add Reddit sentiment if available
        if reddit_data is not None and not reddit_data.empty:
            reddit_daily = self.aggregate_daily_sentiment(reddit_data, 'post_date', 'sentiment_polarity')
            if reddit_daily is not None:
                merged_data = pd.merge(merged_data, reddit_daily, on='date', how='left')
                merged_data.rename(columns={'sentiment_polarity': 'reddit_sentiment'}, inplace=True)
        
        # Add economic data if available
        if economic_data is not None and not economic_data.empty:
            economic_data['date'] = pd.to_datetime(economic_data['date'])
            # Select relevant economic indicators
            econ_columns = ['date', 'GDP', 'UNRATE', 'CPIAUCSL', 'DFF']
            econ_data_subset = economic_data[economic_data.columns.intersection(econ_columns)]
            merged_data = pd.merge(merged_data, econ_data_subset, on='date', how='left')
        
        # Add news sentiment if available
        if news_data is not None and not news_data.empty:
            news_data['published_at'] = pd.to_datetime(news_data['published_at'])
            news_data['date'] = news_data['published_at'].dt.date
            daily_news = news_data.groupby('date')['sentiment_score'].mean().reset_index()
            daily_news['date'] = pd.to_datetime(daily_news['date'])
            merged_data = pd.merge(merged_data, daily_news, on='date', how='left')
            merged_data.rename(columns={'sentiment_score': 'news_sentiment'}, inplace=True)
        
        # Forward fill missing values for economic indicators (they don't update daily)
        merged_data.fillna(method='ffill', inplace=True)
        
        return merged_data
    
    def calculate_correlations(self, merged_data):
        """Calculate correlation matrix between different data sources"""
        if merged_data is None or merged_data.empty:
            logger.error("No data available for correlation analysis")
            return None
        
        # Drop date column for correlation calculation
        data_for_corr = merged_data.drop(columns=['date'])
        
        # Calculate correlation matrix
        correlation_matrix = data_for_corr.corr()
        
        return correlation_matrix
    
    def visualize_correlations(self, correlation_matrix, title="Correlation Matrix", save_path=None):
        """Visualize correlation matrix as a heatmap"""
        if correlation_matrix is None:
            logger.error("No correlation matrix to visualize")
            return
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, center=0)
        plt.title(title)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Correlation heatmap saved to {save_path}")
        else:
            plt.show()
    
    def analyze_ticker_correlations(self, ticker, days=30, save_path=None):
        """Run complete correlation analysis for a ticker"""
        logger.info(f"Starting correlation analysis for {ticker} over the last {days} days")
        
        # Fetch data from different sources
        stock_data = self.fetch_stock_data(ticker, days)
        twitter_data = self.fetch_sentiment_data(ticker, "twitter", days)
        reddit_data = self.fetch_sentiment_data(ticker, "reddit", days)
        economic_data = self.fetch_economic_data(days)
        news_data = self.fetch_news_data(ticker, days)
        
        # Merge datasets
        merged_data = self.merge_datasets(
            stock_data, twitter_data, reddit_data, economic_data, news_data
        )
        
        if merged_data is None or merged_data.empty:
            logger.error("Failed to create merged dataset for correlation analysis")
            return None
        
        # Calculate correlations
        correlation_matrix = self.calculate_correlations(merged_data)
        
        # Visualize correlations
        if save_path is None:
            # Create default save path in project directory
            project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            save_dir = os.path.join(project_dir, "analysis_results")
            os.makedirs(save_dir, exist_ok=True)
            save_path = os.path.join(save_dir, f"{ticker}_correlation_{datetime.now().strftime('%Y%m%d')}.png")
        
        self.visualize_correlations(
            correlation_matrix, 
            title=f"Correlation Matrix for {ticker} (Last {days} Days)",
            save_path=save_path
        )
        
        return correlation_matrix, merged_data

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Financial Data Correlation Analysis")
    parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL, TSLA)")
    parser.add_argument("--days", type=int, default=30, help="Number of days to analyze")
    parser.add_argument("--save-path", help="Path to save correlation heatmap")
    
    args = parser.parse_args()
    
    analyzer = CorrelationAnalyzer()
    analyzer.analyze_ticker_correlations(args.ticker.upper(), args.days, args.save_path)
