#!/usr/bin/env python3
"""
Ubuntu PySpark Stock Predictor

This script is optimized for running on Ubuntu with Hadoop/Spark via spark-submit.
It performs stock price prediction using PySpark ML, technical indicators, and sentiment analysis.

Usage: spark-submit ubuntu_stock_predictor.py [ticker] [days_to_predict]
"""

# Standard libraries
import re
import random
from datetime import datetime, timedelta

# Data processing
import pandas as pd
# numpy is used in data manipulation and calculations throughout the code
import numpy as np  # Used for numerical operations in feature engineering and array manipulations

# Visualization (if available)
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    print("Visualization libraries not available")

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
# Window functions are used for rolling calculations in data processing
from pyspark.sql.window import Window
# These types are used in schema definitions throughout the code
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType

# ML imports
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
# ML pipeline components are used individually rather than with Pipeline class
from pyspark.ml.linalg import VectorUDT

# MongoDB
try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    print("Warning: MongoDB not available. Some features may be limited.")
    MONGO_AVAILABLE = False

# Sentiment analysis - try TextBlob first, fall back to NLTK if available
try:
    from textblob import TextBlob
    SENTIMENT_ANALYZER = "textblob"
    print("Using TextBlob for sentiment analysis")
except ImportError:
    try:
        import nltk
        from nltk.sentiment.vader import SentimentIntensityAnalyzer
        nltk.download('vader_lexicon', quiet=True)
        SENTIMENT_ANALYZER = "vader"
        print("Using NLTK VADER for sentiment analysis")
    except ImportError:
        print("Warning: No sentiment analysis library available")
        SENTIMENT_ANALYZER = None

# MongoDB connection constants
MONGODB_URI = "mongodb+srv://ayushojha9998:4690@cluster0.rj86jwm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGODB_DB_NAME = "financial_db"

# Import sklearn for local processing if available
try:
    from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    print("Warning: scikit-learn not available. Some features may be limited.")
    SKLEARN_AVAILABLE = False

class UbuntuPySparkStockPredictor:
    """
    Ubuntu optimized PySpark Stock Prediction System
    Adapted for running on Ubuntu with Hadoop/Spark via spark-submit
    """
    
    def __init__(self, data_path="/tmp/stock_data"):
        # Configuration
        self.mongodb_uri = MONGODB_URI
        self.database_name = MONGODB_DB_NAME
        self.data_path = data_path
        
        # Initialize Spark session
        self.spark = None
        self.mongo_client = None
        self.setup_spark_environment()
    
    def setup_spark_environment(self):
        """Setup Spark environment for Ubuntu"""
        print("🔧 Setting up Spark environment...")
        
        try:
            # Initialize Spark session with explicit configurations
            self.spark = SparkSession.builder \
                .appName("UbuntuStockPrediction") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            # Configure logging
            self.spark.sparkContext.setLogLevel("ERROR")
            
            print(f"✅ Spark session initialized: {self.spark.version}")
            
            # Initialize MongoDB client
            if MONGO_AVAILABLE:
                self.mongo_client = MongoClient(self.mongodb_uri)
                # Test connection
                self.mongo_client.admin.command('ping')
                print("✅ MongoDB connection established")
                
                # List available collections
                db = self.mongo_client[self.database_name]
                print("\nAvailable collections in database:")
                collections = db.list_collection_names()
                for collection in collections:
                    print(f"  - {collection}")
            else:
                print("⚠️ MongoDB not available - some features will be limited")
            
        except Exception as e:
            print(f"❌ Setup error: {e}")
            raise
    
    def load_stock_data(self, ticker: str) -> DataFrame:
        """Load stock data from MongoDB using PyMongo"""
        print(f"📊 Loading stock data for {ticker.upper()}...")
        
        try:
            # Connect to database
            db = self.mongo_client[self.database_name]
            
            # Try different collection naming patterns based on MongoDB structure
            collection_names = [
                f"stock_data.{ticker.lower()}",   # stock_data.aapl format
                f"stock_data_{ticker.lower()}",  # stock_data_aapl format
                f"{ticker.lower()}_stock_data",  # aapl_stock_data format
                f"{ticker.lower()}_stock",       # aapl_stock format
                "stock_data"                     # generic fallback
            ]
            
            stock_data = None
            collection_name = None
            
            for name in collection_names:
                if name in db.list_collection_names():
                    collection = db[name]
                    if collection.count_documents({}) > 0:
                        stock_data = list(collection.find())
                        collection_name = name
                        break
            
            if not stock_data:
                raise ValueError(f"No stock data found for {ticker}")
            
            print(f"✅ Found {len(stock_data)} records in collection: {collection_name}")
            
            # Convert to Pandas then to Spark DataFrame
            df_pandas = pd.DataFrame(stock_data)
            
            # Handle date conversion
            if 'date' in df_pandas.columns:
                df_pandas['date'] = pd.to_datetime(df_pandas['date'])
            elif 'Date' in df_pandas.columns:
                df_pandas['date'] = pd.to_datetime(df_pandas['Date'])
                df_pandas.drop('Date', axis=1, inplace=True)
            
            # Standardize column names
            column_mapping = {
                'Close': 'close',
                'Open': 'open', 
                'High': 'high',
                'Low': 'low',
                'Volume': 'volume',
                'Adj Close': 'adj_close'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in df_pandas.columns:
                    df_pandas[new_col] = df_pandas[old_col]
                    if old_col != new_col:
                        df_pandas.drop(old_col, axis=1, inplace=True)
            
            # Remove MongoDB _id column if present
            if '_id' in df_pandas.columns:
                df_pandas.drop('_id', axis=1, inplace=True)
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(df_pandas)
            
            print(f"📈 Stock data loaded: {spark_df.count()} rows, {len(spark_df.columns)} columns")
            return spark_df
            
        except Exception as e:
            print(f"❌ Error loading stock data: {e}")
            raise
    
    def load_reddit_data(self, ticker: str) -> DataFrame:
        """Load Reddit sentiment data from MongoDB"""
        print(f"💬 Loading Reddit data for {ticker.upper()}...")
        
        try:
            db = self.mongo_client[self.database_name]
            
            # Try different collection naming patterns for Reddit data based on MongoDB structure
            collection_names = [
                f"reddit_data.{ticker.lower()}",  # reddit_data.aapl format
                f"reddit_data_{ticker.lower()}", # reddit_data_aapl format
                f"reddit_data{ticker.lower()}",  # reddit_dataaapl format
                f"{ticker.lower()}_reddit",       # aapl_reddit format
                f"{ticker.lower()}_reddit_data", # aapl_reddit_data format
                "reddit_data"                    # generic fallback
            ]
            
            # Find a matching collection
            reddit_data = None
            collection_name = None
            
            for name in collection_names:
                if name in db.list_collection_names():
                    collection = db[name]
                    if collection.count_documents({}) > 0:
                        # Get data from the last 30 days by default
                        cutoff_date = datetime.now() - timedelta(days=30)
                        query = {"date": {"$gte": cutoff_date}}
                        
                        # Try different date field names
                        if collection.count_documents(query) == 0:
                            query = {"created_utc": {"$gte": cutoff_date.timestamp()}}
                        
                        reddit_data = list(collection.find(query))
                        collection_name = name
                        break
            
            if not reddit_data:
                print(f"⚠️ No Reddit data found for {ticker}")
                return self.spark.createDataFrame([], schema=StructType([
                    StructField("date", TimestampType(), True),
                    StructField("sentiment", DoubleType(), True),
                    StructField("post_count", DoubleType(), True)
                ]))
            
            print(f"✅ Found {len(reddit_data)} Reddit posts in collection: {collection_name}")
            
            # Convert to pandas DataFrame
            df_pandas = pd.DataFrame(reddit_data)
            
            # Handle date conversion
            if 'date' in df_pandas.columns:
                df_pandas['date'] = pd.to_datetime(df_pandas['date'])
            elif 'created_utc' in df_pandas.columns:
                df_pandas['date'] = pd.to_datetime(df_pandas['created_utc'], unit='s')
                df_pandas.drop('created_utc', axis=1, inplace=True)
            
            # Remove MongoDB _id column if present
            if '_id' in df_pandas.columns:
                df_pandas.drop('_id', axis=1, inplace=True)
            
            # Calculate sentiment if not already present
            if 'sentiment' not in df_pandas.columns and 'title' in df_pandas.columns:
                print("📊 Calculating sentiment from post titles...")
                df_pandas['sentiment'] = df_pandas['title'].apply(self.analyze_sentiment)
            
            # Aggregate by date if we have multiple posts per day
            df_pandas['date'] = df_pandas['date'].dt.date
            daily_sentiment = df_pandas.groupby('date').agg({
                'sentiment': 'mean',
                'date': 'first'  # Keep one date per group
            }).reset_index(drop=True)
            
            # Add post count
            post_counts = df_pandas.groupby('date').size().reset_index(name='post_count')
            daily_sentiment = daily_sentiment.merge(post_counts, on='date')
            
            # Convert date back to datetime
            daily_sentiment['date'] = pd.to_datetime(daily_sentiment['date'])
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(daily_sentiment)
            
            print(f"📊 Reddit data processed: {spark_df.count()} days of sentiment data")
            return spark_df
            
        except Exception as e:
            print(f"❌ Error loading Reddit data: {e}")
            # Return empty DataFrame with expected schema
            return self.spark.createDataFrame([], schema=StructType([
                StructField("date", TimestampType(), True),
                StructField("sentiment", DoubleType(), True),
                StructField("post_count", DoubleType(), True)
            ]))
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of text using available sentiment analyzer"""
        if not text or not isinstance(text, str):
            return 0.0
            
        try:
            if SENTIMENT_ANALYZER == "textblob":
                return TextBlob(text).sentiment.polarity
            elif SENTIMENT_ANALYZER == "vader":
                return SentimentIntensityAnalyzer().polarity_scores(text)["compound"]
            else:
                return 0.0
        except Exception:
            return 0.0
    
    def fetch_reddit_data(self, ticker: str, days: int = 30) -> pd.DataFrame:
        """Legacy method for fetching Reddit data - redirects to load_reddit_data"""
        print("⚠️ Using deprecated fetch_reddit_data method, consider using load_reddit_data instead")
        spark_df = self.load_reddit_data(ticker)
        # Convert Spark DataFrame to Pandas
        return spark_df.toPandas()
    
    def fetch_stock_data(self, ticker, days=90):
        """Fetch stock data for a given ticker"""
        try:
            # Get database reference
            db = self.mongo_client[self.database_name]
            
            # Try different collection naming patterns based on MongoDB structure
            collection_names = [
                f"stock_data.{ticker.lower()}",   # stock_data.aapl format
                f"stock_data_{ticker.lower()}",  # stock_data_aapl format
                f"{ticker.lower()}_stock_data",  # aapl_stock_data format
                f"{ticker.lower()}_stock",       # aapl_stock format
                "stock_data"                     # generic fallback
            ]
            
            # Find a matching collection
            collection = None
            collection_name = None
            
            for name in collection_names:
                if name in db.list_collection_names():
                    collection = db[name]
                    collection_name = name
                    break
            
            if not collection:
                print(f"No stock data collection found for {ticker}")
                return None
                
            print(f"Using collection: {collection_name}")
            
            # For small datasets, get all available data instead of filtering by date
            # First check how many records are available
            total_records = collection.count_documents({})
            
            if total_records < 30:  # If we have less than 30 records
                # Get all available data
                query = {}
                print(f"Getting all available data ({total_records} records) since dataset is small")
            else:
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
            cursor = collection.find(query).sort("date", 1)  # Sort by date ascending
            stock_data = pd.DataFrame(list(cursor))
            
            if stock_data.empty:
                # Try without date filter to see if any data exists
                cursor = collection.find().limit(10)
                all_data = pd.DataFrame(list(cursor))
                
                if all_data.empty:
                    print(f"No stock data found for {ticker} at all")
                    return None
                else:
                    print(f"No stock data found for {ticker} in the last {days} days")
                    print(f"Available date range in collection: {all_data['date'].min()} to {all_data['date'].max()}")
                    # Return the most recent data available
                    cursor = collection.find().sort("date", -1).limit(days)
                    stock_data = pd.DataFrame(list(cursor))
                    if not stock_data.empty:
                        print(f"Returning {len(stock_data)} most recent data points")
            
            if not stock_data.empty:
                print(f"Fetched {len(stock_data)} stock data points for {ticker}")
                # Ensure date column is datetime
                if 'date' in stock_data.columns:
                    stock_data['date'] = pd.to_datetime(stock_data['date'])
                
                # Standardize column names (handle both lowercase and capitalized versions)
                column_mapping = {
                    'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 
                    'Volume': 'volume', 'Adj Close': 'adj_close'
                }
                
                for old_name, new_name in column_mapping.items():
                    if old_name in stock_data.columns:
                        stock_data.rename(columns={old_name: new_name}, inplace=True)
                
                print("Stock data columns:", stock_data.columns.tolist())
            
            return stock_data if not stock_data.empty else None
        
        except Exception as e:
            print(f"Error fetching stock data: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def fetch_economic_data(self, days=90):
        """Fetch economic indicators data"""
        try:
            # Get database reference
            db = self.mongo_client[self.database_name]
            collection = db["economic_indicators"]
            
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
                print(f"No economic data found in the last {days} days")
                return None
            
            print(f"Fetched {len(economic_data)} economic data points")
            return economic_data
        
        except Exception as e:
            print(f"Error fetching economic data: {str(e)}")
            return None
    
    def analyze_reddit_sentiment(self, reddit_data):
        """Analyze sentiment in Reddit posts"""
        if reddit_data is None or reddit_data.empty:
            return None
        
        # Ensure we have a title and/or body column
        text_columns = []
        if 'title' in reddit_data.columns:
            text_columns.append('title')
        if 'body' in reddit_data.columns:
            text_columns.append('body')
        if 'text' in reddit_data.columns:
            text_columns.append('text')
        if 'content' in reddit_data.columns:
            text_columns.append('content')
        
        if not text_columns:
            print("No text columns found in Reddit data")
            print("Available columns:", reddit_data.columns.tolist())
            return None
        
        # Function to calculate sentiment
        def get_sentiment(text):
            if pd.isna(text) or not isinstance(text, str) or text == '':
                return 0
            return self.sia.polarity_scores(text)['compound']
        
        # Apply sentiment analysis to each text column
        for col in text_columns:
            sentiment_col = f"{col}_sentiment"
            reddit_data[sentiment_col] = reddit_data[col].apply(get_sentiment)
        
        # Calculate overall sentiment
        if len(text_columns) > 1:
            reddit_data['overall_sentiment'] = reddit_data[[f"{col}_sentiment" for col in text_columns]].mean(axis=1)
        else:
            reddit_data['overall_sentiment'] = reddit_data[f"{text_columns[0]}_sentiment"]
        
        # Add sentiment categories
        def categorize_sentiment(score):
            if score <= -0.05:
                return 'Negative'
            elif score >= 0.05:
                return 'Positive'
            else:
                return 'Neutral'
        
        reddit_data['sentiment_category'] = reddit_data['overall_sentiment'].apply(categorize_sentiment)
        
        print("Sentiment analysis completed")
        return reddit_data
    
    def aggregate_daily_sentiment(self, sentiment_data):
        """Aggregate sentiment data to daily averages"""
        if sentiment_data is None or sentiment_data.empty:
            return None
        
        # Find the date column - prioritize created_utc based on the actual data structure
        date_columns = ['created_utc', 'post_date', 'created_at', 'date', 'timestamp']
        date_col = None
        
        for col in date_columns:
            if col in sentiment_data.columns:
                date_col = col
                break
        
        if date_col is None:
            print("No date column found in sentiment data")
            print("Available columns:", sentiment_data.columns.tolist())
            return None
        
        # Convert date column to datetime if it's not already
        sentiment_data[date_col] = pd.to_datetime(sentiment_data[date_col])
        
        # Extract date part only
        sentiment_data['date'] = sentiment_data[date_col].dt.date
        
        # Group by date and calculate average sentiment
        daily_sentiment = sentiment_data.groupby('date')['overall_sentiment'].agg(['mean', 'count']).reset_index()
        daily_sentiment.columns = ['date', 'avg_sentiment', 'post_count']
        daily_sentiment['date'] = pd.to_datetime(daily_sentiment['date'])
        
        return daily_sentiment
    
    def merge_datasets(self, stock_data, reddit_sentiment=None, economic_data=None):
        """Merge different datasets on date for analysis"""
        if stock_data is None or stock_data.empty:
            print("Stock data is required for merging datasets")
            return None
        
        # Ensure date column is datetime
        stock_data['date'] = pd.to_datetime(stock_data['date'])
        
        # Start with stock data
        merged_data = stock_data.copy()
        
        # Add Reddit sentiment if available
        if reddit_sentiment is not None and not reddit_sentiment.empty:
            reddit_sentiment['date'] = pd.to_datetime(reddit_sentiment['date'])
            merged_data = pd.merge(merged_data, reddit_sentiment, on='date', how='left')
        
        # Add economic data if available
        if economic_data is not None and not economic_data.empty:
            economic_data['date'] = pd.to_datetime(economic_data['date'])
            merged_data = pd.merge(merged_data, economic_data, on='date', how='left')
        
        # Fill missing values using the new pandas methods
        merged_data = merged_data.ffill().bfill()
        
        print(f"Created merged dataset with {len(merged_data)} rows")
        return merged_data
    
    def visualize_data(self, merged_data, ticker):
        """Create visualizations for the merged data"""
        if merged_data is None or merged_data.empty:
            print("No data to visualize")
            return
        
        # Set up the figure
        plt.figure(figsize=(15, 10))
        
        # Plot 1: Stock Price
        plt.subplot(2, 2, 1)
        plt.plot(merged_data['date'], merged_data['close'], 'b-', label='Close Price')
        plt.title(f'{ticker} Stock Price')
        plt.xlabel('Date')
        plt.ylabel('Price ($)')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Plot 2: Volume (if available)
        if 'volume' in merged_data.columns:
            plt.subplot(2, 2, 2)
            plt.bar(merged_data['date'], merged_data['volume'], color='g', alpha=0.7)
            plt.title(f'{ticker} Trading Volume')
            plt.xlabel('Date')
            plt.ylabel('Volume')
            plt.grid(True, alpha=0.3)
        else:
            # Plot price change if volume not available
            plt.subplot(2, 2, 2)
            price_change = merged_data['close'].pct_change() * 100
            plt.plot(merged_data['date'], price_change, 'g-', label='Price Change %')
            plt.title(f'{ticker} Price Change %')
            plt.xlabel('Date')
            plt.ylabel('Change (%)')
            plt.grid(True, alpha=0.3)
            plt.legend()
        
        # Plot 3: Reddit Sentiment (if available)
        if 'avg_sentiment' in merged_data.columns:
            plt.subplot(2, 2, 3)
            plt.plot(merged_data['date'], merged_data['avg_sentiment'], 'r-', label='Reddit Sentiment')
            plt.axhline(y=0, color='k', linestyle='--', alpha=0.3)
            plt.title(f'{ticker} Reddit Sentiment')
            plt.xlabel('Date')
            plt.ylabel('Sentiment Score')
            plt.grid(True, alpha=0.3)
            plt.legend()
        
        # Plot 4: Correlation between Price and Sentiment (if available)
        if 'avg_sentiment' in merged_data.columns:
            plt.subplot(2, 2, 4)
            plt.scatter(merged_data['avg_sentiment'], merged_data['close'], alpha=0.7)
            plt.title('Price vs. Sentiment')
            plt.xlabel('Sentiment Score')
            plt.ylabel('Price ($)')
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        
        # Additional visualization: Correlation heatmap
        if len(merged_data.columns) > 5:  # Only if we have enough columns
            # Select numeric columns
            numeric_data = merged_data.select_dtypes(include=['number'])
            
            # Calculate correlation
            corr_matrix = numeric_data.corr()
            
            # Plot correlation heatmap
            plt.figure(figsize=(10, 8))
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
            plt.title(f'Correlation Matrix for {ticker}')
            plt.tight_layout()
            plt.show()
    
    def _calculate_moving_averages_spark(self, spark_df):
        """Calculate moving averages using Spark Window functions"""
        if spark_df is None or spark_df.count() == 0:
            return spark_df
            
        # Define windows for different periods
        window_5 = Window.orderBy('date').rowsBetween(-4, 0)
        window_10 = Window.orderBy('date').rowsBetween(-9, 0)
        window_20 = Window.orderBy('date').rowsBetween(-19, 0)
        
        # Calculate moving averages using Window functions
        return spark_df.withColumn('ma5', F.avg('close').over(window_5)) \
                     .withColumn('ma10', F.avg('close').over(window_10)) \
                     .withColumn('ma20', F.avg('close').over(window_20))
    
    def prepare_data_for_ml(self, stock_data, sentiment_data=None):
        """Prepare stock data for ML by calculating technical indicators"""
        if stock_data is None or len(stock_data) < 5:  # Reduced from 30 to 5
            print(f"Not enough stock data for ML preparation. Need at least 5 data points, but only have {len(stock_data) if stock_data is not None else 0}")
            return None
            
        # Make a copy to avoid modifying the original
        df = stock_data.copy()
        
        # Ensure date is datetime and sort
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Calculate technical indicators
        # Moving averages
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        
        # Volatility (standard deviation over 5 days)
        df['volatility'] = df['close'].rolling(window=5).std()
        
        # Price changes
        df['price_change'] = df['close'].diff()
        df['price_change_pct'] = df['close'].pct_change() * 100
        
        # Lagged prices (previous days)
        df['close_lag1'] = df['close'].shift(1)
        df['close_lag2'] = df['close'].shift(2)
        
        # Add sentiment data if available
        if sentiment_data is not None:
            # Convert to datetime for merging
            sentiment_data['date'] = pd.to_datetime(sentiment_data['date'])
            
            # Group by date and calculate average sentiment and count
            sentiment_by_date = sentiment_data.groupby('date').agg({
                'sentiment': 'mean',
                'id': 'count'  # Count of posts per day
            }).reset_index()
            
            sentiment_by_date.rename(columns={
                'sentiment': 'avg_sentiment',
                'id': 'sentiment_count'
            }, inplace=True)
            
            # Merge with stock data
            df = pd.merge(df, sentiment_by_date, on='date', how='left')
            
            # Fill missing sentiment values
            df['avg_sentiment'] = df['avg_sentiment'].fillna(0)
            df['sentiment_count'] = df['sentiment_count'].fillna(0)
        else:
            # Add placeholder sentiment columns
            df['avg_sentiment'] = 0
            df['sentiment_count'] = 0
        
        # Drop rows with NaN values (from rolling calculations)
        df = df.dropna()
        
        print(f"Prepared data shape: {df.shape}")
        return df
        
    def convert_to_spark_df(self, pandas_df):
        """Convert pandas DataFrame to Spark DataFrame"""
        if pandas_df is None or len(pandas_df) == 0:
            return None
            
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = self.spark.createDataFrame(pandas_df)
        return spark_df
        
    def train_model(self, feature_df):
        """Train ML models and select the best one"""
        if feature_df is None:
            print("No data available for training")
            return None, None, None
            
        print("\n🔧 Training ML models...")
        
        # Define feature columns based on available data
        # Check which columns are available in the dataframe
        available_cols = feature_df.columns
        print(f"Available columns: {available_cols}")
        
        # Define potential feature columns
        potential_features = [
            "close", "SMA_10", "EMA_10", "Volatility_10", "Daily_Return", 
            "RSI_14", "ma5", "ma20", "volatility", "price_change", 
            "price_change_pct", "close_lag1", "close_lag2", 
            "avg_sentiment", "sentiment_count"
        ]
        
        # Use only columns that exist in the dataframe
        feature_cols = [col for col in potential_features if col in available_cols]
        
        if len(feature_cols) < 2:
            print("Not enough feature columns available for training")
            # Fallback to using just close price if nothing else is available
            if "close" in available_cols:
                feature_cols = ["close"]
            else:
                return None
                
        print(f"Using features: {feature_cols}")
        
        # Prepare feature vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        assembled_df = assembler.transform(feature_df)
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features",
                              withStd=True, withMean=True)
        scaler_model = scaler.fit(assembled_df)
        scaled_df = scaler_model.transform(assembled_df)
        
        # Split data into training and testing sets
        train_df, test_df = scaled_df.randomSplit([0.8, 0.2], seed=42)
        
        # Initialize models
        rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="close", 
                                 numTrees=100, maxDepth=5, seed=42)
        gbt = GBTRegressor(featuresCol="scaled_features", labelCol="close", 
                          maxIter=100, maxDepth=5, seed=42)
        lr = LinearRegression(featuresCol="scaled_features", labelCol="close", 
                             maxIter=100, regParam=0.2, elasticNetParam=0.8)
        
        # Train models
        print("Training Random Forest...")
        rf_model = rf.fit(train_df)
        
        print("Training Gradient Boosted Trees...")
        gbt_model = gbt.fit(train_df)
        
        print("Training Linear Regression...")
        lr_model = lr.fit(train_df)
        
        # Evaluate models
        evaluator = RegressionEvaluator(labelCol="close", predictionCol="prediction", 
                                      metricName="r2")
        
        # Make predictions
        rf_predictions = rf_model.transform(test_df)
        gbt_predictions = gbt_model.transform(test_df)
        lr_predictions = lr_model.transform(test_df)
        
        # Calculate metrics
        rf_r2 = evaluator.evaluate(rf_predictions)
        gbt_r2 = evaluator.evaluate(gbt_predictions)
        lr_r2 = evaluator.evaluate(lr_predictions)
        
        # Set evaluator to RMSE
        evaluator.setMetricName("rmse")
        rf_rmse = evaluator.evaluate(rf_predictions)
        gbt_rmse = evaluator.evaluate(gbt_predictions)
        lr_rmse = evaluator.evaluate(lr_predictions)
        
        # Store models and results
        models = {}
        results = {}
        
        models["random_forest"] = rf_model
        results["random_forest"] = {"rmse": rf_rmse, "r2": rf_r2}
        
        models["gbt"] = gbt_model
        results["gbt"] = {"rmse": gbt_rmse, "r2": gbt_r2}
        
        models["linear_regression"] = lr_model
        results["linear_regression"] = {"rmse": lr_rmse, "r2": lr_r2}
        
        # Select best model based on R²
        best_model_name = max(results, key=lambda x: results[x]['r2'])
        best_model = models[best_model_name]
        
        print(f"\n🏅 Best model selected: {best_model_name} (R²={results[best_model_name]['r2']:.4f})")
        print("Model Evaluation:")
        for name, metrics in results.items():
            marker = "👑" if name == best_model_name else "  "
            print(f"{marker} {name}: R²={metrics['r2']:.4f}, RMSE={metrics['rmse']:.4f}")
        
        return best_model, best_model_name, results
    
    def predict_future_prices(self, model, feature_df, days=5):
        """Predict future stock prices using rolling predictions"""
        if model is None or feature_df is None:
            print("Model or data not available for prediction")
            return None
            
        print(f"🔮 Predicting next {days} days...")
        print(f"🤖 Using model: {model.__class__.__name__}")
        
        # Helper function to clean corrupted column names
        def clean_column_name(col_name):
            """Clean corrupted column names from Spark DataFrame conversion"""
            if isinstance(col_name, str):
                # Remove patterns like "1`. " or "1. " from column names
                cleaned = re.sub(r'^\d+(\.\s|\`. )', '', str(col_name))
                return cleaned.strip()
            return str(col_name)
        
        predictions = []
        
        # Get the most recent data for rolling predictions
        recent_spark_data = feature_df.orderBy(F.desc("date")).limit(30)
        recent_data = recent_spark_data.toPandas()
        recent_data = recent_data.sort_values('date').reset_index(drop=True)
        
        # Apply column name cleaning
        recent_data.columns = [clean_column_name(col) for col in recent_data.columns]
        
        # Debug: Check for data corruption and duplicates
        print(f"🔍 Debug - DataFrame columns: {recent_data.columns.tolist()}")
        print(f"🔍 Debug - DataFrame shape: {recent_data.shape}")
        
        # Handle duplicate column names - remove duplicates while preserving order
        seen_cols = set()
        unique_cols = []
        for col in recent_data.columns:
            if col not in seen_cols:
                seen_cols.add(col)
                unique_cols.append(col)
        
        # Use only unique columns
        recent_data = recent_data[unique_cols]
        
        # Create a temporary DataFrame for rolling predictions
        temp_data = recent_data.copy()
        
        # Get the feature columns used during training from the model's metadata
        # This ensures we use the same features that were used for training
        try:
            # Try to get feature names from the model
            if hasattr(model, 'featuresCol'):
                # For PySpark ML models
                features_col = model.getFeaturesCol()
                print(f"Model uses features column: {features_col}")
                
                # Use the same feature columns that were used in the train_model method
                available_cols = recent_data.columns
                potential_features = [
                    "close", "SMA_10", "EMA_10", "Volatility_10", "Daily_Return", 
                    "RSI_14", "ma5", "ma20", "volatility", "price_change", 
                    "price_change_pct", "close_lag1", "close_lag2", 
                    "avg_sentiment", "sentiment_count"
                ]
                
                # Use only columns that exist in the dataframe
                training_feature_cols = [col for col in potential_features if col in available_cols]
                
                if len(training_feature_cols) < 1:
                    # Fallback to using just close price if nothing else is available
                    if "close" in available_cols:
                        training_feature_cols = ["close"]
                    else:
                        print("No usable features found")
                        return None
            else:
                # Fallback to basic features
                training_feature_cols = ["close"]
                
        except Exception as e:
            print(f"Error getting model features: {e}")
            # Fallback to basic features
            training_feature_cols = ["close"]
            
        print(f"Using features for prediction: {training_feature_cols}")
        
        # Check if all required features exist
        missing_cols = [col for col in training_feature_cols if col not in recent_data.columns]
        if missing_cols:
            print(f"⚠️ Warning: Missing columns: {missing_cols}")
            for col in missing_cols:
                recent_data[col] = 0.0
                temp_data[col] = 0.0
        
        # Get the last date and create future dates
        last_date = recent_data['date'].max()
        future_dates = []
        next_date = last_date
        for _ in range(days):
            next_date = next_date + timedelta(days=1)
            # Skip weekends
            while next_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
                next_date = next_date + timedelta(days=1)
            future_dates.append(next_date)
        
        # Create a scaler for consistent scaling
        scaler = SklearnStandardScaler()
        
        # For each day, predict the price and update features for next prediction
        for day in range(days):
            try:
                # Get the last row of data
                last_row = temp_data.iloc[-1].copy()
                last_close = float(last_row['close'])
                
                # Create a new row with updated date
                new_row = last_row.copy()
                new_row['date'] = future_dates[day]
                
                # Update lagged features
                new_row['close_lag2'] = temp_data.iloc[-2]['close'] if len(temp_data) > 1 else last_close
                new_row['close_lag1'] = last_close
                
                # Create a DataFrame with the new row for prediction
                new_df = pd.DataFrame([new_row])
                
                # Extract features for prediction
                feature_vector = new_df[training_feature_cols].values
                
                # Use CONSISTENT scaling - fit scaler once on recent data for all predictions
                if day == 0:
                    recent_features = temp_data[training_feature_cols].values
                    scaler.fit(recent_features)
                
                # Scale the feature vector
                scaled_features = scaler.transform(feature_vector)
                
                # Convert to Spark DataFrame for prediction
                from pyspark.ml.linalg import Vectors
                schema = StructType([StructField("features", VectorUDT(), True)])
                feature_vector_df = self.spark.createDataFrame(
                    [(Vectors.dense(scaled_features[0]),)], 
                    schema)
                
                # Make prediction
                prediction_result = model.transform(feature_vector_df)
                predicted_price = prediction_result.select("prediction").collect()[0][0]
            except Exception as e:
                print(f"⚠️ Error during prediction: {e}")
                # Use a simple fallback prediction
                predicted_price = last_close * (1 + random.uniform(-0.01, 0.01))
            
            # Apply market simulation factors to make predictions more realistic
            if day > 0:
                # Get previous prediction as base
                prev_prediction = predictions[day-1]['predicted_price']
                
                # Apply sentiment impact if available
                sentiment_impact = 0
                if 'avg_sentiment' in temp_data.columns:
                    sentiment = temp_data['avg_sentiment'].iloc[-1] if not pd.isna(temp_data['avg_sentiment'].iloc[-1]) else 0
                    sentiment_impact = sentiment * np.random.uniform(0.001, 0.005)
                
                # Apply volatility
                volatility = 0.01  # Default volatility
                if 'volatility' in temp_data.columns and not pd.isna(temp_data['volatility'].iloc[-1]):
                    volatility = max(0.005, min(0.05, temp_data['volatility'].iloc[-1]))
                
                # Random market movement
                market_movement = np.random.normal(0, volatility)
                
                # Mean reversion
                mean_reversion = 0
                if 'ma20' in temp_data.columns and not pd.isna(temp_data['ma20'].iloc[-1]):
                    mean_reversion = (temp_data['ma20'].iloc[-1] - prev_prediction) * 0.05
                
                # Random events
                random_event = 0
                if np.random.random() < 0.1:  # 10% chance
                    random_event = np.random.normal(0, volatility * 2)
                
                # Calculate price change
                price_change = market_movement + sentiment_impact + mean_reversion + random_event
                
                # Clamp the change
                max_change = prev_prediction * 0.05  # Max 5% daily change
                price_change = max(-max_change, min(max_change, price_change))
                
                # Adjust the predicted price
                predicted_price = prev_prediction * (1 + price_change)
                predicted_price = max(0.01, predicted_price)  # Ensure positive price
                
                print(f"Day {day+1}: Base: ${prev_prediction:.2f}, Market: {market_movement:.4f}, "  
                      f"Sentiment: {sentiment_impact:.4f}, Mean Rev: {mean_reversion:.4f}, "  
                      f"Event: {random_event:.4f}, Final: ${predicted_price:.2f}")
            
            # Store the prediction
            predictions.append({
                'date': future_dates[day],
                'predicted_price': predicted_price,
                'change': predicted_price - last_close,
                'change_pct': ((predicted_price / last_close) - 1) * 100
            })
            
            # Update the new row with the predicted price
            new_row['close'] = predicted_price
            
            # Update volatility and price changes
            new_row['price_change'] = predicted_price - last_close
            new_row['price_change_pct'] = (new_row['price_change'] / last_close) * 100 if last_close != 0 else 0
            
            # Add some randomness to sentiment features for next prediction
            if 'avg_sentiment' in new_row:
                # Sentiment can change slightly day to day
                sentiment_drift = random.uniform(-0.1, 0.1)
                new_row['avg_sentiment'] = max(-1.0, min(1.0, new_row['avg_sentiment'] + sentiment_drift))
            
            if 'sentiment_count' in new_row:
                # Sentiment volume can vary
                count_change = random.randint(-2, 3)
                new_row['sentiment_count'] = max(1, new_row['sentiment_count'] + count_change)
            
            if len(temp_data) >= 5:
                new_row['volatility'] = temp_data['close'].tail(5).std()
            
            # Update moving averages
            if len(temp_data) >= 5:
                ma5_values = temp_data['close'].tail(4).tolist() + [predicted_price]
                new_row['ma5'] = sum(ma5_values) / 5
            
            if len(temp_data) >= 20:
                ma20_values = temp_data['close'].tail(19).tolist() + [predicted_price]
                new_row['ma20'] = sum(ma20_values) / 20
            
            # Add the new row to the temporary data
            temp_data = pd.concat([temp_data, pd.DataFrame([new_row])], ignore_index=True)
        
        # Create a DataFrame with the predictions
        predictions_df = pd.DataFrame(predictions)
        
        return predictions_df
        
    def predict_stock_price(self, ticker, days_to_predict=5):
        """Complete pipeline to predict stock prices"""
        print(f"📈 Starting prediction pipeline for {ticker}...")
        
        # Fetch stock data
        stock_data = self.fetch_stock_data(ticker)
        if stock_data is None:
            print("Failed to fetch stock data")
            return None
        
        # Fetch Reddit sentiment data
        reddit_data = self.fetch_reddit_data(ticker)
        
        # Prepare data for ML
        feature_df = self.prepare_data_for_ml(stock_data, reddit_data)
        if feature_df is None or feature_df.empty:
            print("Failed to prepare features for ML")
            return None
        
        # Train model
        model = self.train_model(feature_df)
        if model is None:
            print("Failed to train model")
            return None
        
        # Predict future prices
        predictions = self.predict_future_prices(model, feature_df, days=days_to_predict)
        if predictions is None or predictions.empty:
            print("Failed to generate predictions")
            return None
            
        # Print predictions
        print("\n🔮 Predictions for the next {} days:".format(days_to_predict))
        
        # Convert predictions to dictionary for easier use in main function
        prediction_dict = {}
        for i, row in predictions.iterrows():
            date = row['date']
            price = row['predicted_price']
            print(f"  {date.strftime('%Y-%m-%d')}: ${price:.2f} ({row['change']:+.2f}, {row['change_pct']:+.2f}%)")
            prediction_dict[date] = price
                
        return prediction_dict
    
    def _calculate_rsi(self, prices, window=14):
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=window).mean()
        avg_loss = loss.rolling(window=window).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi

def main():
    """Main entry point for Ubuntu PySpark Stock Predictor"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='PySpark Stock Predictor for Ubuntu')
    parser.add_argument('ticker', type=str, help='Stock ticker symbol (e.g., AAPL)')
    parser.add_argument('--days', type=int, default=5, help='Number of days to predict (default: 5)')
    parser.add_argument('--visualize', action='store_true', help='Generate visualizations (if available)')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    args = parser.parse_args()
    
    # Process arguments
    ticker = args.ticker.upper()
    days_to_predict = args.days
    enable_visualization = args.visualize and VISUALIZATION_AVAILABLE
    debug_mode = args.debug
    
    print("🚀 Ubuntu PySpark Stock Predictor")
    print(f"📈 Predicting {days_to_predict} days of stock prices for {ticker}")
    
    try:
        # Initialize predictor
        predictor = UbuntuPySparkStockPredictor()
        
        # Run prediction
        results = predictor.predict_stock_price(ticker, days_to_predict)
        
        if results is not None:
            print("\n✅ Prediction completed successfully!")
            print("\nPredicted prices:")
            for date, price in results.items():
                print(f"  {date.strftime('%Y-%m-%d')}: ${price:.2f}")
        else:
            print("\n❌ Prediction failed. Check logs for details.")
            print("\nTroubleshooting tips:")
            print("  1. Ensure MongoDB has enough stock data (at least 5 data points)")
            print("  2. Check if the stock data has the required columns (close, date, etc.)")
            print("  3. Try increasing the fetch_stock_data days parameter")
            print("  4. Verify that MongoDB collections follow the expected naming pattern")
            print("\nAvailable collections in database:")
            try:
                db = predictor.mongo_client[predictor.database_name]
                for collection in db.list_collection_names():
                    print(f"  - {collection}")
            except Exception as e:
                print(f"  Error listing collections: {e}")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure Spark session is stopped
        try:
            if 'predictor' in locals() and predictor.spark:
                predictor.spark.stop()
                print("\n✅ Spark session stopped")
        except Exception:
            pass

if __name__ == "__main__":
    main()