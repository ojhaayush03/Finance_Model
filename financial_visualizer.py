#!/usr/bin/env python3
"""
Financial Visualizer for Stock Prediction

This module provides enhanced visualization capabilities for stock data
before running the prediction pipeline.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from datetime import datetime, timedelta
import io
import base64
import traceback

class FinancialVisualizer:
    def __init__(self, mongo_client=None, mongodb_uri=None, database_name="financial_db"):
        """Initialize the financial visualizer with MongoDB connection"""
        if mongo_client:
            self.mongo_client = mongo_client
        elif mongodb_uri:
            self.mongo_client = MongoClient(mongodb_uri)
        else:
            raise ValueError("Either mongo_client or mongodb_uri must be provided")
            
        self.db = self.mongo_client[database_name]
        print("Financial Visualizer initialized")
    
    def fetch_stock_data(self, ticker, days=30):
        """Fetch stock data for a given ticker"""
        try:
            # Try multiple possible collection name patterns
            possible_names = [
                f"{ticker.lower()}_stock_data",
                f"stock_data.{ticker.lower()}",
                f"stock_data_{ticker.lower()}",
                f"{ticker.upper()}_stock_data"
            ]
            
            collection = None
            collection_name = None
            
            for name in possible_names:
                if name in self.db.list_collection_names():
                    collection_name = name
                    collection = self.db[name]
                    break
            
            if collection is None:
                print(f"No stock data collection found for {ticker}")
                return None
            
            print(f"Using collection: {collection_name}")
            
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Query for data within the date range
            query = {
                "date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }
            
            # Fetch and convert to DataFrame
            cursor = collection.find(query).sort("date", 1)  # Sort by date ascending
            stock_data = pd.DataFrame(list(cursor))
            
            if stock_data.empty:
                # Try without date filter to see if any data exists
                cursor = collection.find().sort("date", -1).limit(days * 2)  # Get more data for better analysis
                stock_data = pd.DataFrame(list(cursor))
                stock_data = stock_data.sort_values("date").reset_index(drop=True)
                
                if stock_data.empty:
                    print(f"No stock data found for {ticker} at all")
                    return None
            
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
                
                # Remove MongoDB _id column if present
                if '_id' in stock_data.columns:
                    stock_data.drop('_id', axis=1, inplace=True)
            
            return stock_data if not stock_data.empty else None
        
        except Exception as e:
            print(f"Error fetching stock data: {str(e)}")
            traceback.print_exc()
            return None
    
    def fetch_reddit_data(self, ticker, days=30):
        """Fetch Reddit data for a given ticker"""
        try:
            # Try multiple possible collection name patterns
            possible_names = [
                f"reddit_data.{ticker.lower()}",
                f"reddit_data_{ticker.lower()}", 
                "reddit_data"
            ]
            
            collection = None
            collection_name = None
            
            for name in possible_names:
                if name in self.db.list_collection_names():
                    collection_name = name
                    collection = self.db[name]
                    break
            
            if collection is None:
                print(f"No Reddit data collection found for {ticker}.")
                return None
            
            print(f"Using Reddit collection: {collection_name}")
            
            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Try different date field names
            date_fields = ["created_utc", "post_date", "date", "timestamp"]
            date_field = None
            
            # Find the correct date field
            sample = collection.find_one({"ticker": ticker.upper()})
            if not sample:
                sample = collection.find_one()
                
            if sample:
                for field in date_fields:
                    if field in sample:
                        date_field = field
                        break
            
            # Query for data - first try to filter by ticker and date
            if date_field:
                query = {
                    "ticker": ticker.upper(),
                    date_field: {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            else:
                query = {"ticker": ticker.upper()}
            
            # Fetch and convert to DataFrame
            cursor = collection.find(query)
            reddit_data = pd.DataFrame(list(cursor))
            
            if reddit_data.empty:
                # Try without date filter if no results
                cursor = collection.find({"ticker": ticker.upper()})
                reddit_data = pd.DataFrame(list(cursor))
                
                if reddit_data.empty:
                    print(f"No Reddit data found for {ticker}")
                    return None
                else:
                    print(f"Found {len(reddit_data)} Reddit posts for {ticker} (all time)")
            else:
                print(f"Fetched {len(reddit_data)} Reddit posts for {ticker} in the last {days} days")
            
            return reddit_data
        
        except Exception as e:
            print(f"Error fetching Reddit data: {str(e)}")
            traceback.print_exc()
            return None
    
    def analyze_reddit_sentiment(self, reddit_data):
        """Analyze sentiment in Reddit posts using NLTK VADER"""
        try:
            from nltk.sentiment.vader import SentimentIntensityAnalyzer
            import nltk
            
            # Download VADER lexicon if needed
            try:
                nltk.data.find('sentiment/vader_lexicon.zip')
            except LookupError:
                nltk.download('vader_lexicon', quiet=True)
            
            sia = SentimentIntensityAnalyzer()
            
            if reddit_data is None or reddit_data.empty:
                print("No Reddit data to analyze")
                return None
            
            print(f"Analyzing sentiment for {len(reddit_data)} Reddit posts...")
            
            # Find text columns to analyze
            text_columns = []
            for col in ['title', 'text', 'body', 'content', 'selftext']:
                if col in reddit_data.columns:
                    text_columns.append(col)
            
            if not text_columns:
                print("No text columns found in Reddit data")
                return None
            
            # Create a copy to avoid modifying the original
            sentiment_data = reddit_data.copy()
            
            # Calculate sentiment for each text column
            for col in text_columns:
                sentiment_data[f'{col}_sentiment'] = sentiment_data[col].apply(
                    lambda x: sia.polarity_scores(str(x))['compound'] if pd.notna(x) else 0.0
                )
            
            # Calculate overall sentiment as average of all text columns
            sentiment_cols = [f'{col}_sentiment' for col in text_columns]
            sentiment_data['overall_sentiment'] = sentiment_data[sentiment_cols].mean(axis=1)
            
            # Add sentiment category
            sentiment_data['sentiment_category'] = sentiment_data['overall_sentiment'].apply(
                lambda score: "positive" if score > 0.05 else ("negative" if score < -0.05 else "neutral")
            )
            
            print("Sentiment analysis completed")
            return sentiment_data
        
        except Exception as e:
            print(f"Error analyzing Reddit sentiment: {str(e)}")
            traceback.print_exc()
            return reddit_data  # Return original data if sentiment analysis fails
    
    def aggregate_daily_sentiment(self, sentiment_data):
        """Aggregate sentiment data to daily averages"""
        if sentiment_data is None or sentiment_data.empty or 'overall_sentiment' not in sentiment_data.columns:
            print("No sentiment data to aggregate")
            return None
        
        # Find date column
        date_columns = ['date', 'created_utc', 'timestamp', 'post_date']
        date_col = None
        
        for col in date_columns:
            if col in sentiment_data.columns:
                date_col = col
                break
        
        if date_col is None:
            print("No date column found in sentiment data")
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
    
    def create_technical_indicators(self, stock_data):
        """Create technical indicators for stock data"""
        if stock_data is None or stock_data.empty:
            return None
        
        # Create a copy to avoid modifying the original
        df = stock_data.copy()
        
        # Calculate moving averages
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        
        # Calculate RSI
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).fillna(0)
        loss = -delta.where(delta < 0, 0).fillna(0)
        
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        
        rs = avg_gain / avg_loss.replace(0, 0.001)  # Avoid division by zero
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # Calculate MACD
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        
        # Calculate Bollinger Bands
        df['bb_middle'] = df['close'].rolling(window=20).mean()
        df['bb_std'] = df['close'].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + 2 * df['bb_std']
        df['bb_lower'] = df['bb_middle'] - 2 * df['bb_std']
        
        # Calculate price change and volatility
        df['price_change'] = df['close'].pct_change() * 100
        df['volatility'] = df['close'].rolling(window=20).std() / df['close'].rolling(window=20).mean() * 100
        
        return df
    
    def merge_datasets(self, stock_data, reddit_sentiment=None):
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
        
        # Fill missing values
        merged_data = merged_data.ffill().bfill()
        
        print(f"Created merged dataset with {len(merged_data)} rows")
        return merged_data
    
    def _fig_to_base64(self, fig):
        """Convert matplotlib figure to base64 string"""
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        img_str = base64.b64encode(buf.read()).decode('utf-8')
        return img_str
    
    def generate_visualizations(self, ticker, days=30):
        """Generate comprehensive visualizations for a ticker and return base64 encoded images"""
        try:
            # Fetch data
            stock_data = self.fetch_stock_data(ticker, days)
            reddit_data = self.fetch_reddit_data(ticker, days)
            
            if stock_data is None or stock_data.empty:
                print(f"Cannot create visualizations for {ticker}: No stock data available")
                return None
            
            # Add technical indicators
            stock_data_with_indicators = self.create_technical_indicators(stock_data)
            
            # Analyze sentiment if Reddit data is available
            sentiment_data = None
            if reddit_data is not None:
                sentiment_data = self.analyze_reddit_sentiment(reddit_data)
                if sentiment_data is not None:
                    sentiment_data = self.aggregate_daily_sentiment(sentiment_data)
            
            # Merge datasets
            if sentiment_data is not None:
                merged_data = self.merge_datasets(stock_data_with_indicators, sentiment_data)
            else:
                merged_data = stock_data_with_indicators
            
            # Generate visualizations
            images = {}
            
            # 1. Price and Volume Chart
            fig1, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
            ax1.plot(merged_data['date'], merged_data['close'], 'b-', label='Close Price')
            ax1.set_title(f'{ticker} Stock Price', fontsize=16)
            ax1.set_ylabel('Price ($)', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.legend()
            
            if 'volume' in merged_data.columns:
                ax2.bar(merged_data['date'], merged_data['volume'], color='g', alpha=0.7)
                ax2.set_title('Trading Volume', fontsize=14)
                ax2.set_xlabel('Date', fontsize=12)
                ax2.set_ylabel('Volume', fontsize=12)
                ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            images['price_volume'] = self._fig_to_base64(fig1)
            plt.close(fig1)
            
            # 2. Technical Indicators Chart
            fig2, axs = plt.subplots(3, 1, figsize=(12, 12))
            
            # Moving Averages
            axs[0].plot(merged_data['date'], merged_data['close'], 'b-', label='Close')
            axs[0].plot(merged_data['date'], merged_data['ma5'], 'r-', label='MA5')
            axs[0].plot(merged_data['date'], merged_data['ma20'], 'g-', label='MA20')
            axs[0].set_title(f'{ticker} Moving Averages', fontsize=14)
            axs[0].set_ylabel('Price ($)', fontsize=12)
            axs[0].grid(True, alpha=0.3)
            axs[0].legend()
            
            # RSI
            axs[1].plot(merged_data['date'], merged_data['rsi'], 'purple')
            axs[1].axhline(y=70, color='r', linestyle='--', alpha=0.5)
            axs[1].axhline(y=30, color='g', linestyle='--', alpha=0.5)
            axs[1].set_title('RSI (14)', fontsize=14)
            axs[1].set_ylabel('RSI', fontsize=12)
            axs[1].grid(True, alpha=0.3)
            
            # MACD
            axs[2].plot(merged_data['date'], merged_data['macd'], 'b-', label='MACD')
            axs[2].plot(merged_data['date'], merged_data['macd_signal'], 'r-', label='Signal')
            axs[2].bar(merged_data['date'], 
                     merged_data['macd'] - merged_data['macd_signal'], 
                     color=['g' if x > 0 else 'r' for x in merged_data['macd'] - merged_data['macd_signal']], 
                     alpha=0.5)
            axs[2].set_title('MACD', fontsize=14)
            axs[2].set_xlabel('Date', fontsize=12)
            axs[2].set_ylabel('MACD', fontsize=12)
            axs[2].grid(True, alpha=0.3)
            axs[2].legend()
            
            plt.tight_layout()
            images['technical_indicators'] = self._fig_to_base64(fig2)
            plt.close(fig2)
            
            # 3. Bollinger Bands
            fig3, ax = plt.subplots(figsize=(12, 6))
            ax.plot(merged_data['date'], merged_data['close'], 'b-', label='Close')
            ax.plot(merged_data['date'], merged_data['bb_upper'], 'r--', label='Upper Band')
            ax.plot(merged_data['date'], merged_data['bb_middle'], 'g--', label='Middle Band')
            ax.plot(merged_data['date'], merged_data['bb_lower'], 'r--', label='Lower Band')
            ax.fill_between(merged_data['date'], merged_data['bb_upper'], merged_data['bb_lower'], alpha=0.1, color='gray')
            ax.set_title(f'{ticker} Bollinger Bands', fontsize=16)
            ax.set_xlabel('Date', fontsize=12)
            ax.set_ylabel('Price ($)', fontsize=12)
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            plt.tight_layout()
            images['bollinger_bands'] = self._fig_to_base64(fig3)
            plt.close(fig3)
            
            # 4. Reddit Sentiment Analysis (if available)
            if 'avg_sentiment' in merged_data.columns:
                fig4, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
                
                # Sentiment over time
                ax1.plot(merged_data['date'], merged_data['avg_sentiment'], 'r-')
                ax1.axhline(y=0, color='k', linestyle='--', alpha=0.3)
                ax1.set_title(f'{ticker} Reddit Sentiment', fontsize=16)
                ax1.set_ylabel('Sentiment Score', fontsize=12)
                ax1.grid(True, alpha=0.3)
                
                # Price and sentiment overlay
                ax2.plot(merged_data['date'], merged_data['close'], 'b-', label='Close Price')
                ax2_twin = ax2.twinx()
                ax2_twin.plot(merged_data['date'], merged_data['avg_sentiment'], 'r-', label='Sentiment')
                ax2.set_title('Price vs. Sentiment', fontsize=14)
                ax2.set_xlabel('Date', fontsize=12)
                ax2.set_ylabel('Price ($)', fontsize=12, color='b')
                ax2_twin.set_ylabel('Sentiment Score', fontsize=12, color='r')
                ax2.grid(True, alpha=0.3)
                
                # Combine legends
                lines1, labels1 = ax2.get_legend_handles_labels()
                lines2, labels2 = ax2_twin.get_legend_handles_labels()
                ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
                
                plt.tight_layout()
                images['sentiment_analysis'] = self._fig_to_base64(fig4)
                plt.close(fig4)
                
                # 5. Correlation heatmap
                numeric_data = merged_data.select_dtypes(include=['number'])
                # Remove unnecessary columns
                cols_to_drop = ['_id'] if '_id' in numeric_data.columns else []
                numeric_data = numeric_data.drop(columns=cols_to_drop, errors='ignore')
                
                # Calculate correlation
                corr_matrix = numeric_data.corr()
                
                # Plot correlation heatmap
                fig5 = plt.figure(figsize=(10, 8))
                sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
                plt.title(f'Correlation Matrix for {ticker}')
                plt.tight_layout()
                
                images['correlation_heatmap'] = self._fig_to_base64(fig5)
                plt.close(fig5)
            
            return images
        
        except Exception as e:
            print(f"Error generating visualizations: {str(e)}")
            traceback.print_exc()
            return None
