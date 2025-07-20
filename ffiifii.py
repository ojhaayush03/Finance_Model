#!/usr/bin/env python3
"""
Financial Analysis for Google Colab

This script performs sentiment analysis on Reddit data for a specific ticker,
and provides data visualization and prediction using Alpha Vantage and FRED data.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from datetime import datetime, timedelta
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

# Download NLTK resources for sentiment analysis
try:
    nltk.download('vader_lexicon', quiet=True)
except:
    print("NLTK download failed, but continuing...")

# MongoDB connection (hardcoded as requested)
MONGODB_URI = "mongodb+srv://ayushojha9998:4690@cluster0.rj86jwm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGODB_DB_NAME = "financial_db"

class FinancialAnalyzer:
    def __init__(self):
        """Initialize the financial analyzer with MongoDB connection"""
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DB_NAME]
        self.sia = SentimentIntensityAnalyzer()
        print("Financial Analyzer initialized")

        # List available collections
        print("\nAvailable collections in database:")
        collections = self.db.list_collection_names()
        for collection in collections:
            print(f"  - {collection}")

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
                print(f"No Reddit data collection found for {ticker}. Available collections:")
                for coll in self.db.list_collection_names():
                    if 'reddit' in coll.lower():
                        print(f"  - {coll}")
                return None

            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query for data - first try to filter by ticker, then by date
            base_query = {"ticker": ticker.upper()}

            # Try different date field names
            date_fields = ["created_utc", "post_date", "date", "timestamp"]
            query = base_query.copy()

            # Add date filter if possible
            for date_field in date_fields:
                sample = collection.find_one()
                if sample and date_field in sample:
                    query[date_field] = {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                    break

            # Fetch and convert to DataFrame
            cursor = collection.find(query)
            reddit_data = pd.DataFrame(list(cursor))

            if reddit_data.empty:
                # Try without date filter if no results
                cursor = collection.find(base_query)
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
            import traceback
            traceback.print_exc()
            return None

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
                print(f"No stock data collection found for {ticker}. Available collections:")
                for coll in self.db.list_collection_names():
                    if 'stock' in coll.lower():
                        print(f"  - {coll}")
                return None

            print(f"Using collection: {collection_name}")

            # Calculate the date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Query for data within the date range
            # The date field appears to be a datetime object based on the image
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

    def predict_stock_price(self, merged_data, days_to_predict=5):
        """Predict future stock prices based on historical data and sentiment"""
        if merged_data is None or merged_data.empty:
            print("No data for prediction")
            return None

        # Prepare features
        feature_cols = []

        # Check if required columns exist
        required_cols = ['close']  # Only close price is truly required
        optional_cols = ['volume', 'open', 'high', 'low']

        for col in required_cols:
            if col not in merged_data.columns:
                print(f"Required column '{col}' not found in data")
                print("Available columns:", merged_data.columns.tolist())
                return None
            feature_cols.append(col)

        # Add optional columns if they exist
        for col in optional_cols:
            if col in merged_data.columns:
                feature_cols.append(col)
            else:
                print(f"Optional column '{col}' not found, skipping...")

        # Add sentiment features if available
        if 'avg_sentiment' in merged_data.columns:
            feature_cols.append('avg_sentiment')

        # Add economic indicators if available
        economic_indicators = [col for col in merged_data.columns if col.startswith('economic_')]
        feature_cols.extend(economic_indicators)

        # Add technical indicators
        merged_data['ma5'] = merged_data['close'].rolling(window=5).mean()
        merged_data['ma20'] = merged_data['close'].rolling(window=20).mean()
        merged_data['rsi'] = self._calculate_rsi(merged_data['close'])

        feature_cols.extend(['ma5', 'ma20', 'rsi'])

        # Add lag features
        for lag in range(1, 6):
            merged_data[f'close_lag_{lag}'] = merged_data['close'].shift(lag)
            feature_cols.append(f'close_lag_{lag}')

        # Drop NaN values
        merged_data = merged_data.dropna()

        if merged_data.empty:
            print("Not enough data for prediction after adding features")
            return None

        # Prepare target and features
        X = merged_data[feature_cols].values
        y = merged_data['close'].values

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)

        # Evaluate model
        y_pred = model.predict(X_test_scaled)
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        print(f"Model Evaluation:")
        print(f"RMSE: ${rmse:.2f}")
        print(f"MAE: ${mae:.2f}")
        print(f"R²: {r2:.4f}")

        # Visualize predictions vs actual
        plt.figure(figsize=(12, 6))
        plt.plot(merged_data['date'].iloc[-len(y_test):], y_test, 'b-', label='Actual')
        plt.plot(merged_data['date'].iloc[-len(y_pred):], y_pred, 'r--', label='Predicted')
        plt.title('Stock Price Prediction vs Actual')
        plt.xlabel('Date')
        plt.ylabel('Price ($)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.show()

        # Predict future prices
        last_data = merged_data.iloc[-1:][feature_cols].values
        future_predictions = []

        for _ in range(days_to_predict):
            # Scale the last data point
            last_data_scaled = scaler.transform(last_data)
                        # Make prediction
            next_pred = model.predict(last_data_scaled)[0]
            future_predictions.append(next_pred)

            # Update last_data for next prediction
            new_row = last_data.copy()
            new_row[0][0] = next_pred  # Update close price

            # Shift lag features
            for i in range(4, 0, -1):
                new_row[0][feature_cols.index(f'close_lag_{i+1}')] = new_row[0][feature_cols.index(f'close_lag_{i}')]
            new_row[0][feature_cols.index('close_lag_1')] = new_row[0][0]  # current close becomes lag_1

            last_data = new_row

        # Create future dates
        last_date = merged_data['date'].iloc[-1]
        future_dates = [last_date + timedelta(days=i+1) for i in range(days_to_predict)]

        # Plot future predictions
        plt.figure(figsize=(12, 6))
        plt.plot(merged_data['date'].iloc[-30:], merged_data['close'].iloc[-30:], 'b-', label='Historical')
        plt.plot(future_dates, future_predictions, 'r--', label='Forecast')
        plt.title('Stock Price Forecast')
        plt.xlabel('Date')
        plt.ylabel('Price ($)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.show()

        return future_predictions, future_dates

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
    # Initialize the analyzer
    analyzer = FinancialAnalyzer()

    # Get user input
    ticker = input("Enter ticker symbol (e.g., AAPL, MSFT, GOOGL): ")
    days_str = input("Enter number of days for analysis (default: 30): ")
    days = int(days_str) if days_str.strip() else 30

    print("\n" + "=" * 50)
    print(f"ANALYZING {ticker} FOR THE PAST {days} DAYS")
    print("=" * 50 + "\n")

    # Fetch data
    stock_data = analyzer.fetch_stock_data(ticker, days)
    reddit_data = analyzer.fetch_reddit_data(ticker, days)
    economic_data = analyzer.fetch_economic_data(days)

    # Check if we have stock data
    if stock_data is None:
        print(f"Cannot proceed with analysis for {ticker}: No stock data available")
        return

    # Analyze sentiment if Reddit data is available
    sentiment_data = None
    if reddit_data is not None:
        sentiment_data = analyzer.analyze_reddit_sentiment(reddit_data)
        if sentiment_data is not None:
            sentiment_data = analyzer.aggregate_daily_sentiment(sentiment_data)

    # Merge datasets
    merged_data = analyzer.merge_datasets(stock_data, sentiment_data, economic_data)

    if merged_data is not None:
        # Visualize data
        analyzer.visualize_data(merged_data, ticker)

        # Predict stock prices
        analyzer.predict_stock_price(merged_data)

if __name__ == "__main__":
    main()