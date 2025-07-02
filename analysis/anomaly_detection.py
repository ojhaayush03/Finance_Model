"""
Anomaly Detection Module

This module identifies unusual patterns in stock behavior and sentiment data:
- Price anomalies (sudden spikes or drops)
- Volume anomalies (unusual trading volume)
- Sentiment anomalies (unusual social media sentiment)
- Correlation anomalies (breakdown in normal correlations)

It provides functions to detect and visualize anomalies in financial data.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np  # Used in calculations and array operations
import matplotlib.pyplot as plt
import seaborn as sns  # Used for enhanced visualizations
from datetime import datetime, timedelta  # Used for date operations

# Machine learning imports
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler

# For PySpark integration
try:
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler, StandardScaler as SparkStandardScaler
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import PCA
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import IntegerType, DoubleType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("PySpark not available, falling back to pandas implementation")

# Initialize Spark if available
if SPARK_AVAILABLE:
    try:
        spark = SparkSession.builder \
            .appName("Financial Anomaly Detection") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        print("PySpark session initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Spark: {str(e)}")
        SPARK_AVAILABLE = False

# Add parent directory to path to import from sibling directories
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analysis.correlation_analysis import CorrelationAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('anomaly_detection')

class AnomalyDetector:
    def __init__(self):
        """Initialize the anomaly detector"""
        self.correlation_analyzer = CorrelationAnalyzer()
        
        # Create directories for saving results
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.results_dir = os.path.join(project_dir, "analysis_results")
        os.makedirs(self.results_dir, exist_ok=True)
        
        logger.info("Anomaly detector initialized")
    
    def detect_price_anomalies(self, ticker, days=90, method='iforest', contamination=0.05):
        """
        Detect anomalies in stock price data using various algorithms
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to use
            method: Detection method ('iforest', 'lof', or 'kmeans')
            contamination: Expected proportion of outliers in the data
            
        Returns:
            DataFrame with original data and anomaly scores/labels
        """
        logger.info(f"Detecting price anomalies for {ticker} using {method}")
        
        # Fetch stock data
        stock_data = self.correlation_analyzer.fetch_stock_data(ticker, days)
        
        if stock_data is None or stock_data.empty:
            logger.error(f"No stock data available for {ticker}")
            return None
        
        # Extract features for anomaly detection
        features = pd.DataFrame()
        features['date'] = stock_data['date']
        features['close'] = stock_data['close']
        features['volume'] = stock_data['volume']
        
        # Add technical indicators as features
        features['close_pct_change'] = features['close'].pct_change()
        features['volume_pct_change'] = features['volume'].pct_change()
        
        # Add rolling statistics
        features['close_mean_5d'] = features['close'].rolling(window=5).mean()
        features['close_std_5d'] = features['close'].rolling(window=5).std()
        features['volume_mean_5d'] = features['volume'].rolling(window=5).mean()
        features['volume_std_5d'] = features['volume'].rolling(window=5).std()
        
        # Z-score for price
        features['close_zscore'] = (features['close'] - features['close_mean_5d']) / features['close_std_5d']
        
        # Z-score for volume
        features['volume_zscore'] = (features['volume'] - features['volume_mean_5d']) / features['volume_std_5d']
        
        # Drop NaN values
        features = features.dropna()
        
        # Extract numerical features for anomaly detection
        feature_cols = ['close', 'volume', 'close_pct_change', 'volume_pct_change',
                       'close_zscore', 'volume_zscore']
        
        # Use PySpark if available, otherwise fall back to sklearn
        if SPARK_AVAILABLE:
            try:
                # Convert pandas DataFrame to Spark DataFrame
                spark_df = spark.createDataFrame(features)
                
                # Assemble features into a vector
                assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
                assembled_df = assembler.transform(spark_df)
                
                # Scale features
                scaler = SparkStandardScaler(inputCol="features", outputCol="scaled_features", 
                                           withStd=True, withMean=True)
                scaler_model = scaler.fit(assembled_df)
                scaled_df = scaler_model.transform(assembled_df)
                
                # Apply anomaly detection based on method
                if method == 'kmeans':
                    # Use KMeans for anomaly detection
                    kmeans = KMeans(k=2, featuresCol="scaled_features", predictionCol="cluster")
                    model = kmeans.fit(scaled_df)
                    predictions = model.transform(scaled_df)
                    
                    # Calculate distance to cluster center as anomaly score
                    centers = model.clusterCenters()
                    
                    # Convert back to pandas for further processing
                    result_df = predictions.select("date", "close", "volume", "cluster").toPandas()
                    
                    # Merge back with original features
                    features = features.merge(result_df[["date", "cluster"]], on="date", how="left")
                    
                    # Determine which cluster represents anomalies (smaller cluster)
                    cluster_counts = features["cluster"].value_counts()
                    anomaly_cluster = cluster_counts.idxmin() if len(cluster_counts) > 1 else 1
                    
                    # Set anomaly flags
                    features["is_anomaly"] = (features["cluster"] == anomaly_cluster).astype(int)
                    features["anomaly_score"] = features["close_zscore"].abs() + features["volume_zscore"].abs()
                    
                else:
                    # For other methods, fall back to pandas implementation
                    logger.info(f"Method {method} not implemented in PySpark, falling back to pandas")
                    return self._detect_price_anomalies_pandas(features, feature_cols, method, contamination)
                    
            except Exception as e:
                logger.error(f"Error in PySpark anomaly detection: {str(e)}")
                logger.info("Falling back to pandas implementation")
                return self._detect_price_anomalies_pandas(features, feature_cols, method, contamination)
        else:
            # Use pandas implementation
            return self._detect_price_anomalies_pandas(features, feature_cols, method, contamination)
        
        # Count anomalies
        anomaly_count = features['is_anomaly'].sum()
        logger.info(f"Detected {anomaly_count} price anomalies out of {len(features)} data points using PySpark")
        
        return features
        
    def _detect_price_anomalies_pandas(self, features, feature_cols, method='iforest', contamination=0.05):
        """
        Pandas implementation of price anomaly detection
        """
        # Extract features
        X = features[feature_cols].values
        
        # Scale the features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Select anomaly detection model
        if method == 'iforest':
            model = IsolationForest(contamination=contamination, random_state=42)
        elif method == 'lof':
            model = LocalOutlierFactor(n_neighbors=20, contamination=contamination, novelty=True)
        else:
            logger.error(f"Unknown anomaly detection method for pandas: {method}")
            # Default to Isolation Forest
            model = IsolationForest(contamination=contamination, random_state=42)
        
        # Fit the model
        model.fit(X_scaled)
        
        # Predict anomalies (-1 for anomalies, 1 for normal)
        if method == 'lof':
            y_pred = model.predict(X_scaled)
        else:
            y_pred = model.predict(X_scaled)
        
        # Convert to binary labels (1 for anomalies, 0 for normal)
        features['is_anomaly'] = (y_pred == -1).astype(int)
        
        # Get anomaly scores
        if hasattr(model, 'decision_function'):
            # For Isolation Forest
            scores = model.decision_function(X_scaled)
            # Convert so that higher score = more anomalous
            features['anomaly_score'] = -scores
        elif hasattr(model, 'score_samples'):
            # For LOF
            scores = model.score_samples(X_scaled)
            # Convert so that higher score = more anomalous
            features['anomaly_score'] = -scores
        else:
            # Fallback to using z-scores
            features['anomaly_score'] = features['close_zscore'].abs() + features['volume_zscore'].abs()
        
        # Count anomalies
        anomaly_count = features['is_anomaly'].sum()
        logger.info(f"Detected {anomaly_count} price anomalies out of {len(features)} data points using pandas")
        
        return features
    
    def detect_volume_anomalies(self, ticker, days=90, threshold=3.0):
        """
        Detect anomalies in trading volume using statistical methods
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to use
            threshold: Z-score threshold for anomaly detection
            
        Returns:
            DataFrame with original data and volume anomaly flags
        """
        logger.info(f"Detecting volume anomalies for {ticker} with threshold {threshold}")
        
        # Fetch stock data
        stock_data = self.correlation_analyzer.fetch_stock_data(ticker, days)
        
        if stock_data is None or stock_data.empty:
            logger.error(f"No stock data available for {ticker}")
            return None
        
        # Convert date to datetime if it's not already
        stock_data['date'] = pd.to_datetime(stock_data['date'])
        
        # Sort by date
        stock_data = stock_data.sort_values('date')
        
        # Calculate rolling mean and standard deviation of volume
        stock_data['volume_mean_20d'] = stock_data['volume'].rolling(window=20).mean()
        stock_data['volume_std_20d'] = stock_data['volume'].rolling(window=20).std()
        
        # Calculate z-score
        stock_data['volume_zscore'] = (stock_data['volume'] - stock_data['volume_mean_20d']) / stock_data['volume_std_20d']
        
        # Flag anomalies based on z-score threshold
        stock_data['volume_anomaly'] = (stock_data['volume_zscore'].abs() > threshold).astype(int)
        
        # Drop NaN values
        stock_data = stock_data.dropna()
        
        anomaly_count = stock_data['volume_anomaly'].sum()
        logger.info(f"Detected {anomaly_count} volume anomalies out of {len(stock_data)} data points")
        
        return stock_data
    
    def detect_sentiment_anomalies(self, ticker, source='twitter', days=30, threshold=2.0):
        """
        Detect anomalies in social media sentiment
        
        Args:
            ticker: Stock ticker symbol
            source: Sentiment source ('twitter' or 'reddit')
            days: Number of days of historical data to use
            threshold: Z-score threshold for anomaly detection
            
        Returns:
            DataFrame with original data and sentiment anomaly flags
        """
        logger.info(f"Detecting {source} sentiment anomalies for {ticker}")
        
        # Fetch sentiment data
        sentiment_data = self.correlation_analyzer.fetch_sentiment_data(ticker, source, days)
        
        if sentiment_data is None or sentiment_data.empty:
            logger.error(f"No {source} sentiment data available for {ticker}")
            return None
        
        # Field name for date depends on the source
        date_field = "created_at" if source == "twitter" else "post_date"
        sentiment_field = "sentiment_polarity"
        
        # Convert date to datetime if it's not already
        sentiment_data[date_field] = pd.to_datetime(sentiment_data[date_field])
        
        # Extract date part only
        sentiment_data['date'] = sentiment_data[date_field].dt.date
        sentiment_data['date'] = pd.to_datetime(sentiment_data['date'])
        
        # Group by date and calculate daily sentiment statistics
        daily_sentiment = sentiment_data.groupby('date').agg({
            sentiment_field: ['mean', 'std', 'count']
        }).reset_index()
        
        # Flatten multi-level columns
        daily_sentiment.columns = ['date', 'sentiment_mean', 'sentiment_std', 'post_count']
        
        # Calculate rolling statistics
        daily_sentiment['sentiment_mean_7d'] = daily_sentiment['sentiment_mean'].rolling(window=7).mean()
        daily_sentiment['sentiment_std_7d'] = daily_sentiment['sentiment_mean'].rolling(window=7).std()
        
        # Calculate z-score
        daily_sentiment['sentiment_zscore'] = (daily_sentiment['sentiment_mean'] - daily_sentiment['sentiment_mean_7d']) / daily_sentiment['sentiment_std_7d']
        
        # Flag anomalies based on z-score threshold
        daily_sentiment['sentiment_anomaly'] = (daily_sentiment['sentiment_zscore'].abs() > threshold).astype(int)
        
        # Drop NaN values
        daily_sentiment = daily_sentiment.dropna()
        
        anomaly_count = daily_sentiment['sentiment_anomaly'].sum()
        logger.info(f"Detected {anomaly_count} sentiment anomalies out of {len(daily_sentiment)} days")
        
        return daily_sentiment
    
    def detect_correlation_breakdown(self, ticker, days=90, window=20, threshold=0.5):
        """
        Detect breakdowns in normal correlations between price and other factors
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to use
            window: Rolling window size for correlation calculation
            threshold: Threshold for correlation change to be considered an anomaly
            
        Returns:
            DataFrame with correlation breakdown flags
        """
        logger.info(f"Detecting correlation breakdowns for {ticker}")
        
        # Fetch data from different sources
        stock_data = self.correlation_analyzer.fetch_stock_data(ticker, days)
        twitter_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "twitter", days)
        reddit_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "reddit", days)
        economic_data = self.correlation_analyzer.fetch_economic_data(days)
        
        # Merge datasets
        merged_data = self.correlation_analyzer.merge_datasets(
            stock_data, twitter_data, reddit_data, economic_data
        )
        
        if merged_data is None or merged_data.empty:
            logger.error("Failed to create merged dataset for correlation analysis")
            return None
        
        # Calculate rolling correlations
        correlations = pd.DataFrame(index=merged_data.index)
        correlations['date'] = merged_data['date']
        
        # Correlation between price and volume
        correlations['price_volume_corr'] = merged_data['close'].rolling(window=window).corr(merged_data['volume'])
        
        # Correlation between price and Twitter sentiment (if available)
        if 'twitter_sentiment' in merged_data.columns:
            correlations['price_twitter_corr'] = merged_data['close'].rolling(window=window).corr(merged_data['twitter_sentiment'])
        
        # Correlation between price and Reddit sentiment (if available)
        if 'reddit_sentiment' in merged_data.columns:
            correlations['price_reddit_corr'] = merged_data['close'].rolling(window=window).corr(merged_data['reddit_sentiment'])
        
        # Calculate changes in correlations
        for col in correlations.columns:
            if col != 'date':
                correlations[f'{col}_change'] = correlations[col].diff().abs()
                correlations[f'{col}_anomaly'] = (correlations[f'{col}_change'] > threshold).astype(int)
        
        # Drop NaN values
        correlations = correlations.dropna()
        
        # Count anomalies
        anomaly_columns = [col for col in correlations.columns if col.endswith('_anomaly')]
        total_anomalies = correlations[anomaly_columns].sum().sum()
        
        logger.info(f"Detected {total_anomalies} correlation breakdown anomalies")
        
        return correlations
    
    def visualize_price_anomalies(self, anomaly_data, ticker, save_path=None):
        """
        Visualize price anomalies
        
        Args:
            anomaly_data: DataFrame with anomaly detection results
            ticker: Stock ticker symbol
            save_path: Path to save the visualization
        """
        if anomaly_data is None or anomaly_data.empty:
            logger.error("No anomaly data to visualize")
            return
        
        plt.figure(figsize=(12, 8))
        
        # Plot stock price
        plt.subplot(2, 1, 1)
        plt.plot(anomaly_data['date'], anomaly_data['close'], label='Close Price', color='blue')
        
        # Highlight anomalies
        anomalies = anomaly_data[anomaly_data['is_anomaly'] == 1]
        plt.scatter(anomalies['date'], anomalies['close'], color='red', label='Anomalies', zorder=5)
        
        plt.title(f'{ticker} Price Anomalies')
        plt.ylabel('Price')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Plot anomaly scores
        plt.subplot(2, 1, 2)
        plt.plot(anomaly_data['date'], anomaly_data['anomaly_score'], color='purple', label='Anomaly Score')
        plt.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
        
        plt.title('Anomaly Scores')
        plt.xlabel('Date')
        plt.ylabel('Score')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Price anomaly visualization saved to {save_path}")
        else:
            plt.show()
    
    def visualize_sentiment_anomalies(self, sentiment_anomalies, ticker, source, save_path=None):
        """
        Visualize sentiment anomalies
        
        Args:
            sentiment_anomalies: DataFrame with sentiment anomaly detection results
            ticker: Stock ticker symbol
            source: Sentiment source ('twitter' or 'reddit')
            save_path: Path to save the visualization
        """
        if sentiment_anomalies is None or sentiment_anomalies.empty:
            logger.error("No sentiment anomaly data to visualize")
            return
        
        plt.figure(figsize=(12, 8))
        
        # Plot sentiment
        plt.subplot(2, 1, 1)
        plt.plot(sentiment_anomalies['date'], sentiment_anomalies['sentiment_mean'], label='Sentiment', color='green')
        
        # Highlight anomalies
        anomalies = sentiment_anomalies[sentiment_anomalies['sentiment_anomaly'] == 1]
        plt.scatter(anomalies['date'], anomalies['sentiment_mean'], color='red', label='Anomalies', zorder=5)
        
        plt.title(f'{ticker} {source.capitalize()} Sentiment Anomalies')
        plt.ylabel('Sentiment')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Plot post count
        plt.subplot(2, 1, 2)
        plt.bar(sentiment_anomalies['date'], sentiment_anomalies['post_count'], color='skyblue', label='Post Count')
        
        plt.title(f'{source.capitalize()} Post Count')
        plt.xlabel('Date')
        plt.ylabel('Count')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
            logger.info(f"Sentiment anomaly visualization saved to {save_path}")
        else:
            plt.show()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Financial Data Anomaly Detection")
    parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL, TSLA)")
    parser.add_argument("--days", type=int, default=90, help="Number of days to analyze")
    parser.add_argument("--method", choices=['iforest', 'lof', 'knn', 'auto_encoder'], 
                        default='iforest', help="Anomaly detection method")
    parser.add_argument("--price", action="store_true", help="Detect price anomalies")
    parser.add_argument("--volume", action="store_true", help="Detect volume anomalies")
    parser.add_argument("--twitter", action="store_true", help="Detect Twitter sentiment anomalies")
    parser.add_argument("--reddit", action="store_true", help="Detect Reddit sentiment anomalies")
    parser.add_argument("--correlation", action="store_true", help="Detect correlation breakdowns")
    parser.add_argument("--all", action="store_true", help="Run all anomaly detection methods")
    parser.add_argument("--save-dir", help="Directory to save visualizations")
    
    args = parser.parse_args()
    
    # Create anomaly detector
    detector = AnomalyDetector()
    
    # Create save directory if specified
    save_dir = None
    if args.save_dir:
        save_dir = args.save_dir
        os.makedirs(save_dir, exist_ok=True)
    
    # Run selected anomaly detection methods
    if args.price or args.all:
        price_anomalies = detector.detect_price_anomalies(
            args.ticker.upper(), 
            days=args.days,
            method=args.method
        )
        
        if price_anomalies is not None:
            save_path = None
            if save_dir:
                save_path = os.path.join(save_dir, f"{args.ticker.upper()}_price_anomalies.png")
            
            detector.visualize_price_anomalies(
                price_anomalies,
                args.ticker.upper(),
                save_path=save_path
            )
    
    if args.volume or args.all:
        volume_anomalies = detector.detect_volume_anomalies(
            args.ticker.upper(),
            days=args.days
        )
    
    if args.twitter or args.all:
        twitter_anomalies = detector.detect_sentiment_anomalies(
            args.ticker.upper(),
            source='twitter',
            days=args.days
        )
        
        if twitter_anomalies is not None:
            save_path = None
            if save_dir:
                save_path = os.path.join(save_dir, f"{args.ticker.upper()}_twitter_anomalies.png")
            
            detector.visualize_sentiment_anomalies(
                twitter_anomalies,
                args.ticker.upper(),
                'twitter',
                save_path=save_path
            )
    
    if args.reddit or args.all:
        reddit_anomalies = detector.detect_sentiment_anomalies(
            args.ticker.upper(),
            source='reddit',
            days=args.days
        )
        
        if reddit_anomalies is not None:
            save_path = None
            if save_dir:
                save_path = os.path.join(save_dir, f"{args.ticker.upper()}_reddit_anomalies.png")
            
            detector.visualize_sentiment_anomalies(
                reddit_anomalies,
                args.ticker.upper(),
                'reddit',
                save_path=save_path
            )
    
    if args.correlation or args.all:
        correlation_anomalies = detector.detect_correlation_breakdown(
            args.ticker.upper(),
            days=args.days
        )