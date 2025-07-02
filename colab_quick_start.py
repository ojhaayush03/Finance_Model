# Google Colab Quick Start for PySpark Stock Prediction
# Copy and paste this entire cell into Google Colab

# Step 1: Install dependencies
print("🔧 Installing dependencies...")
import subprocess
import sys

# Install Java
subprocess.run(["apt-get", "update", "-qq"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.run(["apt-get", "install", "openjdk-8-jdk-headless", "-qq"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Install Python packages
packages = ["pyspark", "pymongo", "dnspython", "textblob", "matplotlib", "seaborn", "pandas", "numpy"]
subprocess.run([sys.executable, "-m", "pip", "install"] + packages, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print("✅ Dependencies installed successfully!")

# Step 2: Set environment variables and clear Spark conflicts
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Clear any existing Spark environment variables to avoid conflicts
spark_env_vars = ["SPARK_HOME", "SPARK_CONF_DIR", "SPARK_LOCAL_DIRS", "PYSPARK_PYTHON"]
for var in spark_env_vars:
    if var in os.environ:
        del os.environ[var]

# Ensure we use the pip-installed PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Step 3: Test installation
try:
    import pyspark
    print(f"✅ PySpark version: {pyspark.__version__}")
except ImportError as e:
    print(f"❌ PySpark import failed: {e}")

# Step 4: Create simplified predictor class
from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set plotting style
plt.style.use('default')
sns.set_palette("husl")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pymongo import MongoClient
from textblob import TextBlob

class ColabStockPredictor:
    def __init__(self):
        self.mongodb_uri = "mongodb+srv://ayush:ayush123@cluster0.1asgj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.spark = None
        self.mongo_client = None
        self._initialize()
    
    def _initialize(self):
        """Initialize connections"""
        try:
            print("🔧 Initializing Spark session...")
            self.spark = SparkSession.builder \
                .appName("ColabStockPrediction") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            print(f"✅ Spark session initialized: {self.spark.version}")
            
            print("🔗 Connecting to MongoDB...")
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB connection established")
            
        except Exception as e:
            print(f"❌ Initialization error: {e}")
            raise
    
    def predict_stock(self, ticker="TSLA", days=5):
        """Simple prediction function"""
        print(f"🚀 Starting prediction for {ticker.upper()}...")
        
        try:
            # Load data from MongoDB
            db = self.mongo_client['financial_db']
            
            # Try different collection naming patterns
            collection_names = [f"{ticker.lower()}_stock_data", f"stock_data.{ticker.lower()}", ticker.lower()]
            stock_data = None
            
            for collection_name in collection_names:
                try:
                    collection = db[collection_name]
                    data = list(collection.find().limit(1000).sort("date", -1))
                    if data:
                        stock_data = data
                        print(f"✅ Found {len(data)} records in {collection_name}")
                        break
                except:
                    continue
            
            if not stock_data:
                print(f"❌ No stock data found for {ticker}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(stock_data)
            
            # Standardize column names
            column_mapping = {
                'Close': 'close', 'close': 'close',
                'Volume': 'volume', 'volume': 'volume',
                'High': 'high', 'high': 'high',
                'Low': 'low', 'low': 'low',
                'Open': 'open', 'open': 'open'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            # Ensure we have required columns
            if 'close' not in df.columns:
                print("❌ 'close' column not found")
                return None
            
            # Handle date conversion
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            else:
                print("❌ 'date' column not found")
                return None
            
            df = df.sort_values('date').reset_index(drop=True)
            
            # Create simple features
            df['ma5'] = df['close'].rolling(window=5).mean()
            df['ma20'] = df['close'].rolling(window=20).mean()
            df['price_change'] = df['close'].pct_change()
            df['volatility'] = df['close'].rolling(window=10).std()
            
            # Remove NaN values
            df = df.dropna()
            
            if len(df) < 50:
                print("❌ Insufficient data for prediction")
                return None
            
            # Simple prediction using moving average trend
            recent_ma5 = df['ma5'].iloc[-5:].mean()
            recent_ma20 = df['ma20'].iloc[-5:].mean()
            current_price = df['close'].iloc[-1]
            
            # Trend prediction
            trend = (recent_ma5 - recent_ma20) / recent_ma20
            avg_volatility = df['volatility'].iloc[-20:].mean()
            
            predictions = []
            for day in range(1, days + 1):
                # Simple trend-based prediction with some randomness
                trend_factor = 1 + (trend * 0.1)
                volatility_factor = np.random.normal(1, avg_volatility * 0.01)
                predicted_price = current_price * trend_factor * volatility_factor
                
                next_date = df['date'].iloc[-1] + pd.Timedelta(days=day)
                predictions.append({
                    "date": next_date.strftime("%Y-%m-%d"),
                    "predicted_price": round(predicted_price, 2),
                    "day": day
                })
                current_price = predicted_price
            
            # Create visualization
            plt.figure(figsize=(12, 8))
            
            # Plot historical prices
            plt.subplot(2, 1, 1)
            recent_data = df.tail(30)
            plt.plot(recent_data['date'], recent_data['close'], 'b-', linewidth=2, label='Historical Prices')
            
            pred_dates = [pd.to_datetime(p["date"]) for p in predictions]
            pred_prices = [p["predicted_price"] for p in predictions]
            plt.plot(pred_dates, pred_prices, 'ro-', linewidth=2, markersize=8, label='Predictions')
            
            plt.title(f"{ticker.upper()} Stock Price Prediction", fontsize=16)
            plt.xlabel("Date")
            plt.ylabel("Price ($)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            # Plot technical indicators
            plt.subplot(2, 1, 2)
            plt.plot(recent_data['date'], recent_data['ma5'], 'g-', label='MA5', alpha=0.7)
            plt.plot(recent_data['date'], recent_data['ma20'], 'r-', label='MA20', alpha=0.7)
            plt.title("Moving Averages", fontsize=14)
            plt.xlabel("Date")
            plt.ylabel("Price ($)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.show()
            
            # Print results
            print(f"\n🎯 Prediction Results for {ticker.upper()}:")
            print(f"Current Price: ${df['close'].iloc[-1]:.2f}")
            print(f"Trend: {'Bullish' if trend > 0 else 'Bearish'} ({trend*100:.1f}%)")
            
            for pred in predictions:
                change = pred["predicted_price"] - df['close'].iloc[-1]
                change_pct = (change / df['close'].iloc[-1]) * 100
                print(f"Day {pred['day']} ({pred['date']}): ${pred['predicted_price']:.2f} "
                      f"({change:+.2f}, {change_pct:+.1f}%)")
            
            return {
                "ticker": ticker.upper(),
                "current_price": df['close'].iloc[-1],
                "predictions": predictions,
                "trend": trend
            }
            
        except Exception as e:
            print(f"❌ Prediction error: {e}")
            return None
    
    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()
        if self.mongo_client:
            self.mongo_client.close()
        print("🧹 Resources cleaned up")

# Quick prediction function
def quick_predict(ticker="TSLA", days=5):
    """
    Quick prediction function for Google Colab
    
    Usage:
    quick_predict("TSLA", 5)  # Predict Tesla for 5 days
    quick_predict("AAPL", 3)  # Predict Apple for 3 days
    """
    predictor = None
    try:
        print("🌟 Google Colab Stock Prediction System")
        print("=" * 50)
        
        predictor = ColabStockPredictor()
        results = predictor.predict_stock(ticker, days)
        
        if results:
            print("\n✅ Prediction completed successfully!")
            return results
        else:
            print("\n❌ Prediction failed")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        if predictor:
            predictor.cleanup()

# Example usage
print("\n" + "="*50)
print("🎉 Setup Complete! You can now run predictions:")
print("quick_predict('TSLA', 5)  # Predict Tesla for 5 days")
print("quick_predict('AAPL', 3)  # Predict Apple for 3 days")
print("="*50)

# Uncomment the line below to run a test prediction
# quick_predict("TSLA", 5)
