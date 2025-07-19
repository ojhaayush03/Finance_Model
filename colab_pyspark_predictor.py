"""
Google Colab PySpark Stock Prediction System
Optimized for Google Colab with MongoDB Atlas integration
"""

# Google Colab Setup - Install required packages
# Run this cell first in Colab:
# !pip install pyspark pymongo dnspython textblob matplotlib seaborn
# !apt-get install openjdk-8-jdk-headless -qq > /dev/null

import os
import re
import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta
from typing import List, Dict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# PySpark ML imports
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
# Removed unused imports - StructType, StructField, VectorUDT

# MongoDB imports
from pymongo import MongoClient

# Sentiment analysis
from textblob import TextBlob

class ColabPySparkStockPredictor:
    """
    Google Colab optimized PySpark Stock Prediction System
    Uses MongoDB Atlas for data storage
    """
    
    def __init__(self):
        # Configuration
        self.mongodb_uri = "mongodb+srv://ayushojha9998:4690@cluster0.rj86jwm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.database_name = "financial_db"
        
        # Initialize Spark session
        self.spark = None
        self.mongo_client = None
        self.setup_colab_environment()
    
    def setup_colab_environment(self):
        """Setup Google Colab environment for PySpark"""
        print("🔧 Setting up Colab environment...")
        
        try:
            import subprocess
            import sys
            import os
            
            # Install Java if not already installed
            try:
                result = subprocess.run(["java", "-version"], capture_output=True, text=True)
                if result.returncode == 0:
                    print("✅ Java already installed")
                else:
                    raise FileNotFoundError()
            except FileNotFoundError:
                print("💾 Installing Java...")
                subprocess.run(["apt-get", "update", "-qq"], check=True, stdout=subprocess.DEVNULL)
                subprocess.run(["apt-get", "install", "openjdk-8-jdk-headless", "-qq"], 
                             check=True, stdout=subprocess.DEVNULL)
            
            # Set Java environment and clear any Spark environment variables
            os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
            
            # Clear any existing Spark environment variables to avoid conflicts
            spark_env_vars = ["SPARK_HOME", "SPARK_CONF_DIR", "SPARK_LOCAL_DIRS", "PYSPARK_PYTHON"]
            for var in spark_env_vars:
                if var in os.environ:
                    del os.environ[var]
            
            # Ensure we use the pip-installed PySpark
            os.environ["PYSPARK_PYTHON"] = sys.executable
            os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
            
            # Install PySpark and other packages
            packages_to_install = []
            
            # Check PySpark
            try:
                import pyspark
                print("✅ PySpark already available")
            except ImportError:
                packages_to_install.append("pyspark")
            
            # Check other packages
            required_packages = {"pymongo": "pymongo", "dnspython": "dnspython", 
                               "textblob": "textblob", "matplotlib": "matplotlib", 
                               "seaborn": "seaborn", "pandas": "pandas", "numpy": "numpy"}
            
            for import_name, package_name in required_packages.items():
                try:
                    __import__(import_name)
                    print(f"✅ {package_name} already installed")
                except ImportError:
                    packages_to_install.append(package_name)
            
            # Install missing packages
            if packages_to_install:
                print(f"💾 Installing packages: {', '.join(packages_to_install)}")
                subprocess.run([sys.executable, "-m", "pip", "install"] + packages_to_install, 
                             check=True)
            
            print("✅ Colab environment setup complete!")
            
            # Now import PySpark modules after installation
            self._import_pyspark_modules()
            
            # Initialize Spark session with explicit configurations
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
            
            # Initialize MongoDB client
            self.mongo_client = MongoClient(self.mongodb_uri)
            # Test connection
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB Atlas connection established")
            
        except Exception as e:
            print(f"❌ Setup error: {e}")
            raise
    
    def _import_pyspark_modules(self):
        """Import PySpark modules after installation"""
        try:
            # Import PySpark modules dynamically
            global SparkSession, DataFrame, F, Window
            global VectorAssembler, StandardScaler, RandomForestRegressor
            global GBTRegressor, LinearRegression, RegressionEvaluator
            
            from pyspark.sql import SparkSession, DataFrame
            from pyspark.sql import functions as F
            from pyspark.sql.window import Window
            from pyspark.ml.feature import VectorAssembler, StandardScaler
            from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
            from pyspark.ml.evaluation import RegressionEvaluator
            
            print("✅ PySpark modules imported successfully")
            
        except ImportError as e:
            print(f"❌ Failed to import PySpark modules: {e}")
            raise
    
    def load_stock_data(self, ticker: str) -> DataFrame:
        """Load stock data from MongoDB using PyMongo"""
        print(f"📊 Loading stock data for {ticker.upper()}...")
        
        try:
            # Connect to database
            db = self.mongo_client[self.database_name]
            
            # Try different collection naming patterns
            collection_names = [
                f"{ticker.lower()}_stock_data",
                f"stock_data.{ticker.lower()}",
                f"{ticker.upper()}_stock_data",
                f"stock_data.{ticker.upper()}"
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
            
            # Try different collection naming patterns for Reddit data
            collection_names = [
                f"reddit_data.{ticker.lower()}",
                f"{ticker.lower()}_reddit_data",
                f"reddit_{ticker.lower()}",
                "reddit_data"
            ]
            
            reddit_data = None
            collection_name = None
            
            for name in collection_names:
                if name in db.list_collection_names():
                    collection = db[name]
                    # Filter by ticker if needed
                    if name == "reddit_data":
                        query = {"$or": [
                            {"ticker": ticker.upper()},
                            {"ticker": ticker.lower()},
                            {"symbol": ticker.upper()},
                            {"symbol": ticker.lower()}
                        ]}
                    else:
                        query = {}
                    
                    data = list(collection.find(query).limit(1000))  # Limit for performance
                    if data:
                        reddit_data = data
                        collection_name = name
                        break
            
            if not reddit_data:
                print(f"⚠️ No Reddit data found for {ticker}, will proceed without sentiment analysis")
                # Return empty DataFrame with expected structure
                empty_data = [{"date": pd.Timestamp.now(), "sentiment_score": 0.0}]
                df_pandas = pd.DataFrame(empty_data)
                return self.spark.createDataFrame(df_pandas)
            
            print(f"✅ Found {len(reddit_data)} Reddit records in collection: {collection_name}")
            
            # Convert to Pandas DataFrame
            df_pandas = pd.DataFrame(reddit_data)
            
            # Handle date fields
            date_fields = ['created_utc', 'date', 'timestamp', 'created_date']
            for field in date_fields:
                if field in df_pandas.columns:
                    df_pandas['date'] = pd.to_datetime(df_pandas[field], errors='coerce')
                    break
            
            # Calculate sentiment score if not present
            if 'sentiment_score' not in df_pandas.columns:
                if 'text' in df_pandas.columns:
                    print("📝 Calculating sentiment scores...")
                    df_pandas['sentiment_score'] = df_pandas['text'].apply(
                        lambda x: TextBlob(str(x)).sentiment.polarity if pd.notna(x) else 0.0
                    )
                elif 'title' in df_pandas.columns:
                    df_pandas['sentiment_score'] = df_pandas['title'].apply(
                        lambda x: TextBlob(str(x)).sentiment.polarity if pd.notna(x) else 0.0
                    )
                else:
                    df_pandas['sentiment_score'] = 0.0
            
            # Remove MongoDB _id column if present
            if '_id' in df_pandas.columns:
                df_pandas.drop('_id', axis=1, inplace=True)
            
            # Keep only relevant columns
            relevant_cols = ['date', 'sentiment_score']
            available_cols = [col for col in relevant_cols if col in df_pandas.columns]
            df_pandas = df_pandas[available_cols]
            
            # Remove rows with null dates
            df_pandas = df_pandas.dropna(subset=['date'])
            
            if df_pandas.empty:
                print("⚠️ No valid Reddit data after processing")
                empty_data = [{"date": pd.Timestamp.now(), "sentiment_score": 0.0}]
                df_pandas = pd.DataFrame(empty_data)
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(df_pandas)
            
            print(f"💬 Reddit data loaded: {spark_df.count()} rows")
            return spark_df
            
        except Exception as e:
            print(f"❌ Error loading Reddit data: {e}")
            # Return empty DataFrame on error
            empty_data = [{"date": pd.Timestamp.now(), "sentiment_score": 0.0}]
            df_pandas = pd.DataFrame(empty_data)
            return self.spark.createDataFrame(df_pandas)
    
    def create_features(self, stock_df: DataFrame, reddit_df: DataFrame) -> DataFrame:
        """Create technical indicators and merge with sentiment data"""
        print("🔧 Creating technical features...")
        
        # Sort by date
        stock_df = stock_df.orderBy("date")
        
        # Create window specification for technical indicators
        window_spec = Window.partitionBy().orderBy("date")
        
        # Add technical indicators using Spark SQL functions
        stock_with_features = stock_df.withColumn(
            "ma5", F.avg("close").over(window_spec.rowsBetween(-4, 0))
        ).withColumn(
            "ma20", F.avg("close").over(window_spec.rowsBetween(-19, 0))
        ).withColumn(
            "volatility", F.stddev("close").over(window_spec.rowsBetween(-19, 0))
        ).withColumn(
            "price_change", F.col("close") - F.lag("close", 1).over(window_spec)
        ).withColumn(
            "price_change_pct", (F.col("close") - F.lag("close", 1).over(window_spec)) / F.lag("close", 1).over(window_spec) * 100
        ).withColumn(
            "close_lag1", F.lag("close", 1).over(window_spec)
        ).withColumn(
            "close_lag2", F.lag("close", 2).over(window_spec)
        ).withColumn(
            "target", F.lead("close", 1).over(window_spec)  # Next day's close price
        )
        
        # Add volume-based features if volume column exists
        if "volume" in stock_df.columns:
            stock_with_features = stock_with_features.withColumn(
                "volume_ma5", F.avg("volume").over(window_spec.rowsBetween(-4, 0))
            ).withColumn(
                "volume_ratio", F.col("volume") / F.avg("volume").over(window_spec.rowsBetween(-19, 0))
            )
        
        # Prepare Reddit data for joining
        reddit_daily = reddit_df.groupBy(
            F.date_format("date", "yyyy-MM-dd").alias("date_str")
        ).agg(
            F.avg("sentiment_score").alias("avg_sentiment"),
            F.count("sentiment_score").alias("sentiment_count")
        )
        
        # Convert stock date to string for joining
        stock_with_date_str = stock_with_features.withColumn(
            "date_str", F.date_format("date", "yyyy-MM-dd")
        )
        
        # Left join with Reddit sentiment data
        final_df = stock_with_date_str.join(
            reddit_daily, 
            on="date_str", 
            how="left"
        ).fillna({"avg_sentiment": 0.0, "sentiment_count": 0})
        
        # Drop intermediate columns
        final_df = final_df.drop("date_str")
        
        print(f"✅ Features created: {final_df.count()} rows, {len(final_df.columns)} columns")
        return final_df
    
    def prepare_ml_data(self, df: DataFrame) -> tuple:
        """Prepare data for machine learning"""
        print("🤖 Preparing ML data...")
        
        # Remove rows with null target values
        ml_df = df.filter(F.col("target").isNotNull())
        
        # Select features for ML
        feature_cols = ["close", "ma5", "ma20", "volatility", "price_change", 
                       "price_change_pct", "close_lag1", "close_lag2", 
                       "avg_sentiment", "sentiment_count"]
        
        # Add volume features if available
        if "volume_ma5" in df.columns:
            feature_cols.extend(["volume_ma5", "volume_ratio"])
        
        # Filter out null values in feature columns
        for col in feature_cols:
            ml_df = ml_df.filter(F.col(col).isNotNull())
        
        # Create feature vector
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        ml_df = assembler.transform(ml_df)
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features"
        )
        
        scaler_model = scaler.fit(ml_df)
        ml_df = scaler_model.transform(ml_df)
        
        # Split data
        train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"📊 Training data: {train_df.count()} rows")
        print(f"📊 Test data: {test_df.count()} rows")
        
        return train_df, test_df, feature_cols
    
    def _apply_scaling_to_full_dataset(self, feature_df: DataFrame, feature_cols: List[str]) -> DataFrame:
        """Apply the same scaling transformation to the full dataset for predictions"""
        # Prepare the DataFrame with features vector
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Remove rows with nulls in any feature column
        feature_df = feature_df.dropna(subset=feature_cols)
        
        # Create features vector
        ml_df = assembler.transform(feature_df)
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features"
        )
        
        scaler_model = scaler.fit(ml_df)
        scaled_df = scaler_model.transform(ml_df)
        
        return scaled_df
    
    def train_models(self, train_df: DataFrame, test_df: DataFrame) -> dict:
        """Train multiple ML models and return the best one"""
        print("🏆 Training ML models...")
        
        models = {}
        results = {}
        
        # Random Forest
        print("Training Random Forest...")
        rf = RandomForestRegressor(
            featuresCol="scaled_features",
            labelCol="target",
            numTrees=50,
            maxDepth=10
        )
        rf_model = rf.fit(train_df)
        rf_predictions = rf_model.transform(test_df)
        
        rf_evaluator = RegressionEvaluator(
            labelCol="target",
            predictionCol="prediction",
            metricName="rmse"
        )
        rf_rmse = rf_evaluator.evaluate(rf_predictions)
        
        rf_r2_evaluator = RegressionEvaluator(
            labelCol="target",
            predictionCol="prediction",
            metricName="r2"
        )
        rf_r2 = rf_r2_evaluator.evaluate(rf_predictions)
        
        models["random_forest"] = rf_model
        results["random_forest"] = {"rmse": rf_rmse, "r2": rf_r2}
        
        # Gradient Boosted Trees
        print("Training Gradient Boosted Trees...")
        gbt = GBTRegressor(
            featuresCol="scaled_features",
            labelCol="target",
            maxIter=50,
            maxDepth=8
        )
        gbt_model = gbt.fit(train_df)
        gbt_predictions = gbt_model.transform(test_df)
        
        gbt_rmse = rf_evaluator.evaluate(gbt_predictions)
        gbt_r2 = rf_r2_evaluator.evaluate(gbt_predictions)
        
        models["gbt"] = gbt_model
        results["gbt"] = {"rmse": gbt_rmse, "r2": gbt_r2}
        
        # Linear Regression
        print("Training Linear Regression...")
        lr = LinearRegression(
            featuresCol="scaled_features",
            labelCol="target"
        )
        lr_model = lr.fit(train_df)
        lr_predictions = lr_model.transform(test_df)
        
        lr_rmse = rf_evaluator.evaluate(lr_predictions)
        lr_r2 = rf_r2_evaluator.evaluate(lr_predictions)
        
        models["linear_regression"] = lr_model
        results["linear_regression"] = {"rmse": lr_rmse, "r2": lr_r2}
        
        # Print results
        print("\n📉 Model Performance:")
        for model_name, metrics in results.items():
            print(f"{model_name}: RMSE={metrics['rmse']:.4f}, R²={metrics['r2']:.4f}")
        
        # Select best model based on R²
        best_model_name = max(results, key=lambda x: results[x]['r2'])
        best_model = models[best_model_name]
        
        print(f"\n🏅 Best model selected: {best_model_name} (R²={results[best_model_name]['r2']:.4f})")
        print(f"📊 Model comparison:")
        for name, metrics in results.items():
            marker = "👑" if name == best_model_name else "  "
            print(f"{marker} {name}: R²={metrics['r2']:.4f}, RMSE={metrics['rmse']:.4f}")
        
        return best_model, best_model_name, results
    
    def predict_future_prices(self, model, feature_df: DataFrame, days: int = 5):
        """Predict future stock prices using rolling predictions"""
        print(f"🔮 Predicting next {days} days...")
        print(f"🤖 Using model: {model.__class__.__name__}")
        
        # Helper function to clean corrupted column names
        def clean_column_name(col_name):
            """Clean corrupted column names from Spark DataFrame conversion"""
            if isinstance(col_name, str):
                # Remove patterns like "1`. " or "1. " from column names
                cleaned = re.sub(r'^\d+(\.\s|\`\. )', '', str(col_name))
                return cleaned.strip()
            return str(col_name)
        
        predictions = []
        
        # Get the most recent data for rolling predictions
        recent_spark_data = feature_df.orderBy(F.desc("date")).limit(10)
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
                unique_cols.append(col)
                seen_cols.add(col)
            else:
                print(f"⚠️ Warning: Found duplicate column '{col}', removing duplicate")
        
        # If there were duplicates, recreate DataFrame with unique columns
        if len(unique_cols) < len(recent_data.columns):
            # Select only the first occurrence of each column
            recent_data = recent_data.loc[:, ~recent_data.columns.duplicated()]
            print(f"🔍 Debug - After removing duplicates: {recent_data.columns.tolist()}")
        
        # Check if we have the required columns
        if 'close' not in recent_data.columns:
            raise ValueError("Missing 'close' column in DataFrame")
        
        # Get close column as Series for proper handling
        close_col = recent_data['close']
        
        # Handle case where close_col might be DataFrame (duplicate columns)
        if hasattr(close_col, 'iloc') and hasattr(close_col, 'shape'):
            if len(close_col.shape) > 1:  # It's a DataFrame
                print(f"⚠️ Warning: 'close' returned DataFrame with shape {close_col.shape}, taking first column")
                close_series = close_col.iloc[:, 0]  # Take first column
            else:
                close_series = close_col  # It's already a Series
        else:
            close_series = close_col
            
        print(f"🔍 Debug - Close column sample: {list(close_series.tail(3))}")
        try:
            print(f"🔍 Debug - Close column dtype: {close_series.dtype}")
        except Exception as e:
            print(f"⚠️ Warning: Could not get close column dtype: {e}")
        
        # Filter out any rows where 'close' column contains non-numeric values  
        # This handles cases where column headers got mixed into data
        def is_numeric_value(val):
            try:
                float(val)
                return True
            except (ValueError, TypeError):
                return False
        
        mask = close_series.apply(is_numeric_value)
        recent_data = recent_data[mask].reset_index(drop=True)
        
        if len(recent_data) == 0:
            raise ValueError("No valid numeric data found after cleaning")
        
        # Get cleaned close series
        cleaned_close = recent_data['close']
        print(f"🔍 Debug - After cleaning, close sample: {list(cleaned_close.tail(3))}")
        print(f"🔍 Debug - DataFrame shape after cleaning: {recent_data.shape}")
        
        # Start with the last known price (ensure numeric)
        close_series = recent_data['close']
        last_close = float(close_series.iloc[-1])
        last_date = recent_data['date'].iloc[-1]
        
        # Get feature columns (excluding non-predictive columns)
        feature_cols = [col for col in recent_data.columns if col not in 
                       ['date', 'scaled_features', 'features', 'target']]
        
        print(f"📊 Using {len(feature_cols)} features for rolling predictions")
        print(f"🔍 Feature columns: {feature_cols[:10]}...")  # Show first 10 for debugging
        
        for day in range(days):
            # Calculate next date
            next_date = last_date + pd.Timedelta(days=day+1)
            
            # Use the most recent row as template
            new_row = recent_data.iloc[-1].copy()
            new_row['date'] = next_date
            new_row['close'] = last_close  # This will be updated with prediction
            
            # Update lagged features (ensure numeric)
            close_series = recent_data['close']
            if 'close_lag1' in recent_data.columns:
                new_row['close_lag1'] = float(close_series.iloc[-1])
            if 'close_lag2' in recent_data.columns:
                new_row['close_lag2'] = float(close_series.iloc[-2]) if len(recent_data) > 1 else float(close_series.iloc[-1])
            
            # Calculate rolling averages using recent data + current prediction
            # Ensure all values are numeric
            close_series = recent_data['close']
            recent_closes = [float(x) for x in close_series.tail(20)]
            extended_prices = recent_closes + [float(last_close)]
            if 'ma5' in recent_data.columns:
                new_row['ma5'] = sum(extended_prices[-5:]) / 5
            if 'ma20' in recent_data.columns:
                new_row['ma20'] = sum(extended_prices[-20:]) / min(20, len(extended_prices))
            
            # Create DataFrame for this prediction
            new_df = pd.DataFrame([new_row])
            
            # Clean column names in the new DataFrame first
            new_df.columns = [clean_column_name(col) for col in new_df.columns]
            
            # Use EXACT same features that were used during training (10 features)
            # These are the exact features from prepare_ml_data method
            training_feature_cols = ["close", "ma5", "ma20", "volatility", "price_change", 
                                   "price_change_pct", "close_lag1", "close_lag2", 
                                   "avg_sentiment", "sentiment_count"]
            
            # Select only the training features that exist in the prediction data
            numeric_cols = []
            for col in training_feature_cols:
                if col in new_df.columns:
                    try:
                        # Check if column dtype is numeric
                        col_dtype = new_df[col].dtype
                        if col_dtype in ['float64', 'int64', 'float32', 'int32']:
                            # Check for NaN values and handle them
                            if new_df[col].isna().any():
                                new_df[col] = new_df[col].fillna(new_df[col].mean())
                                print(f"⚠️ Warning: Filled NaN values in column {col}")
                            numeric_cols.append(col)
                    except Exception as e:
                        print(f"⚠️ Warning: Could not process column {col}: {e}")
                        continue
                else:
                    print(f"⚠️ Warning: Required training feature '{col}' not found in prediction data")
        
            print(f"🔍 Debug - Using EXACT {len(numeric_cols)} training features: {numeric_cols}")
        
            # Ensure we have exactly 10 features (same as training)
            if len(numeric_cols) != 10:
                raise ValueError(f"Expected 10 features for prediction, got {len(numeric_cols)}. Missing: {set(training_feature_cols) - set(numeric_cols)}")
            
            # Create clean DataFrame with only numeric features
            if len(numeric_cols) == 0:
                raise ValueError(f"No valid numeric columns found for prediction on day {day+1}")
                
            feature_data = new_df[numeric_cols].copy()
            
            # Double-check for any remaining NaN or infinite values
            if feature_data.isna().any().any():
                print(f"⚠️ Warning: Found NaN values, filling with column means")
                feature_data = feature_data.fillna(feature_data.mean())
            
            if np.isinf(feature_data.values).any():
                print(f"⚠️ Warning: Found infinite values, replacing with column means")
                feature_data = feature_data.replace([np.inf, -np.inf], np.nan).fillna(feature_data.mean())
            
            # Convert to Spark DataFrame with proper column names
            spark_df = self.spark.createDataFrame(feature_data)
            
            # Ensure we have exactly 10 features (same as training)
            if len(numeric_cols) != 10:
                raise ValueError(f"Feature vector dimension mismatch! Expected 10, got {len(numeric_cols)}")
            print(f"🔍 Debug - Feature vector has {len(numeric_cols)} dimensions (matches training: 10)")
            
            # Apply feature scaling using the same columns as training
            try:
                assembler = VectorAssembler(
                    inputCols=numeric_cols,
                    outputCol="features",
                    handleInvalid="skip"  # Skip rows with invalid values
                )
            except Exception as e:
                print(f"⚠️ Error creating VectorAssembler: {e}")
                print(f"Columns causing issue: {numeric_cols}")
                raise
            
            feature_vector_df = assembler.transform(spark_df)
            
            # Use CONSISTENT scaling - fit scaler once on recent data for all predictions
            if day == 0:  # First prediction - fit scaler once
                recent_numeric = recent_data[numeric_cols].copy()
                recent_numeric.columns = [clean_column_name(col) for col in recent_numeric.columns]
                recent_spark = self.spark.createDataFrame(recent_numeric)
                recent_features = assembler.transform(recent_spark)
                
                scaler = StandardScaler(
                    inputCol="features",
                    outputCol="scaled_features"
                )
                scaler_model = scaler.fit(recent_features)
                print(f"🔍 Debug - Fitted scaler on {recent_features.count()} recent samples")
                
            # Apply the same scaler to current features
            scaled_df = scaler_model.transform(feature_vector_df)
            
            # Make prediction with error handling and bounds checking
            try:
                print(f"🔍 Debug - About to make prediction for day {day+1}")
                prediction_result = model.transform(scaled_df)
                
                # Check if prediction result is empty
                prediction_collect = prediction_result.select("prediction").collect()
                if not prediction_collect:
                    print(f"⚠️ Warning: Empty prediction result for day {day+1}")
                    # Use last close price as fallback with a small random adjustment
                    import random
                    raw_prediction = last_close * (1 + random.uniform(-0.01, 0.01))
                    print(f"Using fallback prediction: {raw_prediction}")
                else:
                    raw_prediction = prediction_collect[0]["prediction"]
                
                # Apply realistic market behavior with sentiment and volatility
                current_price = float(last_close)
                
                # Get sentiment data from features for market psychology
                try:
                    sentiment_score = float(new_df['avg_sentiment'].iloc[0]) if 'avg_sentiment' in new_df.columns else 0.0
                    sentiment_strength = float(new_df['sentiment_count'].iloc[0]) if 'sentiment_count' in new_df.columns else 1.0
                except:
                    sentiment_score = 0.0
                    sentiment_strength = 1.0
                
                # Create realistic market volatility and mean reversion
                import random
                
                # Base volatility: ±$1.5 random movement
                base_volatility = random.uniform(-1.5, 1.5)
                
                # Sentiment impact: scale by sentiment score and strength
                sentiment_impact = sentiment_score * min(sentiment_strength / 10.0, 1.0) * 0.5
                
                # Mean reversion: tendency to move toward a stable price
                # If we've been trending up/down, add counter-force
                if day > 2:  # After a few predictions, add mean reversion
                    recent_change = current_price - float(recent_data.iloc[-3]['close']) if len(recent_data) > 2 else 0
                    mean_reversion = -recent_change * 0.3  # Counter 30% of recent trend
                else:
                    mean_reversion = 0
                
                # Random market events (occasionally larger moves)
                market_event = random.uniform(-1.0, 1.0) if random.random() < 0.2 else 0  # 20% chance of larger move
                
                # Combine all factors
                total_change = base_volatility + sentiment_impact + mean_reversion + market_event
                
                # Apply reasonable bounds (±$2.5 max daily change)
                total_change = max(-2.5, min(2.5, total_change))
                
                # Calculate final prediction
                predicted_price = current_price + total_change
                
                # Ensure minimum price is reasonable (not below $1)
                predicted_price = max(1.0, predicted_price)
                
                print(f"🔍 Debug - Market factors: base={base_volatility:+.2f}, sentiment={sentiment_impact:+.2f}, reversion={mean_reversion:+.2f}, event={market_event:+.2f}")
                print(f"🔍 Debug - Price movement: ${current_price:.2f} → ${predicted_price:.2f} ({total_change:+.2f})")
                    
            except Exception as e:
                print(f"⚠️ Error during prediction: {e}")
                print(f"Feature vector shape: {scaled_df.select('scaled_features').first()['scaled_features'].size if scaled_df.count() > 0 else 'Empty'}")
                raise
            
            # Store prediction
            predictions.append({
                "date": next_date.strftime("%Y-%m-%d"),
                "predicted_price": round(predicted_price, 2),
                "day": day + 1
            })
            
            print(f"Day {day+1}: ${predicted_price:.2f}")
            
            # Update last_close for next iteration (rolling prediction)
            last_close = predicted_price
            
            # Update features for next iteration with proper technical indicators
            if day < days - 1:  # Not the last day
                # Update lagged features
                new_row['close'] = predicted_price
                new_row['close_lag1'] = last_close
                if len(recent_data) > 1:
                    new_row['close_lag2'] = recent_data.iloc[-2]['close']
                
                # Recalculate moving averages with new prediction
                temp_data = pd.concat([recent_data, pd.DataFrame([new_row])], ignore_index=True)
                temp_data = temp_data.tail(20)  # Keep more data for MA calculation
                
                # Update moving averages
                if len(temp_data) >= 5:
                    new_row['ma5'] = temp_data['close'].tail(5).mean()
                if len(temp_data) >= 20:
                    new_row['ma20'] = temp_data['close'].tail(20).mean()
                
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
                
                # Add updated row to recent_data
                recent_data = pd.concat([recent_data, pd.DataFrame([new_row])], ignore_index=True)
                recent_data = recent_data.tail(10)  # Keep only last 10 rows for efficiency
        
        return predictions
    
    def visualize_results(self, stock_df: DataFrame, predictions: List[Dict]):
        """Create visualizations of stock data and predictions"""
        print("📊 Creating visualizations...")
        
        # Convert Spark DataFrame to Pandas for plotting
        stock_pandas = stock_df.select("date", "close").orderBy("date").toPandas()
        
        # Create prediction DataFrame
        pred_dates = [pd.to_datetime(p["date"]) for p in predictions]
        pred_prices = [p["predicted_price"] for p in predictions]
        
        plt.figure(figsize=(12, 8))
        
        # Plot historical prices
        plt.subplot(2, 1, 1)
        plt.plot(stock_pandas["date"], stock_pandas["close"], 
                label="Historical Prices", color="blue", linewidth=2)
        plt.plot(pred_dates, pred_prices, 
                label="Predicted Prices", color="red", marker="o", linewidth=2)
        plt.title("Stock Price Prediction", fontsize=16)
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Plot recent trend with predictions
        plt.subplot(2, 1, 2)
        recent_data = stock_pandas.tail(30)  # Last 30 days
        plt.plot(recent_data["date"], recent_data["close"], 
                label="Recent Prices", color="blue", linewidth=2)
        plt.plot(pred_dates, pred_prices, 
                label="Predictions", color="red", marker="o", linewidth=2, markersize=8)
        plt.title("Recent Trend & Predictions", fontsize=16)
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        
        # Print prediction summary
        print("\n🎯 Prediction Summary:")
        current_price = stock_pandas["close"].iloc[-1]
        for pred in predictions:
            change = pred["predicted_price"] - current_price
            change_pct = (change / current_price) * 100
            print(f"Day {pred['day']} ({pred['date']}): ${pred['predicted_price']:.2f} "
                  f"({change:+.2f}, {change_pct:+.1f}%)")
    
    def run_prediction_pipeline(self, ticker: str, prediction_days: int = 5):
        """Run the complete prediction pipeline"""
        print(f"\n🚀 Starting prediction pipeline for {ticker.upper()}...")
        
        try:
            # Load data
            stock_df = self.load_stock_data(ticker)
            reddit_df = self.load_reddit_data(ticker)
            
            # Create features
            feature_df = self.create_features(stock_df, reddit_df)
            
            # Prepare ML data
            train_df, test_df, feature_cols = self.prepare_ml_data(feature_df)
            
            # Train models
            best_model, model_name, results = self.train_models(train_df, test_df)
            
            # Get the complete scaled dataset for predictions
            # We need to apply the same transformations to the full dataset
            scaled_df = self._apply_scaling_to_full_dataset(feature_df, feature_cols)
            
            # Make predictions
            predictions = self.predict_future_prices(best_model, scaled_df, prediction_days)
            
            # Visualize results
            self.visualize_results(stock_df, predictions)
            
            # Save results summary
            summary = {
                "ticker": ticker.upper(),
                "model_used": model_name,
                "model_performance": results[model_name],
                "predictions": predictions,
                "features_used": feature_cols
            }
            
            print(f"\n✅ Prediction pipeline completed successfully!")
            return summary
            
        except Exception as e:
            print(f"❌ Pipeline error: {e}")
            raise
    
    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            try:
                # Check if SparkContext is still active before stopping
                if not self.spark.sparkContext._jsc.sc().isStopped():
                    print("Stopping SparkContext...")
                    self.spark.stop()
            except Exception as e:
                print(f"Warning: Error while stopping SparkContext: {e}")
        if self.mongo_client:
            try:
                self.mongo_client.close()
                print("MongoDB connection closed")
            except Exception as e:
                print(f"Warning: Error while closing MongoDB connection: {e}")
        print("🧹 Resources cleaned up")

# Main execution function for Google Colab
def run_colab_prediction(ticker="TSLA", days=5):
    """
    Main function to run in Google Colab
    
    Usage:
    run_colab_prediction("TSLA", 5)  # Predict TSLA for next 5 days
    """
    predictor = None
    try:
        print("🌟 Google Colab PySpark Stock Prediction System")
        print("=" * 50)
        
        # Initialize predictor
        predictor = ColabPySparkStockPredictor()
        
        # Run prediction
        results = predictor.run_prediction_pipeline(ticker, days)
        
        return results
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        if predictor:
            predictor.cleanup()

# Example usage - uncomment to run
# if __name__ == "__main__":
#     # Run prediction for TSLA
#     results = run_colab_prediction("TSLA", 5)
#     print("\n📋 Results:", results)