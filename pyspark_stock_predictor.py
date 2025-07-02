"""
PySpark-based Stock Price Prediction System
Scalable stock prediction using Apache Spark for big data processing
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import numpy as np
import pandas as pd

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window

# PySpark ML imports
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import Pipeline as MLPipeline

# MongoDB connector for Spark
try:
    from pymongo import MongoClient
except ImportError:
    print("Warning: pymongo not installed. Install with: pip install pymongo")

# Kafka integration
try:
    from kafka import KafkaProducer, KafkaConsumer
    import json
except ImportError:
    print("Warning: kafka-python not installed. Install with: pip install kafka-python")

class PySparkStockPredictor:
    """
    PySpark-based Stock Price Prediction System
    Handles large-scale data processing and machine learning
    """
    
    def __init__(self, app_name="StockPrediction", master="local[*]"):
        self.app_name = app_name
        self.master = master
        self.spark = None
        self.mongodb_uri = "mongodb://localhost:27017"
        self.database_name = "financial_db"
        self.kafka_bootstrap_servers = "localhost:9092"
        
        # Initialize Spark session
        self._initialize_spark()
        
        # MongoDB client
        self.mongo_client = None
        self._initialize_mongodb()
    
    def _initialize_spark(self):
        """Initialize Spark session with MongoDB and Kafka connectors"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .config("spark.jars.packages", 
                        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
                .config("spark.mongodb.input.uri", f"{self.mongodb_uri}/{self.database_name}") \
                .config("spark.mongodb.output.uri", f"{self.mongodb_uri}/{self.database_name}") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            print(f"✅ Spark session initialized: {self.spark.version}")
            
        except Exception as e:
            print(f"❌ Error initializing Spark: {e}")
            # Fallback to basic Spark session
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .getOrCreate()
    
    def _initialize_mongodb(self):
        """Initialize MongoDB connection"""
        try:
            self.mongo_client = MongoClient(self.mongodb_uri)
            # Test connection
            self.mongo_client.admin.command('ping')
            print("✅ MongoDB connection established")
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
    
    def load_stock_data_from_mongodb(self, ticker: str, collection_suffix: str = "_stock_data") -> DataFrame:
        """
        Load stock data from MongoDB using Spark
        """
        collection_name = f"{ticker.lower()}{collection_suffix}"
        
        try:
            # Try to load using Spark MongoDB connector
            df = self.spark.read \
                .format("mongo") \
                .option("collection", collection_name) \
                .load()
            
            print(f"✅ Loaded {df.count()} records from {collection_name}")
            return df
            
        except Exception as e:
            print(f"❌ Error loading from MongoDB via Spark: {e}")
            
            # Fallback: Load via pymongo and convert to Spark DataFrame
            return self._load_stock_data_fallback(ticker, collection_suffix)
    
    def _load_stock_data_fallback(self, ticker: str, collection_suffix: str) -> DataFrame:
        """Fallback method to load data via pymongo"""
        try:
            db = self.mongo_client[self.database_name]
            collection_name = f"{ticker.lower()}{collection_suffix}"
            collection = db[collection_name]
            
            # Get data from MongoDB
            cursor = collection.find().sort("date", 1)
            data = list(cursor)
            
            if not data:
                print(f"❌ No data found in {collection_name}")
                return None
            
            # Convert to pandas DataFrame first
            pdf = pd.DataFrame(data)
            
            # Convert ObjectId to string if present
            if '_id' in pdf.columns:
                pdf['_id'] = pdf['_id'].astype(str)
            
            # Convert to Spark DataFrame
            spark_df = self.spark.createDataFrame(pdf)
            
            print(f"✅ Loaded {spark_df.count()} records via fallback method")
            return spark_df
            
        except Exception as e:
            print(f"❌ Fallback loading failed: {e}")
            return None
    
    def load_reddit_sentiment_data(self, ticker: str) -> DataFrame:
        """Load Reddit sentiment data from MongoDB"""
        try:
            # Try multiple collection naming patterns
            possible_collections = [
                f"reddit_data.{ticker.lower()}",
                f"reddit_data_{ticker.lower()}",
                "reddit_data"
            ]
            
            for collection_name in possible_collections:
                try:
                    df = self.spark.read \
                        .format("mongo") \
                        .option("collection", collection_name) \
                        .load()
                    
                    if df.count() > 0:
                        # Filter by ticker if needed
                        if "ticker" in df.columns:
                            df = df.filter(F.col("ticker") == ticker.lower())
                        
                        print(f"✅ Loaded {df.count()} Reddit records from {collection_name}")
                        return df
                        
                except Exception:
                    continue
            
            print(f"❌ No Reddit data found for {ticker}")
            return None
            
        except Exception as e:
            print(f"❌ Error loading Reddit data: {e}")
            return None
    
    def create_technical_indicators(self, df: DataFrame) -> DataFrame:
        """
        Create technical indicators using Spark SQL window functions
        """
        # Define window specifications
        window_5 = Window.orderBy("date").rowsBetween(-4, 0)
        window_10 = Window.orderBy("date").rowsBetween(-9, 0)
        window_20 = Window.orderBy("date").rowsBetween(-19, 0)
        window_50 = Window.orderBy("date").rowsBetween(-49, 0)
        
        # Moving averages
        df = df.withColumn("ma_5", F.avg("close").over(window_5)) \
               .withColumn("ma_10", F.avg("close").over(window_10)) \
               .withColumn("ma_20", F.avg("close").over(window_20)) \
               .withColumn("ma_50", F.avg("close").over(window_50))
        
        # Price change features
        df = df.withColumn("price_change", F.col("close") - F.lag("close", 1).over(Window.orderBy("date"))) \
               .withColumn("price_change_pct", 
                          (F.col("close") - F.lag("close", 1).over(Window.orderBy("date"))) / 
                          F.lag("close", 1).over(Window.orderBy("date")) * 100)
        
        # Volatility (rolling standard deviation)
        df = df.withColumn("volatility_5", F.stddev("close").over(window_5)) \
               .withColumn("volatility_20", F.stddev("close").over(window_20))
        
        # High-Low spread
        if "high" in df.columns and "low" in df.columns:
            df = df.withColumn("hl_spread", F.col("high") - F.col("low")) \
                   .withColumn("hl_spread_pct", (F.col("high") - F.col("low")) / F.col("close") * 100)
        
        # Lag features
        for lag in range(1, 6):
            df = df.withColumn(f"close_lag_{lag}", F.lag("close", lag).over(Window.orderBy("date")))
        
        print("✅ Technical indicators created")
        return df
    
    def create_sentiment_features(self, stock_df: DataFrame, sentiment_df: DataFrame) -> DataFrame:
        """
        Merge stock data with sentiment features
        """
        if sentiment_df is None:
            print("⚠️ No sentiment data available")
            return stock_df
        
        try:
            # Aggregate sentiment by date
            sentiment_agg = sentiment_df.groupBy("date") \
                .agg(F.avg("sentiment_score").alias("avg_sentiment"),
                     F.count("*").alias("post_count"),
                     F.stddev("sentiment_score").alias("sentiment_volatility"))
            
            # Join with stock data
            merged_df = stock_df.join(sentiment_agg, on="date", how="left")
            
            # Fill null sentiment values
            merged_df = merged_df.fillna(0, subset=["avg_sentiment", "post_count", "sentiment_volatility"])
            
            print("✅ Sentiment features merged")
            return merged_df
            
        except Exception as e:
            print(f"❌ Error merging sentiment features: {e}")
            return stock_df
    
    def prepare_features_for_ml(self, df: DataFrame, target_col: str = "close") -> DataFrame:
        """
        Prepare features for machine learning
        """
        # Select numeric columns for features
        numeric_cols = [field.name for field in df.schema.fields 
                       if field.dataType in [DoubleType(), F.IntegerType()] 
                       and field.name not in ['_id', target_col]]
        
        # Remove null values
        df = df.dropna(subset=numeric_cols + [target_col])
        
        # Create target column for next day prediction
        window_spec = Window.orderBy("date")
        df = df.withColumn(f"{target_col}_next", F.lead(target_col, 1).over(window_spec))
        
        # Remove last row (no target)
        df = df.filter(F.col(f"{target_col}_next").isNotNull())
        
        print(f"✅ Features prepared. Shape: {df.count()} rows, {len(numeric_cols)} features")
        return df, numeric_cols
    
    def train_prediction_models(self, df: DataFrame, feature_cols: List[str], 
                              target_col: str = "close_next") -> Dict:
        """
        Train multiple ML models using PySpark MLlib
        """
        # Prepare feature vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # Feature scaling
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set: {train_df.count()} rows")
        print(f"Test set: {test_df.count()} rows")
        
        models = {}
        
        # Random Forest
        rf = RandomForestRegressor(featuresCol="scaled_features", labelCol=target_col, 
                                 numTrees=100, seed=42)
        rf_pipeline = MLPipeline(stages=[assembler, scaler, rf])
        
        print("Training Random Forest...")
        rf_model = rf_pipeline.fit(train_df)
        rf_predictions = rf_model.transform(test_df)
        
        rf_evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction")
        rf_rmse = rf_evaluator.evaluate(rf_predictions, {rf_evaluator.metricName: "rmse"})
        rf_r2 = rf_evaluator.evaluate(rf_predictions, {rf_evaluator.metricName: "r2"})
        
        models['random_forest'] = {
            'model': rf_model,
            'rmse': rf_rmse,
            'r2': rf_r2,
            'predictions': rf_predictions
        }
        
        # Gradient Boosted Trees
        gbt = GBTRegressor(featuresCol="scaled_features", labelCol=target_col, 
                          maxIter=100, seed=42)
        gbt_pipeline = MLPipeline(stages=[assembler, scaler, gbt])
        
        print("Training Gradient Boosted Trees...")
        gbt_model = gbt_pipeline.fit(train_df)
        gbt_predictions = gbt_model.transform(test_df)
        
        gbt_rmse = rf_evaluator.evaluate(gbt_predictions, {rf_evaluator.metricName: "rmse"})
        gbt_r2 = rf_evaluator.evaluate(gbt_predictions, {rf_evaluator.metricName: "r2"})
        
        models['gbt'] = {
            'model': gbt_model,
            'rmse': gbt_rmse,
            'r2': gbt_r2,
            'predictions': gbt_predictions
        }
        
        # Linear Regression
        lr = LinearRegression(featuresCol="scaled_features", labelCol=target_col)
        lr_pipeline = MLPipeline(stages=[assembler, scaler, lr])
        
        print("Training Linear Regression...")
        lr_model = lr_pipeline.fit(train_df)
        lr_predictions = lr_model.transform(test_df)
        
        lr_rmse = rf_evaluator.evaluate(lr_predictions, {rf_evaluator.metricName: "rmse"})
        lr_r2 = rf_evaluator.evaluate(lr_predictions, {rf_evaluator.metricName: "r2"})
        
        models['linear_regression'] = {
            'model': lr_model,
            'rmse': lr_rmse,
            'r2': lr_r2,
            'predictions': lr_predictions
        }
        
        # Print model comparison
        print("\n" + "=" * 50)
        print("MODEL PERFORMANCE COMPARISON")
        print("=" * 50)
        for name, model_info in models.items():
            print(f"{name.upper()}:")
            print(f"  RMSE: {model_info['rmse']:.4f}")
            print(f"  R²:   {model_info['r2']:.4f}")
        
        return models
    
    def predict_future_prices(self, model, latest_data: DataFrame, days: int = 5) -> List[float]:
        """
        Predict future stock prices
        """
        predictions = []
        current_data = latest_data
        
        for day in range(days):
            # Make prediction
            prediction_df = model.transform(current_data)
            next_price = prediction_df.select("prediction").collect()[0][0]
            predictions.append(next_price)
            
            # Update data for next prediction (simplified)
            # In a real scenario, you'd update all the features properly
            print(f"Day {day + 1} prediction: ${next_price:.2f}")
        
        return predictions
    
    def stream_predictions_to_kafka(self, predictions: List[float], ticker: str):
        """
        Stream predictions to Kafka for real-time consumption
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            topic = f"stock_predictions_{ticker.lower()}"
            
            prediction_data = {
                'ticker': ticker,
                'predictions': predictions,
                'timestamp': datetime.now().isoformat(),
                'model': 'pyspark_ml'
            }
            
            producer.send(topic, prediction_data)
            producer.flush()
            
            print(f"✅ Predictions sent to Kafka topic: {topic}")
            
        except Exception as e:
            print(f"❌ Error sending to Kafka: {e}")
    
    def save_model_to_mongodb(self, model, model_name: str, ticker: str, metrics: Dict):
        """
        Save model metadata to MongoDB
        """
        try:
            db = self.mongo_client[self.database_name]
            models_collection = db['ml_models']
            
            model_doc = {
                'model_name': model_name,
                'ticker': ticker,
                'created_at': datetime.now(),
                'metrics': metrics,
                'model_type': 'pyspark_ml',
                'status': 'active'
            }
            
            models_collection.insert_one(model_doc)
            print(f"✅ Model metadata saved to MongoDB")
            
        except Exception as e:
            print(f"❌ Error saving model metadata: {e}")
    
    def run_prediction_pipeline(self, ticker: str, days_to_predict: int = 5):
        """
        Run the complete prediction pipeline
        """
        print(f"\n🚀 Starting PySpark Stock Prediction Pipeline for {ticker}")
        print("=" * 60)
        
        # Step 1: Load stock data
        stock_df = self.load_stock_data_from_mongodb(ticker)
        if stock_df is None:
            print(f"❌ Cannot proceed without stock data for {ticker}")
            return
        
        # Step 2: Load sentiment data
        sentiment_df = self.load_reddit_sentiment_data(ticker)
        
        # Step 3: Create technical indicators
        stock_df = self.create_technical_indicators(stock_df)
        
        # Step 4: Merge with sentiment data
        merged_df = self.create_sentiment_features(stock_df, sentiment_df)
        
        # Step 5: Prepare features for ML
        final_df, feature_cols = self.prepare_features_for_ml(merged_df)
        
        # Step 6: Train models
        models = self.train_prediction_models(final_df, feature_cols)
        
        # Step 7: Select best model
        best_model_name = max(models.keys(), key=lambda k: models[k]['r2'])
        best_model = models[best_model_name]['model']
        
        print(f"\n🏆 Best model: {best_model_name} (R² = {models[best_model_name]['r2']:.4f})")
        
        # Step 8: Make future predictions
        latest_data = final_df.orderBy(F.desc("date")).limit(1)
        predictions = self.predict_future_prices(best_model, latest_data, days_to_predict)
        
        # Step 9: Stream to Kafka
        self.stream_predictions_to_kafka(predictions, ticker)
        
        # Step 10: Save model metadata
        self.save_model_to_mongodb(best_model, best_model_name, ticker, models[best_model_name])
        
        print(f"\n✅ Pipeline completed successfully!")
        return predictions, models
    
    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()
        if self.mongo_client:
            self.mongo_client.close()
        print("✅ Resources cleaned up")

def main():
    """Main function to run the PySpark stock prediction"""
    predictor = PySparkStockPredictor()
    
    try:
        # Get user input
        ticker = input("Enter ticker symbol (e.g., TSLA, AAPL): ").upper()
        days = int(input("Days to predict (default 5): ") or 5)
        
        # Run prediction pipeline
        predictions, models = predictor.run_prediction_pipeline(ticker, days)
        
        print(f"\n📈 FUTURE PRICE PREDICTIONS FOR {ticker}:")
        print("-" * 40)
        for i, price in enumerate(predictions, 1):
            print(f"Day {i}: ${price:.2f}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        predictor.cleanup()

if __name__ == "__main__":
    main()