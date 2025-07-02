# Google Colab PySpark Stock Prediction Setup Guide

## 🚀 Quick Start in Google Colab

### 1. Install Required Packages
```python
# Install Java and Python packages
!apt-get update -qq
!apt-get install openjdk-8-jdk-headless -qq
!pip install pyspark pymongo dnspython textblob matplotlib seaborn pandas numpy

# Verify installations
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Test Java installation
!java -version

# Test PySpark installation
import pyspark
print(f"PySpark version: {pyspark.__version__}")
```

### 2. Upload the PySpark Predictor
Upload `colab_pyspark_predictor.py` to your Colab session or copy the code into a cell.

### 3. Run Predictions
```python
# Import and run the predictor
from colab_pyspark_predictor import run_colab_prediction

# Predict TSLA stock for next 5 days
try:
    results = run_colab_prediction("TSLA", 5)
    print("Prediction completed successfully!")
except Exception as e:
    print(f"Error: {e}")

# Predict AAPL stock for next 3 days
try:
    results = run_colab_prediction("AAPL", 3)
    print("Prediction completed successfully!")
except Exception as e:
    print(f"Error: {e}")
```

## 📊 Features

- **Big Data Processing**: Uses PySpark for scalable data processing
- **MongoDB Integration**: Connects to your MongoDB Atlas cluster
- **Feature Engineering**: Creates technical indicators and sentiment features
- **ML Models**: Trains and compares Random Forest, GBT, and Linear Regression
- **Predictions**: Forecasts future stock prices
- **Visualizations**: Creates beautiful charts of predictions
- **Colab Optimized**: Designed specifically for Google Colab environment

## 🔧 Configuration

The system uses your existing MongoDB Atlas URI:
```
mongodb+srv://ayush:ayush123@cluster0.1asgj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
```

## 📈 Usage Examples

### Basic Usage
```python
# Simple prediction
results = run_colab_prediction("TSLA", 5)
print(results["predictions"])
```

### Advanced Usage
```python
# Use the class directly for more control
from colab_pyspark_predictor import ColabPySparkStockPredictor

predictor = ColabPySparkStockPredictor()
try:
    # Load data
    stock_df = predictor.load_stock_data("TSLA")
    reddit_df = predictor.load_reddit_data("TSLA")
    
    # Create features
    feature_df = predictor.create_features(stock_df, reddit_df)
    
    # Train models
    train_df, test_df, _ = predictor.prepare_ml_data(feature_df)
    best_model, model_name, results = predictor.train_models(train_df, test_df)
    
    print(f"Best model: {model_name}")
    print(f"Performance: {results[model_name]}")
    
finally:
    predictor.cleanup()
```

## 🎯 Expected Output

The system will show:
- Data loading progress
- Feature engineering steps
- Model training results
- Prediction values
- Performance metrics
- Beautiful visualizations

## 🛠️ Troubleshooting

### Common Issues:

1. **Java not found**: Make sure to run the Java installation cell first
2. **MongoDB connection**: Verify your MongoDB Atlas cluster is accessible
3. **Memory issues**: Reduce data size or use smaller date ranges
4. **Missing data**: Check if stock/Reddit data exists for the ticker

### Data Requirements:
- Stock data in collections like `{ticker}_stock_data`
- Reddit data in collections like `{ticker}_reddit_data`
- Date fields properly formatted

## 🔮 Prediction Accuracy

The system compares multiple ML models and selects the best one based on R² score:
- **Random Forest**: Good for capturing non-linear patterns
- **Gradient Boosted Trees**: Excellent for complex relationships
- **Linear Regression**: Simple baseline model

## 📋 System Output

Results include:
- Ticker symbol
- Model used
- Performance metrics (RMSE, R²)
- Future price predictions
- Feature importance
- Visualization charts

Enjoy your PySpark-powered stock predictions! 🎉
