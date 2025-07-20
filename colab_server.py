# Colab API Server for Streamlit Frontend

# Install required packages

from flask import Flask, request, jsonify
from flask_cors import CORS
from pyngrok import ngrok
import threading
import time
import io
import base64
import matplotlib.pyplot as plt
import seaborn as sns

# Import your prediction functions
from colab_pyspark_predictor import ColabPySparkStockPredictor
# Import financial analyzer from ffiifii.py instead of financial_visualizer
from ffiifii import FinancialAnalyzer

# Create Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"})

@app.route('/visualize', methods=['POST'])
def visualize():
    data = request.json
    ticker = data.get('ticker', 'TSLA')
    days = data.get('days', 30)
    
    print(f"Received visualization request for {ticker}, {days} days")
    
    global predictor_instance, analyzer_instance
    try:
        # Create predictor instance if it doesn't exist or reuse existing one
        if predictor_instance is None:
            print("Creating new predictor instance...")
            predictor_instance = ColabPySparkStockPredictor()
            print("Predictor initialized successfully")
        else:
            print("Reusing existing predictor instance")
        
        # Create analyzer instance if it doesn't exist
        if 'analyzer_instance' not in globals() or analyzer_instance is None:
            print("Creating new financial analyzer instance...")
            analyzer_instance = FinancialAnalyzer()
            print("Financial analyzer initialized successfully")
        
        # Generate visualizations using ffiifii.py functionality
        print(f"Generating visualizations for {ticker} using FinancialAnalyzer...")
        
        # Fetch data using the FinancialAnalyzer
        stock_data = analyzer_instance.fetch_stock_data(ticker, days)
        reddit_data = analyzer_instance.fetch_reddit_data(ticker, days)
        
        # Process Reddit data if available
        sentiment_data = None
        if reddit_data is not None:
            sentiment_data = analyzer_instance.analyze_reddit_sentiment(reddit_data)
            if sentiment_data is not None:
                sentiment_data = analyzer_instance.aggregate_daily_sentiment(sentiment_data)
        
        # Merge datasets
        merged_data = analyzer_instance.merge_datasets(stock_data, sentiment_data)
        
        if merged_data is None:
            return jsonify({
                "status": "error",
                "message": f"Failed to generate data for {ticker}"
            }), 400
        
        # Generate visualizations and convert to base64
        images = {}
        
        # Function to convert matplotlib figure to base64
        def fig_to_base64(fig):
            buf = io.BytesIO()
            fig.savefig(buf, format='png', bbox_inches='tight')
            buf.seek(0)
            img_str = base64.b64encode(buf.read()).decode('utf-8')
            buf.close()
            plt.close(fig)
            return img_str
        
        # Price and Volume Chart
        fig1 = plt.figure(figsize=(15, 10))
        
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
        
        # Plot 3: RSI Technical Indicator
        plt.subplot(2, 2, 3)
        merged_data['rsi'] = analyzer_instance._calculate_rsi(merged_data['close'])
        plt.plot(merged_data['date'], merged_data['rsi'], 'purple', label='RSI')
        plt.axhline(y=70, color='r', linestyle='--', alpha=0.3)
        plt.axhline(y=30, color='g', linestyle='--', alpha=0.3)
        plt.title(f'{ticker} RSI')
        plt.xlabel('Date')
        plt.ylabel('RSI')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Plot 4: Reddit Sentiment (if available)
        if 'avg_sentiment' in merged_data.columns:
            plt.subplot(2, 2, 4)
            plt.plot(merged_data['date'], merged_data['avg_sentiment'], 'r-', label='Reddit Sentiment')
            plt.axhline(y=0, color='k', linestyle='--', alpha=0.3)
            plt.title(f'{ticker} Reddit Sentiment')
            plt.xlabel('Date')
            plt.ylabel('Sentiment Score')
            plt.grid(True, alpha=0.3)
            plt.legend()
        
        plt.tight_layout()
        images['price_volume'] = fig_to_base64(fig1)
        
        # Correlation heatmap
        if len(merged_data.columns) > 5:
            fig2 = plt.figure(figsize=(10, 8))
            numeric_data = merged_data.select_dtypes(include=['number'])
            corr_matrix = numeric_data.corr()
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
            plt.title(f'Correlation Matrix for {ticker}')
            plt.tight_layout()
            images['correlation'] = fig_to_base64(fig2)
        
        # Price vs Sentiment scatter plot
        if 'avg_sentiment' in merged_data.columns:
            fig3 = plt.figure(figsize=(10, 6))
            plt.scatter(merged_data['avg_sentiment'], merged_data['close'], alpha=0.7)
            plt.title('Price vs. Sentiment')
            plt.xlabel('Sentiment Score')
            plt.ylabel('Price ($)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            images['price_vs_sentiment'] = fig_to_base64(fig3)
        
        # Add technical indicators
        merged_data['ma5'] = merged_data['close'].rolling(window=5).mean()
        merged_data['ma20'] = merged_data['close'].rolling(window=20).mean()
        
        # Technical indicators chart
        fig4 = plt.figure(figsize=(12, 6))
        plt.plot(merged_data['date'], merged_data['close'], 'b-', label='Close Price')
        plt.plot(merged_data['date'], merged_data['ma5'], 'r-', label='5-Day MA')
        plt.plot(merged_data['date'], merged_data['ma20'], 'g-', label='20-Day MA')
        plt.title(f'{ticker} Price with Moving Averages')
        plt.xlabel('Date')
        plt.ylabel('Price ($)')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        images['technical_indicators'] = fig_to_base64(fig4)
        
        if images:
            return jsonify({
                "status": "success",
                "ticker": ticker,
                "days": days,
                "visualizations": images
            })
        else:
            return jsonify({
                "status": "error",
                "message": f"Failed to generate visualizations for {ticker}"
            }), 400
            
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in visualization: {e}")
        print(f"Error trace: {error_trace}")
        
        return jsonify({"error": str(e), "trace": error_trace}), 500

# Global instances to reuse the SparkContext and MongoDB connection
predictor_instance = None
analyzer_instance = None

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json
    ticker = data.get('ticker', 'TSLA')
    days = data.get('days', 5)
    
    print(f"Received prediction request for {ticker}, {days} days")
    
    global predictor_instance, analyzer_instance
    try:
        # Create predictor instance if it doesn't exist or reuse existing one
        if predictor_instance is None:
            print("Creating new predictor instance...")
            predictor_instance = ColabPySparkStockPredictor()
            print("Predictor initialized successfully")
        else:
            print("Reusing existing predictor instance")
        
        # Test MongoDB connection
        try:
            predictor_instance.mongo_client.admin.command('ping')
            print("MongoDB connection successful")
        except Exception as mongo_err:
            print(f"MongoDB connection error: {mongo_err}")
            # Don't clean up on MongoDB error - we can reuse the SparkContext
            return jsonify({"error": f"MongoDB connection failed: {str(mongo_err)}"}), 500
        
        # Create analyzer instance if it doesn't exist
        if analyzer_instance is None:
            print("Creating new financial analyzer instance...")
            analyzer_instance = FinancialAnalyzer()
            print("Financial analyzer initialized successfully")
        
        # Generate visualizations
        print(f"Generating visualizations for {ticker}...")
        images = visualize()
        
        # Run prediction pipeline
        print(f"Starting prediction pipeline for {ticker}...")
        results = predictor_instance.run_prediction_pipeline(ticker, days)
        print("Prediction completed successfully")
        
        # Add visualizations to results
        if images:
            results["visualizations"] = images
        
        return jsonify(results)
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in prediction: {e}")
        print(f"Error trace: {error_trace}")
        
        # Check if the error is related to SparkContext being shut down
        if "SparkContext was shut down" in str(e):
            print("SparkContext was shut down. Creating a new predictor instance on next request.")
            predictor_instance = None
            
        return jsonify({"error": str(e), "trace": error_trace}), 500

# Start ngrok tunnel
def start_ngrok():
    public_url = ngrok.connect(5000)
    print(f"* Ngrok tunnel running at: {public_url}")
    return public_url

# Run Flask app in a separate thread
def run_flask_app():
    app.run(port=5000)

# Start the server
flask_thread = threading.Thread(target=run_flask_app)
flask_thread.start()
time.sleep(1)  # Wait for Flask to start

# Start ngrok and get the public URL
ngrok_url = start_ngrok()
print(f"Share this URL with your Streamlit app: {ngrok_url}")

# Run the server if this file is executed directly
if __name__ == "__main__":
    pass
