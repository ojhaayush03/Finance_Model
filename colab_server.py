# Colab API Server for Streamlit Frontend

# Install required packages

from flask import Flask, request, jsonify
from flask_cors import CORS
from pyngrok import ngrok
import threading
import time

# Import your prediction functions
from colab_pyspark_predictor import ColabPySparkStockPredictor
# Import financial visualizer
from financial_visualizer import FinancialVisualizer

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
    
    global predictor_instance, visualizer_instance
    try:
        # Create predictor instance if it doesn't exist or reuse existing one
        if predictor_instance is None:
            print("Creating new predictor instance...")
            predictor_instance = ColabPySparkStockPredictor()
            print("Predictor initialized successfully")
        else:
            print("Reusing existing predictor instance")
        
        # Create visualizer instance if it doesn't exist
        if visualizer_instance is None:
            print("Creating new visualizer instance...")
            visualizer_instance = FinancialVisualizer(mongo_client=predictor_instance.mongo_client)
            print("Visualizer initialized successfully")
        
        # Generate visualizations
        print(f"Generating visualizations for {ticker}...")
        images = visualizer_instance.generate_visualizations(ticker, days)
        
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
visualizer_instance = None

@app.route('/predict', methods=['POST'])
def predict():
    data = request.json
    ticker = data.get('ticker', 'TSLA')
    days = data.get('days', 5)
    
    print(f"Received prediction request for {ticker}, {days} days")
    
    global predictor_instance, visualizer_instance
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
        
        # Create visualizer instance if it doesn't exist
        if visualizer_instance is None:
            print("Creating new visualizer instance...")
            visualizer_instance = FinancialVisualizer(mongo_client=predictor_instance.mongo_client)
            print("Visualizer initialized successfully")
        
        # Generate visualizations
        print(f"Generating visualizations for {ticker}...")
        images = visualizer_instance.generate_visualizations(ticker, days)
        
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
