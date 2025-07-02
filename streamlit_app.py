import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import requests
import json
import os
from datetime import datetime, timedelta
from env_config import get_ngrok_url, get_prediction_endpoint


# Set page configuration
st.set_page_config(
    page_title="Stock Price Predictor",
    page_icon="📈",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    color: #1E88E5;
    text-align: center;
}
.sub-header {
    font-size: 1.2rem;
    color: #757575;
    text-align: center;
    margin-top: -1rem;
}
</style>
""", unsafe_allow_html=True)

# Colab API Integration
class ColabAPI:
    def __init__(self):
        self.ngrok_url = get_ngrok_url()
        self.api_url = get_prediction_endpoint()
    
    def is_connected(self):
        """Check if connected to Colab API"""
        if not self.ngrok_url:
            return False
        
        try:
            health_url = f"{self.ngrok_url}/health"
            response = requests.get(health_url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"Error checking connection: {e}")
            return False
    
    def predict(self, ticker, days):
        """Send prediction request to Colab API"""
        if not self.api_url:
            raise ValueError("No Colab API URL available")
        
        payload = {
            "ticker": ticker,
            "days": days
        }
        
        st.info(f"Sending prediction request to {self.api_url}")
        print(f"Sending prediction request to {self.api_url} with payload: {payload}")
        
        try:
            # First check if the server is reachable
            health_check = requests.get(f"{self.ngrok_url}/health", timeout=10)
            if health_check.status_code != 200:
                st.error(f"Health check failed with status code: {health_check.status_code}")
                raise ConnectionError(f"Colab server health check failed with status: {health_check.status_code}")
            
            # Send the actual prediction request
            response = requests.post(self.api_url, json=payload, timeout=300)  # 5 minute timeout
            
            # If we get an error response, try to extract more details
            if response.status_code != 200:
                error_details = "Unknown error"
                try:
                    error_data = response.json()
                    if "error" in error_data:
                        error_details = error_data["error"]
                    if "trace" in error_data:
                        st.code(error_data["trace"], language="python")
                except Exception as json_err:
                    st.warning(f"Could not parse error response as JSON: {json_err}")
                    error_details = response.text[:500] if response.text else "No error details available"
                
                st.error(f"Server error ({response.status_code}): {error_details}")
                raise ValueError(f"Server returned error {response.status_code}: {error_details}")
            
            # Process successful response
            return response.json()
            
        except requests.exceptions.ConnectionError as e:
            st.error("⚠️ Connection Error: Could not connect to the Colab server")
            st.info("Make sure your Colab notebook is running and the ngrok URL is correct")
            raise ConnectionError(f"Failed to connect to Colab server: {str(e)}")
            
        except requests.exceptions.Timeout as e:
            st.error("⏱️ Timeout Error: The prediction request took too long")
            st.info("The model might be processing a complex prediction or the server might be overloaded")
            raise TimeoutError(f"Prediction request timed out after 5 minutes: {str(e)}")
            
        except Exception as e:
            st.error(f"❌ Unexpected error: {str(e)}")
            raise

# Initialize Colab API
colab_api = ColabAPI()

# Main header
st.markdown('<h1 class="main-header">📈 Stock Price Predictor</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Powered by PySpark ML & MongoDB</p>', unsafe_allow_html=True)

# Sidebar for inputs
st.sidebar.title("📊 Stock Prediction Settings")

# Stock ticker input
ticker = st.sidebar.text_input("Enter Stock Ticker Symbol", value="TSLA").upper()

# Colab Connection Status
st.sidebar.title("🔌 Colab Connection")
ngrok_url = get_ngrok_url()
st.sidebar.text("Using Colab API at:")
st.sidebar.code(ngrok_url, language="text")

# Test the connection
connection_status = st.sidebar.empty()
if colab_api.is_connected():
    connection_status.success("✅ Connected to Colab!")
else:
    connection_status.error("❌ Not connected to Colab")

# Number of days to predict
prediction_days = st.sidebar.slider("Days to Predict", min_value=1, max_value=10, value=5)

# Run prediction button
if st.sidebar.button("🚀 Run Prediction"):
    st.session_state.button_clicked = True

# Main content
if st.session_state.get('button_clicked', False):
    # Create a container for the prediction process
    with st.container():
        st.header(f"🔍 Analyzing {ticker}")
        
        # Create a progress bar
        progress_bar = st.progress(0)
        
        # Update progress
        progress_bar.progress(25)
        st.info(f"🔍 Fetching data for {ticker}...")
        
        # Update progress
        progress_bar.progress(50)
        st.info(f"📊 Processing market data...")
        
        # Update progress
        progress_bar.progress(70)
        st.info(f"🤖 Running ML prediction for {ticker}...")
        
        try:
            # Check connection
            if not colab_api.is_connected():
                st.error("❌ Not connected to Colab API. Please check your ngrok URL.")
                st.stop()
            
            # Run prediction
            results = colab_api.predict(ticker, prediction_days)
            
            # Update progress
            progress_bar.progress(100)
            
            # Display results
            st.success(f"✅ Prediction completed for {ticker}!")
            
            # Display prediction results
            st.header("🎯 Prediction Results")
            
            # Display the prediction table
            if "predictions" in results:
                # Convert the predictions to a DataFrame
                predictions_df = pd.DataFrame(results["predictions"])
                
                # Ensure date column is properly formatted
                if "date" in predictions_df.columns:
                    predictions_df["date"] = pd.to_datetime(predictions_df["date"])
                
                # Display the DataFrame
                st.dataframe(predictions_df)
                
                # Create a chart of the predictions
                fig, ax = plt.subplots(figsize=(10, 6))
                ax.plot(predictions_df["date"], predictions_df["predicted_price"], marker='o', color='blue', linewidth=2)
                ax.set_title(f"{ticker} Price Prediction", fontsize=16)
                ax.set_xlabel("Date", fontsize=12)
                ax.set_ylabel("Price ($)", fontsize=12)
                ax.grid(True, alpha=0.3)
                
                # Format the x-axis dates
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                st.pyplot(fig)
                
                # Add a download button for the predictions
                csv = predictions_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Predictions",
                    data=csv,
                    file_name=f"{ticker}_predictions.csv",
                    mime="text/csv",
                )
            else:
                st.error("No predictions found in results")
            
            # Display model information
            st.subheader("🤖 Model Information")
            if "model_used" in results:
                st.write(f"**Model:** {results['model_used']}")
            if "model_performance" in results:
                st.write(f"**R² Score:** {results['model_performance'].get('r2', 'N/A')}")
                st.write(f"**RMSE:** {results['model_performance'].get('rmse', 'N/A')}")
            if "features_used" in results:
                st.write("**Features used:**")
                # Display features in a more readable format
                features_cols = st.columns(2)
                for i, feature in enumerate(results['features_used']):
                    col_idx = i % 2
                    features_cols[col_idx].write(f"- {feature}")
        except Exception as e:
            progress_bar.progress(100)
            st.error(f"❌ Error: {str(e)}")
            st.error("Please check that your Colab notebook is running and the ngrok URL is correct.")
            
            # Add a retry button
            if st.button("🔄 Retry"):
                st.rerun()

# Instructions for first-time users
if not st.session_state.get('button_clicked', False):
    st.info("""
    ### 👋 Welcome to the Stock Price Predictor!
    
    This app uses machine learning to predict future stock prices based on historical data and sentiment analysis.
    
    **To get started:**
    1. Enter a stock ticker symbol in the sidebar (e.g., TSLA, AAPL, MSFT)
    2. Select the number of days to predict
    3. Click the "Run Prediction" button
    
    The app will fetch data from various sources, analyze it using PySpark ML in Google Colab, and display the prediction results.
    """)