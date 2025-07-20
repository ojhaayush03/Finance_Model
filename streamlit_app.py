import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import json
import os
import base64
import io
from datetime import datetime, timedelta
from env_config import get_ngrok_url, get_prediction_endpoint, get_visualization_endpoint

# Import local processing modules
from ffiifii import FinancialAnalyzer
from data_ingestion.enhanced_pipeline import EnhancedDataPipeline


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
.stTabs [data-baseweb="tab-list"] {
    gap: 24px;
}
.stTabs [data-baseweb="tab"] {
    height: 50px;
    white-space: pre-wrap;
    background-color: #F0F2F6;
    border-radius: 4px 4px 0px 0px;
    gap: 1px;
    padding-top: 10px;
    padding-bottom: 10px;
}
.stTabs [aria-selected="true"] {
    background-color: #1E88E5 !important;
    color: white !important;
}
</style>
""", unsafe_allow_html=True)

# Colab API Integration
class ColabAPI:
    def __init__(self):
        self.ngrok_url = get_ngrok_url()
        self.prediction_url = get_prediction_endpoint()
        self.visualization_url = get_visualization_endpoint()
    
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
    
    def visualize(self, ticker, days):
        """Send visualization request to Colab API"""
        if not self.visualization_url:
            raise ValueError("No Colab API URL available")
        
        payload = {
            "ticker": ticker,
            "days": days
        }
        
        st.info(f"Sending visualization request to {self.visualization_url}")
        print(f"Sending visualization request to {self.visualization_url} with payload: {payload}")
        
        try:
            # First check if the server is reachable
            health_check = requests.get(f"{self.ngrok_url}/health", timeout=10)
            if health_check.status_code != 200:
                st.error(f"Health check failed with status code: {health_check.status_code}")
                raise ConnectionError(f"Colab server health check failed with status: {health_check.status_code}")
            
            # Send the visualization request
            response = requests.post(self.visualization_url, json=payload, timeout=180)  # 3 minute timeout
            
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
            st.error("⏱️ Timeout Error: The visualization request took too long")
            st.info("The server might be processing a complex request or might be overloaded")
            raise TimeoutError(f"Visualization request timed out after 3 minutes: {str(e)}")
            
        except Exception as e:
            st.error(f"❌ Unexpected error: {str(e)}")
            raise
    
    def predict(self, ticker, days):
        """Send prediction request to Colab API"""
        if not self.prediction_url:
            raise ValueError("No Colab API URL available")
        
        payload = {
            "ticker": ticker,
            "days": days
        }
        
        st.info(f"Sending prediction request to {self.prediction_url}")
        print(f"Sending prediction request to {self.prediction_url} with payload: {payload}")
        
        try:
            # First check if the server is reachable
            health_check = requests.get(f"{self.ngrok_url}/health", timeout=10)
            if health_check.status_code != 200:
                st.error(f"Health check failed with status code: {health_check.status_code}")
                raise ConnectionError(f"Colab server health check failed with status: {health_check.status_code}")
            
            # Send the actual prediction request
            response = requests.post(self.prediction_url, json=payload, timeout=300)  # 5 minute timeout
            
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

# Historical data days
historical_days = st.sidebar.slider("Historical Data Days", min_value=30, max_value=180, value=60)

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

# Action buttons
visualize_button = st.sidebar.button("📊 Visualize Data")
predict_button = st.sidebar.button("🚀 Run Prediction")

# Initialize session state for tabs
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 0

# Main content with tabs
tabs = st.tabs(["📊 Visualizations", "🔮 Predictions", "🖥️ Local Processing", "ℹ️ Help"])

# Visualizations Tab
with tabs[0]:
    if visualize_button or ('visualizations' in st.session_state and st.session_state.active_tab == 0):
        st.session_state.active_tab = 0
        
        # Create a container for the visualization process
        with st.container():
            st.header(f"📊 Visualizing {ticker} Data")
            
            # Create a progress bar
            progress_bar = st.progress(0)
            
            # Update progress
            progress_bar.progress(25)
            st.info(f"🔍 Fetching data for {ticker}...")
            
            # Update progress
            progress_bar.progress(50)
            st.info(f"📊 Processing market data...")
            
            try:
                # Check connection
                if not colab_api.is_connected():
                    st.error("❌ Not connected to Colab API. Please check your ngrok URL.")
                    st.stop()
                
                # Run visualization if not already in session state
                if 'visualizations' not in st.session_state:
                    results = colab_api.visualize(ticker, historical_days)
                    if 'visualizations' in results:
                        st.session_state.visualizations = results['visualizations']
                    else:
                        st.error("No visualizations found in results")
                        st.stop()
                
                # Update progress
                progress_bar.progress(100)
                
                # Display results
                st.success(f"✅ Visualization completed for {ticker}!")
                
                # Display visualization results
                st.header(f"📈 {ticker} Market Analysis")
                
                # Display the visualizations
                if 'visualizations' in st.session_state:
                    visualizations = st.session_state.visualizations
                    
                    # Price and Volume
                    if 'price_volume' in visualizations:
                        st.subheader("Price and Volume Charts")
                        st.image(f"data:image/png;base64,{visualizations['price_volume']}")
                    
                    # Technical Indicators
                    if 'technical_indicators' in visualizations:
                        st.subheader("Technical Indicators")
                        st.image(f"data:image/png;base64,{visualizations['technical_indicators']}")
                    
                    # Correlation Heatmap
                    if 'correlation' in visualizations:
                        st.subheader("Feature Correlation Heatmap")
                        st.image(f"data:image/png;base64,{visualizations['correlation']}")
                    
                    # Price vs Sentiment
                    if 'price_vs_sentiment' in visualizations:
                        st.subheader("Price vs. Sentiment Correlation")
                        st.image(f"data:image/png;base64,{visualizations['price_vs_sentiment']}")
                
            except Exception as e:
                progress_bar.progress(100)
                st.error(f"❌ Error: {str(e)}")
                st.error("Please check that your Colab notebook is running and the ngrok URL is correct.")
                
                # Add a retry button
                if st.button("🔄 Retry Visualization"):
                    if 'visualizations' in st.session_state:
                        del st.session_state.visualizations
                    st.rerun()
    else:
        st.info("""
        ### 📊 Visualize Stock Data
        
        Click the "Visualize Data" button in the sidebar to generate visualizations for your selected stock.
        
        Visualizations include:
        - Price and volume charts
        - Technical indicators
        - Reddit sentiment analysis
        - Correlation analysis
        """)

# Predictions Tab
with tabs[1]:
    if predict_button or ('predictions' in st.session_state and st.session_state.active_tab == 1):
        st.session_state.active_tab = 1
        
        # Create a container for the prediction process
        with st.container():
            st.header(f"🔮 Predicting {ticker} Future Prices")
            
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
                
                # Run prediction if not already in session state
                if 'predictions' not in st.session_state:
                    results = colab_api.predict(ticker, prediction_days)
                    st.session_state.predictions = results
                else:
                    results = st.session_state.predictions
                
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
                
                # Display visualizations if available
                if "visualizations" in results:
                    st.header("📊 Visualizations")
                    visualizations = results["visualizations"]
                    
                    # Price and Volume
                    if 'price_volume' in visualizations:
                        st.subheader("Price and Volume")
                        st.image(f"data:image/png;base64,{visualizations['price_volume']}")
                    
                    # Prediction visualization
                    if 'prediction' in visualizations:
                        st.subheader("Price Forecast")
                        st.image(f"data:image/png;base64,{visualizations['prediction']}")
                
            except Exception as e:
                progress_bar.progress(100)
                st.error(f"❌ Error: {str(e)}")
                st.error("Please check that your Colab notebook is running and the ngrok URL is correct.")
                
                # Add a retry button
                if st.button("🔄 Retry Prediction"):
                    if 'predictions' in st.session_state:
                        del st.session_state.predictions
                    st.rerun()
    else:
        st.info("""
        ### 🔮 Predict Stock Prices
        
        Click the "Run Prediction" button in the sidebar to generate future price predictions for your selected stock.
        
        The prediction model uses:
        - Historical price data
        - Technical indicators
        - Reddit sentiment analysis
        - Machine learning algorithms
        """)

# Local Processing Tab
with tabs[2]:
    st.header("🖥️ Local Data Processing")
    
    st.markdown("""
    ### Process Stock Data Locally
    
    This tab allows you to process stock data locally using the EnhancedDataPipeline and FinancialAnalyzer.
    """)
    
    # Create columns for the two processing options
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Enhanced Data Pipeline")
        st.markdown("""
        Run the complete data pipeline locally, including:
        - Stock data collection
        - Technical indicators
        - News sentiment
        - Reddit sentiment
        - Twitter sentiment
        - Economic indicators
        """)
        
        # Pipeline options
        include_news = st.checkbox("Include News Data", value=True)
        include_economic = st.checkbox("Include Economic Indicators", value=True)
        include_twitter = st.checkbox("Include Twitter Sentiment", value=True)
        include_reddit = st.checkbox("Include Reddit Sentiment", value=True)
        
        # Run pipeline button
        run_pipeline_button = st.button("🚀 Run Enhanced Pipeline")
        
        # Initialize session state for pipeline results
        if 'pipeline_results' not in st.session_state:
            st.session_state.pipeline_results = None
            
        # Run the pipeline when button is clicked
        if run_pipeline_button:
            try:
                with st.spinner("Running enhanced data pipeline..."):
                    # Create progress bar
                    progress_bar = st.progress(0)
                    
                    # Initialize pipeline
                    st.info("Initializing pipeline...")
                    progress_bar.progress(10)
                    pipeline = EnhancedDataPipeline()
                    
                    # Run pipeline
                    st.info(f"Processing data for {ticker}...")
                    progress_bar.progress(30)
                    
                    # Run the pipeline
                    success = pipeline.run_pipeline(
                        ticker,
                        include_news=include_news,
                        include_economic=include_economic,
                        include_twitter=include_twitter,
                        include_reddit=include_reddit
                    )
                    
                    progress_bar.progress(100)
                    
                    if success:
                        st.success(f"✅ Enhanced pipeline for {ticker} completed successfully!")
                        st.session_state.pipeline_results = {
                            "ticker": ticker,
                            "success": True,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "options": {
                                "news": include_news,
                                "economic": include_economic,
                                "twitter": include_twitter,
                                "reddit": include_reddit
                            }
                        }
                    else:
                        st.error(f"⚠️ Pipeline for {ticker} completed with some errors.")
                        st.session_state.pipeline_results = {
                            "ticker": ticker,
                            "success": False,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
            except Exception as e:
                st.error(f"❌ Error running pipeline: {str(e)}")
                st.code(str(e), language="python")
        
        # Display pipeline results if available
        if st.session_state.pipeline_results:
            st.subheader("Pipeline Results")
            results = st.session_state.pipeline_results
            
            st.markdown(f"""
            **Ticker:** {results['ticker']}  
            **Status:** {"✅ Success" if results['success'] else "⚠️ Completed with errors"}  
            **Timestamp:** {results['timestamp']}
            """)
            
            if 'options' in results:
                st.markdown("**Options used:**")
                st.json(results['options'])
    
    with col2:
        st.subheader("📈 Financial Analysis")
        st.markdown("""
        Run financial analysis locally using the FinancialAnalyzer:
        - Fetch stock data
        - Fetch Reddit sentiment
        - Generate visualizations
        - Analyze correlations
        """)
        
        # Analysis options
        analysis_days = st.slider("Historical Days for Analysis", 
                                 min_value=7, 
                                 max_value=180, 
                                 value=30,
                                 help="Number of days of historical data to analyze")
        
        # Run analysis button
        run_analysis_button = st.button("📊 Run Financial Analysis")
        
        # Initialize session state for analysis results
        if 'analysis_results' not in st.session_state:
            st.session_state.analysis_results = None
            
        # Run the analysis when button is clicked
        if run_analysis_button:
            try:
                with st.spinner("Running financial analysis..."):
                    # Create progress bar
                    progress_bar = st.progress(0)
                    
                    # Initialize analyzer
                    st.info("Initializing financial analyzer...")
                    progress_bar.progress(10)
                    analyzer = FinancialAnalyzer()
                    
                    # Fetch stock data
                    st.info(f"Fetching stock data for {ticker}...")
                    progress_bar.progress(30)
                    stock_data = analyzer.fetch_stock_data(ticker, analysis_days)
                    
                    if stock_data is None:
                        st.error(f"❌ No stock data found for {ticker}")
                        st.stop()
                    
                    # Fetch Reddit data
                    st.info(f"Fetching Reddit data for {ticker}...")
                    progress_bar.progress(50)
                    reddit_data = analyzer.fetch_reddit_data(ticker, analysis_days)
                    
                    # Process sentiment if Reddit data is available
                    sentiment_data = None
                    if reddit_data is not None:
                        st.info("Processing Reddit sentiment...")
                        progress_bar.progress(70)
                        sentiment_data = analyzer.analyze_reddit_sentiment(reddit_data)
                        if sentiment_data is not None:
                            sentiment_data = analyzer.aggregate_daily_sentiment(sentiment_data)
                    
                    # Merge datasets
                    st.info("Merging datasets...")
                    progress_bar.progress(80)
                    merged_data = analyzer.merge_datasets(stock_data, sentiment_data)
                    
                    if merged_data is None:
                        st.error(f"❌ Failed to generate merged data for {ticker}")
                        st.stop()
                    
                    # Generate visualizations
                    st.info("Generating visualizations...")
                    progress_bar.progress(90)
                    
                    # Store results in session state
                    st.session_state.analysis_results = {
                        "ticker": ticker,
                        "days": analysis_days,
                        "stock_data": stock_data,
                        "sentiment_data": sentiment_data,
                        "merged_data": merged_data
                    }
                    
                    progress_bar.progress(100)
                    st.success(f"✅ Financial analysis for {ticker} completed successfully!")
                    
            except Exception as e:
                st.error(f"❌ Error running analysis: {str(e)}")
                st.code(str(e), language="python")
        
        # Display analysis results if available
        if st.session_state.analysis_results:
            st.subheader("Analysis Results")
            results = st.session_state.analysis_results
            
            # Display data summary
            st.markdown(f"""
            **Ticker:** {results['ticker']}  
            **Days Analyzed:** {results['days']}  
            **Stock Data Points:** {len(results['stock_data']) if results['stock_data'] is not None else 0}  
            **Sentiment Data Points:** {len(results['sentiment_data']) if results['sentiment_data'] is not None else 0}
            """)
            
            # Display visualizations
            if results['merged_data'] is not None:
                st.subheader("Visualizations")
                
                # Create tabs for different visualizations
                viz_tabs = st.tabs(["Price & Volume", "Sentiment", "Correlation", "Technical"])
                
                # Price and Volume tab
                with viz_tabs[0]:
                    fig1 = plt.figure(figsize=(10, 6))
                    plt.plot(results['merged_data']['date'], results['merged_data']['close'], 'b-', label='Close Price')
                    plt.title(f'{results["ticker"]} Stock Price')
                    plt.xlabel('Date')
                    plt.ylabel('Price ($)')
                    plt.grid(True, alpha=0.3)
                    plt.legend()
                    st.pyplot(fig1)
                    
                    if 'volume' in results['merged_data'].columns:
                        fig2 = plt.figure(figsize=(10, 6))
                        plt.bar(results['merged_data']['date'], results['merged_data']['volume'], color='g', alpha=0.7)
                        plt.title(f'{results["ticker"]} Trading Volume')
                        plt.xlabel('Date')
                        plt.ylabel('Volume')
                        plt.grid(True, alpha=0.3)
                        st.pyplot(fig2)
                
                # Sentiment tab
                with viz_tabs[1]:
                    if 'avg_sentiment' in results['merged_data'].columns:
                        fig3 = plt.figure(figsize=(10, 6))
                        plt.plot(results['merged_data']['date'], results['merged_data']['avg_sentiment'], 'r-', label='Reddit Sentiment')
                        plt.axhline(y=0, color='k', linestyle='--', alpha=0.3)
                        plt.title(f'{results["ticker"]} Reddit Sentiment')
                        plt.xlabel('Date')
                        plt.ylabel('Sentiment Score')
                        plt.grid(True, alpha=0.3)
                        plt.legend()
                        st.pyplot(fig3)
                    else:
                        st.info("No sentiment data available for this ticker.")
                
                # Correlation tab
                with viz_tabs[2]:
                    if len(results['merged_data'].columns) > 5:
                        fig4 = plt.figure(figsize=(10, 8))
                        numeric_data = results['merged_data'].select_dtypes(include=['number'])
                        corr_matrix = numeric_data.corr()
                        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
                        plt.title(f'Correlation Matrix for {results["ticker"]}')
                        plt.tight_layout()
                        st.pyplot(fig4)
                        
                        if 'avg_sentiment' in results['merged_data'].columns:
                            fig5 = plt.figure(figsize=(10, 6))
                            plt.scatter(results['merged_data']['avg_sentiment'], results['merged_data']['close'], alpha=0.7)
                            plt.title('Price vs. Sentiment')
                            plt.xlabel('Sentiment Score')
                            plt.ylabel('Price ($)')
                            plt.grid(True, alpha=0.3)
                            st.pyplot(fig5)
                    else:
                        st.info("Not enough data columns for correlation analysis.")
                
                # Technical tab
                with viz_tabs[3]:
                    # Calculate RSI
                    results['merged_data']['rsi'] = analyzer._calculate_rsi(results['merged_data']['close'])
                    
                    # Calculate moving averages
                    results['merged_data']['ma5'] = results['merged_data']['close'].rolling(window=5).mean()
                    results['merged_data']['ma20'] = results['merged_data']['close'].rolling(window=20).mean()
                    
                    # Plot RSI
                    fig6 = plt.figure(figsize=(10, 6))
                    plt.plot(results['merged_data']['date'], results['merged_data']['rsi'], 'purple', label='RSI')
                    plt.axhline(y=70, color='r', linestyle='--', alpha=0.3)
                    plt.axhline(y=30, color='g', linestyle='--', alpha=0.3)
                    plt.title(f'{results["ticker"]} RSI')
                    plt.xlabel('Date')
                    plt.ylabel('RSI')
                    plt.grid(True, alpha=0.3)
                    plt.legend()
                    st.pyplot(fig6)
                    
                    # Plot moving averages
                    fig7 = plt.figure(figsize=(10, 6))
                    plt.plot(results['merged_data']['date'], results['merged_data']['close'], 'b-', label='Close Price')
                    plt.plot(results['merged_data']['date'], results['merged_data']['ma5'], 'r-', label='5-Day MA')
                    plt.plot(results['merged_data']['date'], results['merged_data']['ma20'], 'g-', label='20-Day MA')
                    plt.title(f'{results["ticker"]} Price with Moving Averages')
                    plt.xlabel('Date')
                    plt.ylabel('Price ($)')
                    plt.grid(True, alpha=0.3)
                    plt.legend()
                    st.pyplot(fig7)
                
                # Add download button for the data
                csv = results['merged_data'].to_csv(index=False)
                st.download_button(
                    label="📥 Download Analysis Data",
                    data=csv,
                    file_name=f"{results['ticker']}_analysis.csv",
                    mime="text/csv",
                )

# Help Tab
with tabs[3]:
    st.header("ℹ️ How to Use This App")
    
    st.markdown("""
    ### 👋 Welcome to the Stock Price Predictor!
    
    This app uses machine learning to predict future stock prices based on historical data and sentiment analysis.
    
    **To get started:**
    1. Enter a stock ticker symbol in the sidebar (e.g., TSLA, AAPL, MSFT)
    2. Select the number of historical days to analyze
    3. Select the number of days to predict
    4. Click the "Visualize Data" button to see market analysis
    5. Click the "Run Prediction" button to generate price forecasts
    
    **Features:**
    - **Visualizations**: View price charts, technical indicators, sentiment analysis, and correlation heatmaps
    - **Predictions**: Get ML-powered price forecasts with evaluation metrics
    - **Data Sources**: The app uses data from:
      - Historical stock prices
      - Reddit sentiment analysis
      - Economic indicators
    
    **Tips:**
    - For better predictions, use at least 60 days of historical data
    - Check the Reddit sentiment analysis to understand market sentiment
    - The correlation heatmap shows relationships between different factors
    """)
    
    st.subheader("🔧 Troubleshooting")
    
    st.markdown("""
    If you encounter any issues:
    
    1. **Connection Problems**:
       - Make sure your Colab notebook is running
       - Check that the ngrok URL is correct in the .env file
       - Try refreshing the page
    
    2. **Slow Performance**:
       - Prediction can take a few minutes for complex models
       - Reduce the number of historical days if it's too slow
    
    3. **No Data Found**:
       - Make sure the ticker symbol is correct
       - Some stocks may not have Reddit sentiment data
    """)