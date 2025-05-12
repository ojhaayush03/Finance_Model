import sys
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import os
import time
import random
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_config

# Get API key from config
api_key = get_config("ALPHA_VANTAGE_API_KEY", "UWDNXAGGFFB6TT01")
ticker = sys.argv[1] if len(sys.argv) > 1 else input("Enter the stock ticker (e.g., AAPL, TSLA, MSFT): ").strip().upper()

# Create absolute paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_data_dir = os.path.join(project_root, "raw_data")

# Create directory if it doesn't exist
os.makedirs(raw_data_dir, exist_ok=True)

# Define save path
save_path = os.path.join(raw_data_dir, f"{ticker}_stock_data.csv")

# Check if we already have recent data for this ticker (within the last day)
def is_data_recent(file_path, max_age_hours=24):
    if not os.path.exists(file_path):
        return False
    
    file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    age = datetime.now() - file_mod_time
    
    return age < timedelta(hours=max_age_hours)

# If we have recent data, use it instead of making an API call
if is_data_recent(save_path):
    print(f"Using existing data for {ticker} (less than 24 hours old)")
    print(f"Data loaded from {save_path}")
    sys.exit(0)  # Exit with success code

# Implement retry mechanism for Alpha Vantage API
max_retries = 3
retry_count = 0
backoff_time = 2  # Initial backoff time in seconds

while retry_count <= max_retries:
    try:
        # Fetch data from Alpha Vantage
        ts = TimeSeries(key=api_key, output_format='pandas')
        data, _ = ts.get_daily(symbol=ticker, outputsize='full')
        
        # Save to absolute path
        data.to_csv(save_path)
        print(f"Data saved to {save_path}")
        break
    except Exception as e:
        error_message = str(e)
        retry_count += 1
        
        if "API call frequency" in error_message or "premium" in error_message:
            print(f"Alpha Vantage API rate limit hit. This is normal with the free tier.")
            
            if os.path.exists(save_path):
                print(f"Using existing data for {ticker} (cached)")
                sys.exit(0)  # Exit with success code
            
            if retry_count > max_retries:
                print(f"Error: Maximum retries exceeded. Alpha Vantage API rate limit hit.")
                print(f"Consider upgrading to a premium plan or waiting before trying again.")
                sys.exit(1)  # Exit with error code
            
            # Calculate backoff time with jitter
            sleep_time = backoff_time + random.uniform(0, 1)
            print(f"Retrying in {sleep_time:.2f} seconds (attempt {retry_count}/{max_retries})")
            time.sleep(sleep_time)
            
            # Exponential backoff
            backoff_time *= 2
        else:
            print(f"Error fetching data for {ticker}: {error_message}")
            sys.exit(1)  # Exit with error code
