import subprocess
import sys
from data_ingestion.db_utils import MongoDBClient

# Initialize MongoDB client
db_client = MongoDBClient()

# User input
ticker = input("Enter the stock ticker (e.g., AAPL, TSLA): ").strip().upper()

# === Step 1: Fetch Stock Data ===
print(f"\n📥 Fetching stock data for {ticker}...")
try:
    subprocess.run(["python", "fetch_stock_data.py"], check=True)
except subprocess.CalledProcessError:
    print("❌ Error fetching data.")
    sys.exit(1)

# === Step 2: Process Indicators ===
print(f"\n🔄 Processing indicators for {ticker}...")
try:
    subprocess.run(["python", "process_indicators.py"], check=True)
except subprocess.CalledProcessError:
    print("❌ Error processing indicators.")
    sys.exit(1)

# === Step 3: Store in MongoDB ===
print(f"\n🚀 Storing {ticker} data in MongoDB...")
try:
    # Read the processed data
    import pandas as pd
    data = pd.read_csv(f"../processed_data/{ticker}_indicators.csv")
    
    # Store in MongoDB
    records_stored = db_client.store_stock_data(ticker, data)
    print(f"✅ Stored {records_stored} records in MongoDB")
except Exception as e:
    print(f"❌ MongoDB storage failed: {str(e)}")
    sys.exit(1)

print(f"\n✅ Pipeline for {ticker} completed successfully!")
