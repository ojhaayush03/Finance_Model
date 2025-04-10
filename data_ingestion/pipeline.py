import subprocess
import os
import sys

# User input
ticker = input("Enter the stock ticker (e.g., AAPL, TSLA): ").strip().upper()

# Paths
raw_path = f"../raw_data/{ticker}_stock_data.csv"
processed_path = f"../processed_data/{ticker}_indicators.csv"
hdfs_dir = f"/user/ayush/financial_project/processed_data/"
hive_script_path = "../hadoop_scripts/create_stock_table.hql"

# === Step 1: Fetch Stock Data ===
print(f"\n📥 Fetching stock data for {ticker}...")
try:
    subprocess.run(["python", "fetch_stock_data.py"], check=True)
except subprocess.CalledProcessError:
    print("❌ Error fetching data.")
    sys.exit(1)

# === Step 2: Process Indicators ===
print(f"\n⚙️ Processing indicators for {ticker}...")
try:
    subprocess.run(["python", "process_indicators.py"], check=True)
except subprocess.CalledProcessError:
    print("❌ Error processing indicators.")
    sys.exit(1)

# === Step 3: Upload to HDFS ===
print(f"\n🚀 Uploading {ticker}_indicators.csv to HDFS...")
try:
    subprocess.run(["python", "upload_to_hdfs.py"], check=True)
except subprocess.CalledProcessError:
    print("❌ HDFS upload failed.")
    sys.exit(1)

# === Step 4: Run Hive Script ===
print(f"\n📊 Running Hive script...")
try:
    subprocess.run(f'cmd /c "hive -f {hive_script_path}"', shell=True, check=True)
except subprocess.CalledProcessError:
    print("❌ Hive script execution failed.")
    sys.exit(1)

print(f"\n✅ Pipeline for {ticker} completed successfully!")
