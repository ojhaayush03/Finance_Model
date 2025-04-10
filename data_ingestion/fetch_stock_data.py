import sys
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import os

api_key = "UWDNXAGGFFB6TT01"  # 🔁 Replace with your actual Alpha Vantage API key
ticker = sys.argv[1] if len(sys.argv) > 1 else input("Enter the stock ticker (e.g., AAPL, TSLA, MSFT): ").strip().upper()

api_key = "YOUR_API_KEY"
ts = TimeSeries(key=api_key, output_format='pandas')
data, _ = ts.get_daily(symbol=ticker, outputsize='full')

save_path = f"../raw_data/{ticker}_stock_data.csv"
data.to_csv(save_path)

print(f"✅ Data saved to {save_path}")
