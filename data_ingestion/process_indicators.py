import pandas as pd
import numpy as np
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ticker = sys.argv[1] if len(sys.argv) > 1 else input("Enter the stock ticker you want to process (e.g., AAPL): ").strip().upper()

# Create absolute paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_data_dir = os.path.join(project_root, "raw_data")
processed_data_dir = os.path.join(project_root, "processed_data")

# Create directories if they don't exist
os.makedirs(raw_data_dir, exist_ok=True)
os.makedirs(processed_data_dir, exist_ok=True)

input_path = os.path.join(raw_data_dir, f"{ticker}_stock_data.csv")
output_path = os.path.join(processed_data_dir, f"{ticker}_indicators.csv")

df = pd.read_csv(input_path)
df = df.sort_values('date')

# Add indicators
df['Close'] = df['4. close']
df['SMA_10'] = df['Close'].rolling(window=10).mean()
df['EMA_10'] = df['Close'].ewm(span=10, adjust=False).mean()
df['Daily_Return'] = df['Close'].pct_change()
df['Volatility_10'] = df['Daily_Return'].rolling(window=10).std()

delta = df['Close'].diff()
gain = delta.where(delta > 0, 0.0)
loss = -delta.where(delta < 0, 0.0)
avg_gain = gain.rolling(window=14).mean()
avg_loss = loss.rolling(window=14).mean()
rs = avg_gain / avg_loss
df['RSI_14'] = 100 - (100 / (1 + rs))

df.to_csv(output_path, index=False)
print(f"Indicators saved to {output_path}")
