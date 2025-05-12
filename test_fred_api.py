import os
import sys
import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import get_config

def test_fred_api():
    """Test the FRED API connection and data retrieval"""
    # Get the API key from config
    api_key = get_config('FRED_API_KEY')
    
    print(f"FRED API Key: {api_key[:5]}...{api_key[-5:]} (length: {len(api_key)})")
    print(f"Is alphanumeric: {api_key.isalnum()}")
    print(f"Is lowercase: {api_key.islower()}")
    
    try:
        # Initialize the FRED API client
        fred = Fred(api_key=api_key)
        
        # Try to fetch some data
        indicators = {
            'GDP': 'GDP',                      # Gross Domestic Product
            'UNRATE': 'UNRATE',                # Unemployment Rate
            'CPIAUCSL': 'CPIAUCSL',            # Consumer Price Index (Inflation)
        }
        
        # Set date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # 1 year of data
        
        # Try to fetch each indicator
        for name, series_id in indicators.items():
            print(f"\nFetching {name} ({series_id})...")
            try:
                data = fred.get_series(
                    series_id, 
                    observation_start=start_date,
                    observation_end=end_date
                )
                print(f"✅ Successfully retrieved {len(data)} data points for {name}")
                print(f"Sample data: {data.head(3)}")
            except Exception as e:
                print(f"❌ Error fetching {name}: {str(e)}")
        
        return True
    except Exception as e:
        print(f"❌ Error initializing FRED API: {str(e)}")
        return False

if __name__ == "__main__":
    print("Testing FRED API...")
    success = test_fred_api()
    if success:
        print("\n✅ FRED API test completed successfully")
    else:
        print("\n❌ FRED API test failed")
