"""
Configuration file for API keys and other settings.
IMPORTANT: Do not commit this file to version control!
"""

import os

# Alpha Vantage API key (for stock data)
ALPHA_VANTAGE_API_KEY = "UWDNXAGGFFB6TT01"  # Your existing key

# NewsAPI key (for financial news)
NEWS_API_KEY = "your_news_api_key_here"

# FRED API key (for economic indicators)
# Note: FRED API key must be a 32-character alpha-numeric lowercase string
# You need to register at https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = "7cdffee84d8491856b12e84a16e54f7e" 

# Twitter/X API credentials
TWITTER_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAADPT1AEAAAAA58xx5owkWFmvSGHgqbPvgT0PDwc%3DWVK4CgxOhPNysGTEgxbEEBzE74UH7TIjXz2TfOQH2T1pGBBiJQ"

# Reddit API credentials
# Note: Check your Reddit API credentials at https://www.reddit.com/prefs/apps
# Make sure to use the correct client ID and secret
REDDIT_CLIENT_ID = "CBm5wcQ_jPg-s6n0gfn6bA"  # This is your actual client ID from the image
REDDIT_CLIENT_SECRET = "kU_slgn1yjb7s-kEHaV3KnwwRSgJ2g"
REDDIT_USER_AGENT = "financial_trends_project (by /u/Quirky_Bluejay6778)"


# MongoDB configuration
MONGODB_URI = "mongodb://localhost:27017/"
MONGODB_DB_NAME = "financial_db"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_PREFIX = "stock_data_"

# Helper function to get config values with environment variable fallback
def get_config(key, default=None):
    """Get configuration value with environment variable fallback"""
    env_value = os.environ.get(key)
    if env_value:
        return env_value
    
    # Get from this module's globals
    return globals().get(key, default)
