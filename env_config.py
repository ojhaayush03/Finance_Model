# Environment configuration for Financial Trends Project

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get ngrok URL from environment variables
NGROK_URL = os.getenv("NGROK_URL", "https://8c96b5d0e3db.ngrok-free.app/")

# Function to get the ngrok URL
def get_ngrok_url():
    return NGROK_URL

# Function to get the prediction endpoint
def get_prediction_endpoint():
    url = NGROK_URL
    # Clean up URL if needed
    if url.endswith('/'):
        url = url[:-1]
    return f"{url}/predict"

# Function to get the health endpoint
def get_health_endpoint():
    url = NGROK_URL
    # Clean up URL if needed
    if url.endswith('/'):
        url = url[:-1]
    return f"{url}/health"

# Function to get the visualization endpoint
def get_visualization_endpoint():
    url = NGROK_URL
    # Clean up URL if needed
    if url.endswith('/'):
        url = url[:-1]
    return f"{url}/visualize"

# Print configuration on import
print(f"Loaded environment configuration. NGROK_URL: {NGROK_URL}")
