# Colab API Integration for Streamlit Frontend

import json
import os
import requests
import time
from typing import Dict, Any, Optional
# At the top of the file
from env_config import get_ngrok_url, get_prediction_endpoint, get_health_endpoint


class ColabIntegration:
    """Class to handle integration between local Streamlit and Google Colab"""
    

    def __init__(self, ngrok_url: Optional[str] = None):
        """Initialize with ngrok URL from environment variables or parameter"""
        # Get ngrok URL from environment or parameter
        self.ngrok_url = ngrok_url or get_ngrok_url()
        
        # Initialize API URL if ngrok URL is available
        if self.ngrok_url:
            self.set_ngrok_url(self.ngrok_url)
        else:
            self.api_url = None

    
    def set_ngrok_url(self, url: str):
        """Set the ngrok URL for the Colab API"""
        # Clean up URL if needed
        url = url.strip()
        if url.endswith('/'):
            url = url[:-1]
            
        self.ngrok_url = url
        self.api_url = f"{url}/predict"
        
        # Test connection
        try:
            if self.is_connected():
                print(f"Successfully connected to Colab API at {url}")
                return True
            else:
                print(f"Failed to connect to Colab API at {url}")
                return False
        except Exception as e:
            print(f"Error testing connection: {e}")
            return False
    
    def is_connected(self) -> bool:
        """Check if connected to Colab API"""
        if not self.ngrok_url:
            return False
        
        try:
            # Use the health endpoint function
            health_url = get_health_endpoint()
            if not health_url:
                health_url = f"{self.ngrok_url}/health"
                
            # Use a shorter timeout for the health check
            response = requests.get(health_url, timeout=5)
            return response.status_code == 200
        except requests.exceptions.Timeout:
            print("Connection to Colab API timed out during health check")
            return False
        except Exception as e:
            print(f"Error checking connection: {e}")
            return False
    
    def predict_stock(self, ticker: str, days: int) -> Dict[str, Any]:
        """Send prediction request to Colab API"""
        if not self.api_url:
            raise ValueError("No Colab API URL set. Use set_ngrok_url() first.")
        
        payload = {
            "ticker": ticker,
            "days": days
        }
        
        try:
            # First check if we're connected
            if not self.is_connected():
                raise ConnectionError("Colab API is not responding to health checks")
                
            print(f"Sending prediction request to {self.api_url}")
            # Increased timeout to 5 minutes for PySpark processing
            response = requests.post(self.api_url, json=payload, timeout=300)  
            
            # Handle server errors with more detailed information
            if response.status_code >= 500:
                error_msg = f"Colab server error (HTTP {response.status_code})"
                try:
                    error_details = response.json()
                    if isinstance(error_details, dict) and 'error' in error_details:
                        error_msg += f": {error_details['error']}"
                except:
                    error_msg += ". Check the Colab notebook for errors."
                raise ConnectionError(error_msg)
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            raise ConnectionError("Prediction request to Colab API timed out after 5 minutes. The PySpark prediction may take longer than expected.")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Failed to connect to Colab API: {e}")
        except json.JSONDecodeError:
            raise ValueError("Invalid response from Colab API")

# Singleton instance for use across the app
colab_api = ColabIntegration()

# Example usage
def predict_with_colab(ticker: str, days: int) -> Dict[str, Any]:
    """Helper function to predict using Colab API"""
    if not colab_api.is_connected():
        raise ConnectionError("Not connected to Colab API")
    
    return colab_api.predict_stock(ticker, days)