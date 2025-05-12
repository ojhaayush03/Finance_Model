import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
import pandas as pd
import requests
from alpha_vantage.timeseries import TimeSeries
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_producer')

class StockDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer for stock data streaming"""
        self.bootstrap_servers = bootstrap_servers
        self.api_key = os.environ.get('ALPHA_VANTAGE_API_KEY', 'UWDNXAGGFFB6TT01')
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f"✅ Kafka producer connected to {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Kafka: {str(e)}")
            self.producer = None
    
    def fetch_intraday_data(self, ticker, interval='1min'):
        """Fetch intraday data from Alpha Vantage"""
        try:
            ts = TimeSeries(key=self.api_key, output_format='pandas')
            data, _ = ts.get_intraday(symbol=ticker, interval=interval, outputsize='compact')
            logger.info(f"✅ Fetched intraday data for {ticker}")
            return data
        except Exception as e:
            logger.error(f"❌ Failed to fetch intraday data: {str(e)}")
            return None
    
    def fetch_latest_quote(self, ticker):
        """Fetch latest quote data from Alpha Vantage"""
        try:
            url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={self.api_key}'
            r = requests.get(url)
            data = r.json()
            
            if 'Global Quote' in data and data['Global Quote']:
                quote = data['Global Quote']
                logger.info(f"✅ Fetched latest quote for {ticker}")
                return {
                    'symbol': ticker,
                    'price': float(quote.get('05. price', 0)),
                    'change': float(quote.get('09. change', 0)),
                    'change_percent': quote.get('10. change percent', '0%').replace('%', ''),
                    'volume': int(quote.get('06. volume', 0)),
                    'timestamp': datetime.now().isoformat()
                }
            else:
                logger.warning(f"⚠️ No quote data for {ticker}")
                return None
        except Exception as e:
            logger.error(f"❌ Failed to fetch quote: {str(e)}")
            return None
    
    def simulate_market_data(self, ticker, base_price=None):
        """Simulate real-time market data when market is closed"""
        if base_price is None:
            base_price = 100 + random.random() * 100
            
        # Random price movement
        price_change = (random.random() - 0.5) * 2  # Between -1 and 1
        new_price = base_price + price_change
        
        # Random volume
        volume = int(random.random() * 10000)
        
        return {
            'symbol': ticker,
            'price': round(new_price, 2),
            'change': round(price_change, 2),
            'change_percent': round((price_change / base_price) * 100, 2),
            'volume': volume,
            'timestamp': datetime.now().isoformat(),
            'simulated': True
        }
    
    def produce_stock_data(self, tickers, interval_seconds=60, topic_prefix='stock_data_'):
        """Continuously produce stock data to Kafka topics"""
        if not self.producer:
            logger.error("❌ Kafka producer not initialized")
            return
        
        logger.info(f"🚀 Starting stock data stream for {tickers}")
        
        # Track last price for simulation
        last_prices = {ticker: None for ticker in tickers}
        
        try:
            while True:
                for ticker in tickers:
                    topic = f"{topic_prefix}{ticker.lower()}"
                    
                    # Try to get real data first
                    data = self.fetch_latest_quote(ticker)
                    
                    # If real data fails, simulate
                    if not data:
                        data = self.simulate_market_data(ticker, last_prices[ticker])
                    
                    # Update last price for future simulation
                    last_prices[ticker] = data['price']
                    
                    # Send to Kafka
                    self.producer.send(topic, key=ticker, value=data)
                    logger.info(f"✅ Sent {ticker} data to topic {topic}")
                
                # Flush to ensure all messages are sent
                self.producer.flush()
                
                # Wait for next interval
                logger.info(f"⏱️ Waiting {interval_seconds} seconds for next update...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("⛔ Producer stopped by user")
        except Exception as e:
            logger.error(f"❌ Error in producer: {str(e)}")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("🔒 Producer closed")

if __name__ == "__main__":
    # Example usage
    producer = StockDataProducer()
    
    # Ask for tickers
    ticker_input = input("Enter stock tickers to stream (comma-separated, e.g., AAPL,MSFT,TSLA): ")
    tickers = [t.strip().upper() for t in ticker_input.split(',')]
    
    # Set update interval
    try:
        interval = int(input("Enter update interval in seconds (default: 60): ") or "60")
    except ValueError:
        interval = 60
    
    # Start producing
    producer.produce_stock_data(tickers, interval)
