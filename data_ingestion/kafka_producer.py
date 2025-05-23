import json
import time
import random
import logging
import os
import sys
import uuid
from datetime import datetime
from confluent_kafka import Producer
import requests
from alpha_vantage.timeseries import TimeSeries

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_producer')

class FinancialDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer for financial data streaming"""
        self.bootstrap_servers = bootstrap_servers
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'financial_data_producer_{uuid.uuid4().hex[:8]}'
        }
        self.producer = Producer(self.producer_config)
        self.running = False
        
        # Define topic prefixes for different data types
        self.topic_prefixes = {
            'stock_data': 'stock_data_',
            'indicators': 'indicators_',
            'news': 'financial_news_',
            'twitter': 'twitter_sentiment_',
            'reddit': 'reddit_sentiment_',
            'economic': 'economic_indicators_'
        }
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def get_topic_name(self, data_type, identifier):
        """Get the topic name for a specific data type and identifier"""
        if data_type in self.topic_prefixes:
            # For economic indicators, we use a general topic
            if data_type == 'economic' and identifier == 'general':
                return f"{self.topic_prefixes[data_type]}general"
            # For all other data types, we append the identifier (usually ticker)
            return f"{self.topic_prefixes[data_type]}{identifier.lower()}"
        else:
            # Default to using data_type as the topic name
            return f"{data_type}_{identifier.lower()}"
    
    def produce_message(self, data_type, identifier, data, key=None):
        """Produce a message to a Kafka topic
        
        Args:
            data_type (str): Type of data ('stock_data', 'indicators', 'news', etc.)
            identifier (str): Identifier for the data (ticker symbol, 'general', etc.)
            data (dict): Data to be sent
            key (str, optional): Message key for partitioning
        """
        topic = self.get_topic_name(data_type, identifier)
        
        try:
            # Add metadata if not present
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().isoformat()
            if 'ticker' not in data and data_type != 'economic':
                data['ticker'] = identifier.upper()
            
            # Convert data to JSON
            message = json.dumps(data).encode('utf-8')
            
            # Encode key if provided
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce message
            self.producer.produce(
                topic=topic,
                value=message,
                key=key_bytes,
                callback=self.delivery_report
            )
            
            # Poll to handle delivery reports
            self.producer.poll(0)
            
            logger.info(f"Produced message to {topic}")
            return True
        except Exception as e:
            logger.error(f"Error producing message to {topic}: {str(e)}")
            return False
    
    def produce_stock_data(self, ticker, data):
        """Produce stock data for a specific ticker"""
        return self.produce_message('stock_data', ticker, data, key=ticker)
    
    def produce_indicator_data(self, ticker, indicator_name, data):
        """Produce technical indicator data for a specific ticker"""
        data['indicator'] = indicator_name
        return self.produce_message('indicators', ticker, data, key=ticker)
    
    def produce_news_data(self, ticker, news_data):
        """Produce financial news data for a specific ticker"""
        return self.produce_message('news', ticker, news_data, key=ticker)
    
    def produce_twitter_sentiment(self, ticker, sentiment_data):
        """Produce Twitter sentiment data for a specific ticker"""
        return self.produce_message('twitter', ticker, sentiment_data, key=ticker)
    
    def produce_reddit_sentiment(self, ticker, sentiment_data):
        """Produce Reddit sentiment data for a specific ticker"""
        return self.produce_message('reddit', ticker, sentiment_data, key=ticker)
    
    def produce_economic_data(self, indicator_name, data):
        """Produce economic indicator data"""
        data['indicator'] = indicator_name
        return self.produce_message('economic', 'general', data)
    
    def flush(self):
        """Flush the producer to ensure all messages are sent"""
        self.producer.flush()
        logger.info("Producer flushed")
    
    def stop(self):
        """Stop the producer"""
        self.running = False
        self.flush()
        logger.info("Producer stopped")


# For backward compatibility
class StockDataProducer(FinancialDataProducer):
    def __init__(self, bootstrap_servers='localhost:9092', topic_prefix='stock_data_'):
        """Initialize Kafka producer for stock data streaming with backward compatibility"""
        super().__init__(bootstrap_servers)
        self.topic_prefix = topic_prefix

    def fetch_intraday_data(self, ticker, interval='1min'):
        """Fetch intraday data from Alpha Vantage"""
        try:
            ts = TimeSeries(key=os.environ.get('ALPHA_VANTAGE_API_KEY', 'UWDNXAGGFFB6TT01'), output_format='pandas')
            data, _ = ts.get_intraday(symbol=ticker, interval=interval, outputsize='compact')
            logger.info(f"✅ Fetched intraday data for {ticker}")
            return data
        except Exception as e:
            logger.error(f"❌ Failed to fetch intraday data: {str(e)}")
            return None
    
    def fetch_latest_quote(self, ticker):
        """Fetch latest quote data from Alpha Vantage"""
        try:
            url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={os.environ.get("ALPHA_VANTAGE_API_KEY", "UWDNXAGGFFB6TT01")}'
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
