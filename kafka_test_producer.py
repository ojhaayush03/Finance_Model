#!/usr/bin/env python
"""
Kafka Test Producer

This script sends test messages to Kafka topics to verify the streaming setup
without relying on external APIs that might have rate limits.
"""

import json
import time
import logging
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_test_producer')

def create_test_stock_data(ticker, num_records=10):
    """Create test stock data for the given ticker"""
    base_price = {
        'AAPL': 150.0,
        'MSFT': 300.0,
        'TSLA': 250.0,
        'GOOGL': 2500.0,
        'AMZN': 3000.0
    }.get(ticker, 100.0)
    
    records = []
    end_date = datetime.now()
    
    for i in range(num_records):
        date = end_date - timedelta(days=i)
        price_change = random.uniform(-5.0, 5.0)
        price = base_price + price_change
        volume = random.randint(1000000, 10000000)
        
        record = {
            'ticker': ticker,
            'date': date.strftime('%Y-%m-%d'),
            'open': round(price * 0.99, 2),
            'high': round(price * 1.02, 2),
            'low': round(price * 0.98, 2),
            'close': round(price, 2),
            'volume': volume,
            'timestamp': datetime.now().isoformat()
        }
        records.append(record)
    
    return records

def create_test_indicators(ticker, num_records=10):
    """Create test technical indicators for the given ticker"""
    records = []
    end_date = datetime.now()
    
    for i in range(num_records):
        date = end_date - timedelta(days=i)
        
        record = {
            'ticker': ticker,
            'date': date.strftime('%Y-%m-%d'),
            'sma_20': random.uniform(90, 110),
            'ema_20': random.uniform(90, 110),
            'rsi_14': random.uniform(30, 70),
            'macd': random.uniform(-2, 2),
            'macd_signal': random.uniform(-2, 2),
            'macd_hist': random.uniform(-1, 1),
            'volatility': random.uniform(1, 5),
            'timestamp': datetime.now().isoformat()
        }
        records.append(record)
    
    return records

def create_test_sentiment(ticker, source, num_records=5):
    """Create test sentiment data for the given ticker and source"""
    records = []
    end_date = datetime.now()
    
    for i in range(num_records):
        date = end_date - timedelta(hours=i*4)
        
        record = {
            'ticker': ticker,
            'source': source,
            'created_at': date.isoformat(),
            'text': f"Test {source} post about {ticker} stock performance #{i+1}",
            'sentiment_score': random.uniform(-1, 1),
            'sentiment_label': random.choice(['positive', 'neutral', 'negative']),
            'processed_at': datetime.now().isoformat()
        }
        records.append(record)
    
    return records

def main():
    """Main function to send test messages to Kafka"""
    # Kafka configuration
    bootstrap_servers = 'localhost:29092'  # Use the external port for local testing
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {str(e)}")
        return
    
    # List of tickers to generate data for
    tickers = ['AAPL', 'MSFT', 'TSLA']
    
    # Topic prefixes
    topic_prefixes = {
        'stock': 'stock_data_',
        'indicators': 'indicators_',
        'twitter': 'twitter_sentiment_',
        'reddit': 'reddit_sentiment_'
    }
    
    try:
        # Send test data in a loop
        iteration = 1
        while True:
            logger.info(f"Sending test data (iteration {iteration})")
            
            for ticker in tickers:
                # Send stock data
                stock_data = create_test_stock_data(ticker, num_records=5)
                stock_topic = f"{topic_prefixes['stock']}{ticker.lower()}"
                
                for record in stock_data:
                    producer.send(stock_topic, key=ticker, value=record)
                logger.info(f"Sent {len(stock_data)} stock data records to {stock_topic}")
                
                # Send indicator data
                indicators = create_test_indicators(ticker, num_records=5)
                indicators_topic = f"{topic_prefixes['indicators']}{ticker.lower()}"
                
                for record in indicators:
                    producer.send(indicators_topic, key=ticker, value=record)
                logger.info(f"Sent {len(indicators)} indicator records to {indicators_topic}")
                
                # Send Twitter sentiment
                twitter_data = create_test_sentiment(ticker, 'twitter', num_records=3)
                twitter_topic = f"{topic_prefixes['twitter']}{ticker.lower()}"
                
                for record in twitter_data:
                    producer.send(twitter_topic, key=ticker, value=record)
                logger.info(f"Sent {len(twitter_data)} Twitter sentiment records to {twitter_topic}")
                
                # Send Reddit sentiment
                reddit_data = create_test_sentiment(ticker, 'reddit', num_records=3)
                reddit_topic = f"{topic_prefixes['reddit']}{ticker.lower()}"
                
                for record in reddit_data:
                    producer.send(reddit_topic, key=ticker, value=record)
                logger.info(f"Sent {len(reddit_data)} Reddit sentiment records to {reddit_topic}")
            
            # Flush to ensure all messages are sent
            producer.flush()
            logger.info(f"All test data sent for iteration {iteration}")
            
            # Wait before sending the next batch
            iteration += 1
            time.sleep(10)  # Send new data every 10 seconds
            
    except KeyboardInterrupt:
        logger.info("Test producer interrupted by user")
    except Exception as e:
        logger.error(f"Error in test producer: {str(e)}")
    finally:
        # Close the producer
        if producer:
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()
