#!/usr/bin/env python
"""
Kafka Integration Test Script

This script tests the Kafka integration by creating topics and publishing test data
to verify that the Kafka setup is working correctly.

NOTE: This script is designed to be run inside the Docker environment where Kafka is running.
If you run it directly on your local machine without Kafka, it will fail to connect.
"""

import sys
import time
import socket
import logging
import argparse
from datetime import datetime
from confluent_kafka.admin import AdminClient, NewTopic
from data_ingestion.kafka_producer import FinancialDataProducer
from data_ingestion.kafka_consumer import StockDataConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_test')

# Check if Kafka is available
def is_kafka_available(host='kafka', port=9092, timeout=5, max_retries=5, retry_delay=2):
    """Check if Kafka is available at the specified host and port with retries"""
    for attempt in range(max_retries):
        try:
            socket.setdefaulttimeout(timeout)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.close()
            logger.info(f"✅ Successfully connected to Kafka at {host}:{port}")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} - Kafka not available at {host}:{port} - {str(e)}")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
    return False
    """Check if Kafka is available at the specified host and port"""
    try:
        socket.setdefaulttimeout(timeout)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        s.close()
        return True
    except Exception as e:
        logger.warning(f"Kafka not available at {host}:{port} - {str(e)}")
        return False

def test_topic_creation(tickers):
    """Test the creation of Kafka topics"""
    logger.info("Testing Kafka topic creation...")
    
    # Create an AdminClient
    admin_client = AdminClient({'bootstrap.servers': 'kafka:9092'})
    
    # Define topic prefixes
    topic_prefixes = [
        'stock_data_',
        'indicators_',
        'twitter_sentiment_',
        'reddit_sentiment_',
        'financial_news_',
    ]
    
    # Create topics for each ticker and data type
    topics_to_create = []
    for ticker in tickers:
        ticker_lower = ticker.lower()
        for prefix in topic_prefixes:
            topic_name = f"{prefix}{ticker_lower}"
            topics_to_create.append(NewTopic(
                topic_name,
                num_partitions=3,
                replication_factor=1
            ))
    
    # Add economic indicators topic
    topics_to_create.append(NewTopic(
        'economic_indicators_general',
        num_partitions=3,
        replication_factor=1
    ))
    
    # Create the topics
    try:
        futures = admin_client.create_topics(topics_to_create)
        
        # Wait for operation to complete
        for topic, future in futures.items():
            try:
                future.result()  # Wait for completion
                logger.info(f"Created topic: {topic}")
            except Exception as e:
                if 'already exists' in str(e):
                    logger.info(f"Topic {topic} already exists")
                else:
                    logger.error(f"Failed to create topic {topic}: {e}")
        
        logger.info(f"✅ Successfully created/verified {len(topics_to_create)} topics")
        return [topic.topic for topic in topics_to_create]
    except Exception as e:
        logger.error(f"❌ Failed to create topics: {e}")
        return []

def test_producer(tickers, max_retries=3):
    """Test the Kafka producer by publishing test data with retries"""
    logger.info("Testing Kafka producer...")
    
    try:
        producer = FinancialDataProducer()
    except Exception as e:
        logger.error(f"Failed to create producer: {e}")
        return False
    
    try:
        # Publish test data for each ticker
        for ticker in tickers:
            # Stock data
            stock_data = {
                "ticker": ticker,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "open": 100.0,
                "high": 105.0,
                "low": 98.0,
                "close": 102.5,
                "volume": 1000000,
                "source": "test_data"
            }
            producer.produce_stock_data(ticker, stock_data)
            
            # Indicator data
            indicator_data = {
                "ticker": ticker,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "indicator": "RSI",
                "value": 65.5,
                "source": "test_data"
            }
            producer.produce_indicator_data(ticker, "RSI", indicator_data)
            
            # News data
            news_data = {
                "ticker": ticker,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "headline": f"Test news for {ticker}",
                "summary": f"This is a test news article for {ticker}",
                "sentiment": 0.75,
                "source": "test_data"
            }
            producer.produce_news_data(ticker, news_data)
            
            # Twitter sentiment
            twitter_data = {
                "ticker": ticker,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "sentiment_score": 0.8,
                "tweet_count": 100,
                "source": "test_data"
            }
            producer.produce_twitter_sentiment(ticker, twitter_data)
            
            # Reddit sentiment
            reddit_data = {
                "ticker": ticker,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "sentiment_score": 0.6,
                "post_count": 50,
                "source": "test_data"
            }
            producer.produce_reddit_sentiment(ticker, reddit_data)
        
        # Economic indicator data
        economic_data = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "indicator": "GDP",
            "value": 3.2,
            "source": "test_data"
        }
        producer.produce_economic_data("GDP", economic_data)
        
        # Flush to ensure all messages are sent
        producer.flush()
        logger.info("✅ Test data published to Kafka topics")
        return True
    except Exception as e:
        logger.error(f"Failed to publish test data: {e}")
        return False
    
        # Flush to ensure all messages are sent
        producer.flush()
        logger.info("✅ Test data published to Kafka topics")
        return True
    except Exception as e:
        logger.error(f"Failed to publish test data: {e}")
        return False

def test_consumer(tickers, duration=30):
    """Test the Kafka consumer by consuming data for a specified duration"""
    logger.info(f"Testing Kafka consumer for {duration} seconds...")
    
    consumer = StockDataConsumer()
    
    # Start consuming data
    consumer_threads = consumer.consume_multiple_tickers(tickers)
    logger.info(f"Started {len(consumer_threads)} consumer threads")
    
    # Run for specified duration
    logger.info(f"Consumer running for {duration} seconds...")
    time.sleep(duration)
    
    # Stop consuming
    consumer.stop_consumers()
    logger.info("✅ Consumer test completed")

def main():
    """Main entry point for the test script"""
    parser = argparse.ArgumentParser(description="Kafka Integration Test Script")
    parser.add_argument("tickers", nargs="+", help="Stock ticker symbols (e.g., AAPL TSLA MSFT)")
    parser.add_argument("--skip-topics", action="store_true", help="Skip topic creation")
    parser.add_argument("--skip-producer", action="store_true", help="Skip producer test")
    parser.add_argument("--skip-consumer", action="store_true", help="Skip consumer test")
    parser.add_argument("--consumer-duration", type=int, default=30, help="Duration to run the consumer test in seconds")
    parser.add_argument("--kafka-host", default="kafka", help="Kafka bootstrap server host")
    parser.add_argument("--kafka-port", type=int, default=9092, help="Kafka bootstrap server port")
    parser.add_argument("--force", action="store_true", help="Force run without Kafka availability check")
    parser.add_argument("--max-retries", type=int, default=5, help="Maximum number of connection retries")
    
    args = parser.parse_args()
    
    # Check Kafka availability unless --force is used
    if not args.force:
        if not is_kafka_available(args.kafka_host, args.kafka_port, max_retries=args.max_retries):
            logger.error("Kafka is not available. Use --force to run anyway.")
            sys.exit(1)
    
    try:
        # Create topics if not skipped
        topics = []
        if not args.skip_topics:
            topics = test_topic_creation(args.tickers)
            if not topics:
                logger.error("Topic creation failed")
                sys.exit(1)
        
        # Run producer test if not skipped
        if not args.skip_producer:
            if not test_producer(args.tickers, max_retries=args.max_retries):
                logger.error("Producer test failed")
                sys.exit(1)
        
        # Run consumer test if not skipped
        if not args.skip_consumer:
            test_consumer(args.tickers, args.consumer_duration)
        
        logger.info("✅ All tests completed successfully")
    except KeyboardInterrupt:
        logger.info("\nTests interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        sys.exit(1)
    """Main entry point for the test script"""
    parser = argparse.ArgumentParser(description="Kafka Integration Test Script")
    parser.add_argument("tickers", nargs="+", help="Stock ticker symbols (e.g., AAPL TSLA MSFT)")
    parser.add_argument("--skip-topics", action="store_true", help="Skip topic creation")
    parser.add_argument("--skip-producer", action="store_true", help="Skip producer test")
    parser.add_argument("--skip-consumer", action="store_true", help="Skip consumer test")
    parser.add_argument("--consumer-duration", type=int, default=30, 
                        help="Duration to run the consumer test in seconds")
    parser.add_argument("--kafka-host", default="localhost", help="Kafka bootstrap server host")
    parser.add_argument("--kafka-port", type=int, default=9092, help="Kafka bootstrap server port")
    parser.add_argument("--force", action="store_true", help="Force run even if Kafka is not available")
    
    args = parser.parse_args()
    
    # Check if Kafka is available
    if not is_kafka_available(args.kafka_host, args.kafka_port) and not args.force:
        logger.error("\n" + "=" * 80)
        logger.error("Kafka is not available at {}:{}.".format(args.kafka_host, args.kafka_port))
        logger.error("This script is designed to be run inside the Docker environment where Kafka is running.")
        logger.error("\nTo run the script with Docker:")
        logger.error("1. Start the Docker containers:")
        logger.error("   docker-compose up -d")
        logger.error("\n2. Run this script inside the Docker container:")
        logger.error("   docker exec -it financial-trends-app python test_kafka_integration.py AAPL MSFT TSLA")
        logger.error("\nIf you want to run the script anyway (for testing purposes), use the --force flag.")
        logger.error("=" * 80)
        return
    
    logger.info(f"Starting Kafka integration test for tickers: {', '.join(args.tickers)}")
    
    # Step 1: Test topic creation
    if not args.skip_topics:
        test_topic_creation(args.tickers)
    else:
        logger.info("Skipping topic creation test")
    
    # Step 2: Test producer
    if not args.skip_producer:
        test_producer(args.tickers)
    else:
        logger.info("Skipping producer test")
    
    # Step 3: Test consumer
    if not args.skip_consumer:
        test_consumer(args.tickers, args.consumer_duration)
    else:
        logger.info("Skipping consumer test")
    
    logger.info("Kafka integration test completed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in test script: {str(e)}")
        sys.exit(1)
