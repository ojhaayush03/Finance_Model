#!/usr/bin/env python
"""
Kafka Verification Script

This script verifies that Kafka is working properly by:
1. Creating necessary topics
2. Publishing test messages
3. Consuming and displaying those messages
"""

import json
import time
import logging
import argparse
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_verify')

def create_topics(bootstrap_servers, tickers):
    """Create Kafka topics for the given tickers"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='financial-trends-admin'
        )
        
        # Define topic prefixes
        topic_prefixes = [
            'stock_data_',
            'indicators_',
            'sentiment_'
        ]
        
        # Create topics for each ticker and prefix
        topic_list = []
        for prefix in topic_prefixes:
            for ticker in tickers:
                topic_name = f"{prefix}{ticker.lower()}"
                topic_list.append(NewTopic(
                    name=topic_name,
                    num_partitions=1,
                    replication_factor=1
                ))
        
        # Create topics (ignore if they already exist)
        for topic in topic_list:
            try:
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info(f"Created topic: {topic.name}")
            except TopicAlreadyExistsError:
                logger.info(f"Topic already exists: {topic.name}")
        
        admin_client.close()
        return True
    except Exception as e:
        logger.error(f"Failed to create topics: {str(e)}")
        return False

def publish_test_messages(bootstrap_servers, tickers, num_messages=5):
    """Publish test messages to Kafka topics"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        logger.info(f"Connected to Kafka at {bootstrap_servers}")
        
        # Publish messages to stock_data topics
        for ticker in tickers:
            topic = f"stock_data_{ticker.lower()}"
            
            for i in range(num_messages):
                # Create a test message
                message = {
                    'symbol': ticker,
                    'price': 100 + i * 0.5,
                    'volume': 1000 * (i + 1),
                    'timestamp': datetime.now().isoformat(),
                    'test_message': True,
                    'message_id': i + 1
                }
                
                # Send the message
                future = producer.send(topic, key=ticker, value=message)
                result = future.get(timeout=10)
                logger.info(f"Published message {i+1} to {topic} at partition {result.partition}, offset {result.offset}")
                
                # Small delay between messages
                time.sleep(0.5)
        
        # Flush and close the producer
        producer.flush()
        producer.close()
        
        logger.info(f"Published {num_messages} test messages for each ticker")
        return True
    except Exception as e:
        logger.error(f"Failed to publish test messages: {str(e)}")
        return False

def consume_test_messages(bootstrap_servers, tickers, timeout_seconds=10):
    """Consume and display test messages from Kafka topics"""
    for ticker in tickers:
        topic = f"stock_data_{ticker.lower()}"
        
        try:
            logger.info(f"Consuming messages from {topic}...")
            
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                consumer_timeout_ms=timeout_seconds * 1000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            message_count = 0
            for message in consumer:
                message_count += 1
                logger.info(f"Received message from {topic}:")
                logger.info(f"  Partition: {message.partition}, Offset: {message.offset}")
                logger.info(f"  Key: {message.key.decode('utf-8') if message.key else None}")
                logger.info(f"  Value: {message.value}")
                logger.info("---")
            
            consumer.close()
            
            if message_count == 0:
                logger.warning(f"No messages found in {topic}")
            else:
                logger.info(f"Consumed {message_count} messages from {topic}")
                
        except Exception as e:
            logger.error(f"Error consuming from {topic}: {str(e)}")

def verify_kafka(bootstrap_servers, tickers, action):
    """Verify Kafka setup by creating topics and publishing/consuming messages"""
    if action in ['all', 'create']:
        create_topics(bootstrap_servers, tickers)
    
    if action in ['all', 'publish']:
        publish_test_messages(bootstrap_servers, tickers)
    
    if action in ['all', 'consume']:
        consume_test_messages(bootstrap_servers, tickers)
    
    logger.info("Kafka verification completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify Kafka Setup")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", 
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--tickers", nargs="+", default=["AAPL", "MSFT", "TSLA"],
                        help="Stock ticker symbols (default: AAPL MSFT TSLA)")
    parser.add_argument("--action", choices=['all', 'create', 'publish', 'consume'], default='all',
                        help="Action to perform (default: all)")
    
    args = parser.parse_args()
    
    verify_kafka(args.bootstrap_servers, args.tickers, args.action)
