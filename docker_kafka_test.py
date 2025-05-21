#!/usr/bin/env python
"""
Docker Kafka Test Script

This script creates Kafka topics and publishes test data in the Docker container.
"""

import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_test')

def create_topics(bootstrap_servers='kafka:9092'):
    """Create Kafka topics"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='financial-trends-admin'
        )
        
        # Define tickers and prefixes
        tickers = ['AAPL', 'MSFT', 'TSLA']
        topic_prefixes = ['stock_data_', 'indicators_', 'sentiment_']
        
        # Create topics
        topic_list = []
        for prefix in topic_prefixes:
            for ticker in tickers:
                topic_name = f"{prefix}{ticker.lower()}"
                topic_list.append(NewTopic(
                    name=topic_name,
                    num_partitions=1,
                    replication_factor=1
                ))
        
        # Create the topics
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Created {len(topic_list)} topics")
        except Exception as e:
            logger.warning(f"Some topics may already exist: {str(e)}")
        
        # List all topics
        topics = admin_client.list_topics()
        logger.info(f"Available topics: {topics}")
        
        admin_client.close()
        return topics
    except Exception as e:
        logger.error(f"Failed to create topics: {str(e)}")
        return []

def publish_test_data(bootstrap_servers='kafka:9092'):
    """Publish test data to Kafka topics"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Define tickers
        tickers = ['AAPL', 'MSFT', 'TSLA']
        
        # Publish test data
        for ticker in tickers:
            topic = f"stock_data_{ticker.lower()}"
            
            # Create 5 test messages
            for i in range(5):
                message = {
                    'symbol': ticker,
                    'price': 100 + i * 0.5,
                    'volume': 1000 * (i + 1),
                    'timestamp': datetime.now().isoformat(),
                    'test_message': True,
                    'message_id': i + 1
                }
                
                # Send the message
                producer.send(topic, key=ticker, value=message)
                logger.info(f"Published message {i+1} to {topic}")
                
                # Small delay
                time.sleep(0.1)
        
        # Flush and close
        producer.flush()
        producer.close()
        
        logger.info("Test data published successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to publish test data: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting Kafka test in Docker container")
    
    # Create topics
    topics = create_topics()
    
    if topics:
        # Publish test data
        publish_test_data()
    
    logger.info("Kafka test completed")
