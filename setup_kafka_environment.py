#!/usr/bin/env python
"""
Kafka Environment Setup Script

This script helps set up the Kafka environment for the Financial Trends Project.
It creates the necessary topics and verifies the Kafka connection.
"""

import os
import sys
import time
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_setup')

def wait_for_kafka(bootstrap_servers, max_retries=10, retry_interval=5):
    """Wait for Kafka to become available"""
    logger.info(f"Checking Kafka availability at {bootstrap_servers}")
    
    retries = 0
    while retries < max_retries:
        try:
            # Try to create a producer as a connection test
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            logger.info("✅ Successfully connected to Kafka")
            return True
        except Exception as e:
            retries += 1
            logger.warning(f"Waiting for Kafka to become available... ({retries}/{max_retries})")
            logger.debug(f"Connection error: {str(e)}")
            time.sleep(retry_interval)
    
    logger.error(f"❌ Failed to connect to Kafka after {max_retries} attempts")
    return False

def create_topics(bootstrap_servers, tickers, topic_prefixes):
    """Create Kafka topics for the given tickers and prefixes"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='financial-trends-admin'
        )
        
        existing_topics = []
        try:
            existing_topics = admin_client.list_topics()
            logger.info(f"Existing topics: {existing_topics}")
        except Exception as e:
            logger.warning(f"Could not list existing topics: {str(e)}")
        
        # Create topics for each ticker and prefix
        topic_list = []
        for prefix in topic_prefixes:
            for ticker in tickers:
                topic_name = f"{prefix}{ticker.lower()}"
                
                if topic_name in existing_topics:
                    logger.info(f"Topic {topic_name} already exists")
                    continue
                
                topic_list.append(NewTopic(
                    name=topic_name,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={
                        'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days retention
                        'cleanup.policy': 'delete'
                    }
                ))
        
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"✅ Created {len(topic_list)} topics")
            
            # List the created topics
            for topic in topic_list:
                logger.info(f"  - {topic.name}")
        else:
            logger.info("No new topics to create")
        
        admin_client.close()
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create topics: {str(e)}")
        return False

def verify_topics(bootstrap_servers, tickers, topic_prefixes):
    """Verify that topics exist and are accessible"""
    try:
        # Create a consumer to list topics
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        available_topics = consumer.topics()
        consumer.close()
        
        # Check if all expected topics exist
        expected_topics = []
        for prefix in topic_prefixes:
            for ticker in tickers:
                expected_topics.append(f"{prefix}{ticker.lower()}")
        
        missing_topics = [topic for topic in expected_topics if topic not in available_topics]
        
        if missing_topics:
            logger.warning(f"❌ Missing topics: {missing_topics}")
            return False
        else:
            logger.info(f"✅ All {len(expected_topics)} expected topics are available")
            return True
    except Exception as e:
        logger.error(f"❌ Failed to verify topics: {str(e)}")
        return False

def setup_kafka_environment(bootstrap_servers, tickers):
    """Set up the complete Kafka environment"""
    logger.info("Setting up Kafka environment...")
    
    # Define topic prefixes
    topic_prefixes = [
        'stock_data_',
        'indicators_',
        'sentiment_',
        'news_',
        'economic_'
    ]
    
    # Step 1: Wait for Kafka to be available
    if not wait_for_kafka(bootstrap_servers):
        return False
    
    # Step 2: Create topics
    if not create_topics(bootstrap_servers, tickers, topic_prefixes):
        return False
    
    # Step 3: Verify topics
    if not verify_topics(bootstrap_servers, tickers, topic_prefixes):
        logger.warning("Some topics may be missing, but continuing anyway")
    
    logger.info("✅ Kafka environment setup completed successfully")
    return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Environment Setup for Financial Trends Project")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", 
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("tickers", nargs="+", help="Stock ticker symbols (e.g., AAPL TSLA MSFT)")
    
    args = parser.parse_args()
    
    if setup_kafka_environment(args.bootstrap_servers, args.tickers):
        sys.exit(0)
    else:
        sys.exit(1)
