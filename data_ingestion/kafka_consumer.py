import json
import logging
import threading
import os
import sys
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

# Add project root to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import MongoDB client
try:
    from data_ingestion.db_utils import MongoDBClient
except ImportError:
    try:
        from db_utils import MongoDBClient
    except ImportError:
        # Last resort - import directly using absolute path
        import importlib.util
        
        module_path = os.path.join(os.path.dirname(__file__), 'db_utils.py')
        if not os.path.exists(module_path):
            module_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_ingestion', 'db_utils.py')
        
        spec = importlib.util.spec_from_file_location("db_utils", module_path)
        db_utils = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(db_utils)
        MongoDBClient = db_utils.MongoDBClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_consumer')

class StockDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka consumer for financial data streaming"""
        self.bootstrap_servers = bootstrap_servers
        self.db_client = MongoDBClient()
        self.consumers = {}
        self.consumer_threads = {}
        self.running = False
        
        # Define topic prefixes and their corresponding MongoDB collections
        self.topic_mapping = {
            'stock_data_': '{ticker}_stock_data',
            'indicators_': '{ticker}_indicators',
            'twitter_sentiment_': '{ticker}_twitter',
            'reddit_sentiment_': '{ticker}_reddit',
            'financial_news_': '{ticker}_news',
            'economic_indicators_': 'economic_indicators'
        }
    
    def create_consumer(self, topics, group_id):
        """Create a Kafka consumer for specific topics"""
        try:
            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True,
            }
            
            consumer = Consumer(config)
            consumer.subscribe(topics)
            
            logger.info(f"Created consumer for topics: {topics}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create consumer for topics {topics}: {str(e)}")
            return None
    
    def get_collection_name(self, topic, ticker=None):
        """Determine the MongoDB collection name based on the topic"""
        for prefix, collection_template in self.topic_mapping.items():
            if topic.startswith(prefix):
                if '{ticker}' in collection_template:
                    if ticker:
                        return collection_template.format(ticker=ticker.lower())
                    else:
                        # Extract ticker from topic if not provided
                        ticker = topic[len(prefix):]
                        return collection_template.format(ticker=ticker)
                else:
                    return collection_template
        
        # Default collection name if no mapping found
        return topic.replace('-', '_')
    
    def process_message(self, msg, topic):
        """Process and store a Kafka message in MongoDB"""
        try:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    logger.debug(f"Reached end of partition for {topic}")
                else:
                    logger.error(f"Error in consumer: {msg.error()}")
                return
            
            # Extract data from message
            try:
                data = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                logger.error(f"Error parsing message: {str(e)}")
                return
            
            # Get ticker from message or key
            ticker = data.get('ticker', None)
            if not ticker and msg.key():
                ticker = msg.key().decode('utf-8')
            
            # Add processing timestamp if not present
            if 'processed_at' not in data:
                data['processed_at'] = datetime.now().isoformat()
            
            # Determine collection name
            collection_name = self.get_collection_name(topic, ticker)
            
            # Store in MongoDB
            if 'date' in data and isinstance(data['date'], str):
                # Use date as part of the document ID for time series data
                result = self.db_client.db[collection_name].update_one(
                    {'date': data['date'], 'ticker': ticker} if ticker else {'date': data['date']},
                    {'$set': data},
                    upsert=True
                )
            else:
                # For other data types, just insert
                result = self.db_client.db[collection_name].insert_one(data)
            
            if result.acknowledged:
                logger.info(f"Stored message from {topic} in MongoDB collection {collection_name}")
            else:
                logger.warning(f"Failed to store message from {topic} in MongoDB")
                
        except Exception as e:
            logger.error(f"Error processing message from {topic}: {str(e)}")
    
    def consume_topic_data(self, topics, group_id):
        """Consume data for specific topics"""
        consumer = self.create_consumer(topics, group_id)
        
        if not consumer:
            logger.error(f"Failed to create consumer for topics: {topics}")
            return
        
        topic_key = ",".join(topics)
        self.consumers[topic_key] = consumer
        
        logger.info(f"Starting consumer for topics: {topics}")
        
        try:
            while self.running:
                msg = consumer.poll(1.0)  # Timeout in seconds
                
                if msg is None:
                    continue
                
                topic = msg.topic()
                logger.info(f"Received message from {topic}")
                self.process_message(msg, topic)
                
        except Exception as e:
            logger.error(f"Error consuming data from topics {topics}: {str(e)}")
        finally:
            try:
                consumer.close()
                logger.info(f"Consumer for topics {topics} closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {str(e)}")
    
    def discover_topics(self):
        """Discover available topics from Kafka"""
        try:
            # Create a temporary admin client to list topics
            config = {'bootstrap.servers': self.bootstrap_servers}
            consumer = Consumer(config)
            metadata = consumer.list_topics(timeout=10)
            topics = [topic for topic in metadata.topics.keys() if not topic.startswith('__')]
            consumer.close()
            
            logger.info(f"Discovered {len(topics)} topics: {topics}")
            return topics
        except Exception as e:
            logger.error(f"Failed to discover topics: {str(e)}")
            return []
    
    def consume_multiple_tickers(self, tickers):
        """Consume data for multiple tickers across all data types"""
        self.running = True
        
        # Discover available topics
        all_topics = self.discover_topics()
        
        # Filter topics related to our tickers
        ticker_topics = []
        for topic in all_topics:
            # Check if topic is related to any of our tickers
            for ticker in tickers:
                ticker_lower = ticker.lower()
                if ticker_lower in topic:
                    ticker_topics.append(topic)
                    break
        
        # Add economic indicators topic if available
        economic_topic = 'economic_indicators_general'
        if economic_topic in all_topics:
            ticker_topics.append(economic_topic)
        
        if not ticker_topics:
            logger.warning(f"No topics found for tickers: {tickers}")
            return []
            
        logger.info(f"Starting consumers for {len(ticker_topics)} topics: {ticker_topics}")
        
        # Group topics by data type
        topic_groups = {}
        for topic in ticker_topics:
            # Determine the data type from the topic prefix
            data_type = None
            for prefix in self.topic_mapping.keys():
                if topic.startswith(prefix):
                    data_type = prefix
                    break
            
            if data_type not in topic_groups:
                topic_groups[data_type] = []
            
            topic_groups[data_type].append(topic)
        
        # Create threads for each topic group
        threads = []
        for data_type, topics in topic_groups.items():
            # Create a unique group ID for each data type
            group_id = f'financial_group_{data_type}_{datetime.now().strftime("%Y%m%d%H%M%S")}'            
            thread = threading.Thread(
                target=self.consume_topic_data, 
                args=(topics, group_id),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            
            # Store thread reference
            self.consumer_threads[data_type] = thread
        
        logger.info(f"Started {len(threads)} consumer threads for {len(ticker_topics)} topics")
        
        # Return the threads so the caller can join them if needed
        return threads
    
    def stop_consumers(self):
        """Stop all consumers"""
        self.running = False
        logger.info("Stopping all consumers...")
        
        # Close all consumers
        for topic_key, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.info(f"Consumer for {topic_key} closed")
            except Exception as e:
                logger.error(f"Error closing consumer for {topic_key}: {str(e)}")
        
        # Wait for threads to finish
        for data_type, thread in self.consumer_threads.items():
            try:
                thread.join(timeout=5.0)
                logger.info(f"Thread for {data_type} joined")
            except Exception as e:
                logger.error(f"Error joining thread for {data_type}: {str(e)}")
        
        self.consumers = {}
        self.consumer_threads = {}
        logger.info("All consumers stopped")

if __name__ == "__main__":
    # Example usage
    consumer = StockDataConsumer()
    
    # Ask for tickers
    ticker_input = input("Enter stock tickers to consume (comma-separated, e.g., AAPL,MSFT,TSLA): ")
    tickers = [t.strip().upper() for t in ticker_input.split(',')]
    
    try:
        # Start consuming
        consumer.start_consuming(tickers)
        
        # Keep running until user interrupts
        print("Press Ctrl+C to stop consuming...")
        while True:
            pass
            
    except KeyboardInterrupt:
        print("Stopping consumers...")
        consumer.stop_consuming()
        print("Consumers stopped.")
