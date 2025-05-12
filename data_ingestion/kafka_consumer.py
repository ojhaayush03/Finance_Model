import json
import logging
import threading
from kafka import KafkaConsumer
from db_utils import MongoDBClient
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_consumer')

class StockDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic_prefix='stock_data_'):
        """Initialize Kafka consumer for stock data streaming"""
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.db_client = MongoDBClient()
        self.consumers = {}
        self.running = False
    
    def create_consumer(self, ticker):
        """Create a Kafka consumer for a specific ticker"""
        topic = f"{self.topic_prefix}{ticker.lower()}"
        
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=f'stock_group_{ticker.lower()}',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"✅ Created consumer for {topic}")
            return consumer
        except Exception as e:
            logger.error(f"❌ Failed to create consumer for {topic}: {str(e)}")
            return None
    
    def process_message(self, message, ticker):
        """Process and store a Kafka message in MongoDB"""
        try:
            # Extract data from message
            data = message.value
            
            # Add processing timestamp
            data['processed_at'] = datetime.now().isoformat()
            
            # Store in MongoDB
            collection = self.db_client.db[f'{ticker.lower()}_realtime']
            collection.create_index('timestamp')
            collection.insert_one(data)
            
            # Log success
            logger.info(f"✅ Processed and stored data for {ticker} at {data['timestamp']}")
            
            return data
        except Exception as e:
            logger.error(f"❌ Error processing message: {str(e)}")
            return None
    
    def consume_ticker_data(self, ticker):
        """Consume data for a specific ticker in a separate thread"""
        consumer = self.create_consumer(ticker)
        if not consumer:
            return
        
        logger.info(f"🔄 Starting consumption for {ticker}")
        
        try:
            for message in consumer:
                if not self.running:
                    break
                
                self.process_message(message, ticker)
                
        except Exception as e:
            logger.error(f"❌ Error in consumer thread for {ticker}: {str(e)}")
        finally:
            consumer.close()
            logger.info(f"🔒 Consumer closed for {ticker}")
    
    def start_consuming(self, tickers):
        """Start consuming data for multiple tickers"""
        self.running = True
        
        # Create and start a thread for each ticker
        for ticker in tickers:
            thread = threading.Thread(
                target=self.consume_ticker_data,
                args=(ticker,),
                daemon=True
            )
            self.consumers[ticker] = thread
            thread.start()
            logger.info(f"🧵 Started thread for {ticker}")
        
        logger.info(f"🚀 All consumers started for tickers: {', '.join(tickers)}")
    
    def stop_consuming(self):
        """Stop all consumer threads"""
        self.running = False
        logger.info("⏹️ Stopping all consumers...")
        
        # Wait for threads to finish
        for ticker, thread in self.consumers.items():
            thread.join(timeout=5.0)
            logger.info(f"🛑 Stopped consumer for {ticker}")
        
        self.consumers = {}
    
    def get_latest_data(self, ticker, limit=100):
        """Get the latest real-time data for a ticker from MongoDB"""
        collection = self.db_client.db[f'{ticker.lower()}_realtime']
        
        # Get the most recent records
        data = list(collection.find().sort('timestamp', -1).limit(limit))
        
        if data:
            # Convert to DataFrame and sort by timestamp
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            
            return df
        else:
            return pd.DataFrame()

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
