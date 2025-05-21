"""
Kafka Integration Module for Financial Trends Project

This module integrates Kafka with the enhanced pipeline to enable real-time data streaming
for all data sources including stock data, economic indicators, and social sentiment.
"""

import os
import sys
import json
import logging
import threading
import time
from datetime import datetime, timedelta

# Add project root to path to import config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import get_config

# Import our modules
from data_ingestion.kafka_producer import StockDataProducer
from data_ingestion.kafka_consumer import StockDataConsumer
from data_ingestion.db_utils import MongoDBClient
from data_ingestion.fetch_stock_data import fetch_stock_data
from data_ingestion.process_indicators import process_indicators
from data_ingestion.economic_indicators import EconomicIndicatorsCollector
from data_ingestion.social_sentiment import TwitterSentimentAnalyzer, RedditSentimentAnalyzer
from data_ingestion.news_api import FinancialNewsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_integration')

class KafkaIntegrationManager:
    """
    Manager class for integrating Kafka with the enhanced pipeline.
    Handles the coordination between producers and consumers for all data sources.
    """
    
    def __init__(self):
        """Initialize the Kafka integration manager"""
        self.bootstrap_servers = get_config('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.db_client = MongoDBClient()
        self.running = False
        self.threads = []
        
        # Initialize data collectors
        self.economic_collector = EconomicIndicatorsCollector()
        self.twitter_analyzer = TwitterSentimentAnalyzer()
        self.reddit_analyzer = RedditSentimentAnalyzer()
        self.news_collector = FinancialNewsCollector()
        
        # Kafka producer
        self.producer = None
        try:
            self.producer = StockDataProducer(bootstrap_servers=self.bootstrap_servers)
            logger.info(f"Kafka producer initialized with bootstrap servers: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
        
        # Topic configuration
        self.topics = {
            'stock': 'stock_data_',
            'indicators': 'indicators_',
            'economic': 'economic_indicators_',
            'twitter': 'twitter_sentiment_',
            'reddit': 'reddit_sentiment_',
            'news': 'financial_news_'
        }
    
    def create_topics(self, tickers):
        """Create all necessary Kafka topics"""
        if not self.producer or not self.producer.producer:
            logger.error("Kafka producer not available for creating topics")
            return False
            
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='financial-trends-admin'
            )
            
            # Create topics for each ticker and data type
            topic_list = []
            
            # Stock data and indicators topics (ticker-specific)
            ticker_topics = ['stock', 'indicators', 'twitter', 'reddit', 'news']
            for topic_type in ticker_topics:
                for ticker in tickers:
                    topic_name = f"{self.topics[topic_type]}{ticker.lower()}"
                    topic_list.append(NewTopic(
                        name=topic_name,
                        num_partitions=1,
                        replication_factor=1
                    ))
            
            # Economic indicators topic (general)
            topic_list.append(NewTopic(
                name=self.topics['economic'] + 'general',
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
    
    def publish_stock_data(self, ticker, data):
        """Publish stock data to Kafka"""
        if not self.producer or not self.producer.producer:
            logger.error("Kafka producer not available for publishing stock data")
            return False
            
        try:
            topic = f"{self.topics['stock']}{ticker.lower()}"
            
            # Convert DataFrame to records
            if hasattr(data, 'reset_index') and callable(data.reset_index):
                records = data.reset_index().to_dict('records')
            elif isinstance(data, dict):
                records = [data]
            else:
                records = data
                
            # Publish each record
            for record in records:
                # Convert date to string for JSON serialization
                if 'date' in record and isinstance(record['date'], datetime):
                    record['date'] = record['date'].isoformat()
                
                # Add metadata
                record['ticker'] = ticker
                record['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka
                self.producer.producer.send(topic, key=ticker, value=record)
            
            self.producer.producer.flush()
            logger.info(f"Published {len(records)} stock data records for {ticker} to Kafka")
            return True
        except Exception as e:
            logger.error(f"Error publishing stock data: {str(e)}")
            return False
    
    def publish_indicators(self, ticker, data):
        """Publish technical indicators to Kafka"""
        if not self.producer or not self.producer.producer:
            logger.error("Kafka producer not available for publishing indicators")
            return False
            
        try:
            topic = f"{self.topics['indicators']}{ticker.lower()}"
            
            # Convert DataFrame to records
            if hasattr(data, 'reset_index') and callable(data.reset_index):
                records = data.reset_index().to_dict('records')
            elif isinstance(data, dict):
                records = [data]
            else:
                records = data
                
            # Publish each record
            for record in records:
                # Convert date to string for JSON serialization
                if 'date' in record and isinstance(record['date'], datetime):
                    record['date'] = record['date'].isoformat()
                
                # Add metadata
                record['ticker'] = ticker
                record['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka
                self.producer.producer.send(topic, key=ticker, value=record)
            
            self.producer.producer.flush()
            logger.info(f"Published {len(records)} indicator records for {ticker} to Kafka")
            return True
        except Exception as e:
            logger.error(f"Error publishing indicators: {str(e)}")
            return False
    
    def publish_economic_data(self, data):
        """Publish economic indicators to Kafka"""
        if not self.producer or not self.producer.producer:
            logger.error("Kafka producer not available for publishing economic data")
            return False
            
        try:
            topic = f"{self.topics['economic']}general"
            
            # Convert data to records format
            if hasattr(data, 'reset_index') and callable(data.reset_index):
                records = data.reset_index().to_dict('records')
            elif isinstance(data, dict):
                records = [data]
            else:
                records = data
                
            # Publish each record
            for record in records:
                # Convert date to string for JSON serialization
                if 'date' in record and isinstance(record['date'], datetime):
                    record['date'] = record['date'].isoformat()
                
                # Add metadata
                record['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka
                self.producer.producer.send(topic, key=record.get('indicator', 'economic'), value=record)
            
            self.producer.producer.flush()
            logger.info(f"Published {len(records)} economic indicator records to Kafka")
            return True
        except Exception as e:
            logger.error(f"Error publishing economic data: {str(e)}")
            return False
    
    def publish_sentiment_data(self, ticker, data, source):
        """Publish sentiment data to Kafka"""
        if not self.producer or not self.producer.producer:
            logger.error(f"Kafka producer not available for publishing {source} sentiment")
            return False
            
        try:
            topic = f"{self.topics[source]}{ticker.lower()}"
            
            # Convert data to records format
            if hasattr(data, 'reset_index') and callable(data.reset_index):
                records = data.reset_index().to_dict('records')
            elif isinstance(data, dict):
                records = [data]
            else:
                records = data
                
            # Publish each record
            for record in records:
                # Convert date to string for JSON serialization
                if 'created_utc' in record and isinstance(record['created_utc'], (int, float)):
                    record['created_at'] = datetime.fromtimestamp(record['created_utc']).isoformat()
                
                # Add metadata
                record['ticker'] = ticker
                record['source'] = source
                record['processed_at'] = datetime.now().isoformat()
                
                # Send to Kafka
                self.producer.producer.send(topic, key=ticker, value=record)
            
            self.producer.producer.flush()
            logger.info(f"Published {len(records)} {source} sentiment records for {ticker} to Kafka")
            return True
        except Exception as e:
            logger.error(f"Error publishing {source} sentiment: {str(e)}")
            return False
    
    def publish_news_data(self, ticker, data):
        """Publish news data to Kafka"""
        if not self.producer or not self.producer.producer:
            logger.error("Kafka producer not available for publishing news")
            return False
            
        try:
            topic = f"{self.topics['news']}{ticker.lower()}"
            
            # Convert data to records format
            if hasattr(data, 'reset_index') and callable(data.reset_index):
                records = data.reset_index().to_dict('records')
            elif isinstance(data, dict):
                records = [data]
            else:
                records = data
                
            # Publish each record
            for record in records:
                # Convert date to string for JSON serialization
                if 'publishedAt' in record and not isinstance(record['publishedAt'], str):
                    record['publishedAt'] = record['publishedAt'].isoformat()
                
                # Add metadata
                record['ticker'] = ticker
                record['processed_at'] = datetime.now().isoformat()
                
                # Send to Kafka
                self.producer.producer.send(topic, key=ticker, value=record)
            
            self.producer.producer.flush()
            logger.info(f"Published {len(records)} news records for {ticker} to Kafka")
            return True
        except Exception as e:
            logger.error(f"Error publishing news: {str(e)}")
            return False
    
    def fetch_and_publish_stock_data(self, ticker, interval_seconds=3600):
        """Continuously fetch and publish stock data"""
        logger.info(f"Starting stock data stream for {ticker}")
        
        while self.running:
            try:
                # Fetch stock data
                stock_data = fetch_stock_data(ticker)
                
                if stock_data is not None:
                    # Process indicators
                    indicators = process_indicators(ticker)
                    
                    # Publish to Kafka
                    self.publish_stock_data(ticker, stock_data)
                    
                    if indicators is not None:
                        self.publish_indicators(ticker, indicators)
                
                # Wait for next interval
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Error in stock data stream for {ticker}: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying
    
    def fetch_and_publish_economic_data(self, interval_seconds=86400):
        """Continuously fetch and publish economic data"""
        logger.info("Starting economic data stream")
        
        while self.running:
            try:
                # Fetch economic indicators
                indicators = self.economic_collector.fetch_all_indicators()
                
                if indicators:
                    # Convert to records format
                    records = []
                    for indicator_id, series in indicators.items():
                        for date, value in series.items():
                            records.append({
                                'indicator': indicator_id,
                                'date': date,
                                'value': value
                            })
                    
                    # Publish to Kafka
                    self.publish_economic_data(records)
                
                # Wait for next interval (daily)
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Error in economic data stream: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying
    
    def fetch_and_publish_social_sentiment(self, ticker, interval_seconds=3600):
        """Continuously fetch and publish social sentiment data"""
        logger.info(f"Starting social sentiment stream for {ticker}")
        
        while self.running:
            try:
                # Fetch Twitter sentiment
                try:
                    twitter_data = self.twitter_analyzer.fetch_twitter_sentiment(ticker)
                    if twitter_data:
                        self.publish_sentiment_data(ticker, twitter_data, 'twitter')
                except Exception as e:
                    logger.error(f"Error fetching Twitter sentiment: {str(e)}")
                
                # Fetch Reddit sentiment
                try:
                    reddit_data = self.reddit_analyzer.fetch_reddit_sentiment(ticker)
                    if reddit_data:
                        self.publish_sentiment_data(ticker, reddit_data, 'reddit')
                except Exception as e:
                    logger.error(f"Error fetching Reddit sentiment: {str(e)}")
                
                # Wait for next interval
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Error in social sentiment stream for {ticker}: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying
    
    def fetch_and_publish_news(self, ticker, interval_seconds=3600):
        """Continuously fetch and publish news data"""
        logger.info(f"Starting news stream for {ticker}")
        
        while self.running:
            try:
                # Fetch news
                news_data = self.news_collector.fetch_company_news(ticker)
                
                if news_data:
                    # Publish to Kafka
                    self.publish_news_data(ticker, news_data)
                
                # Wait for next interval
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Error in news stream for {ticker}: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying
    
    def start_streaming_pipeline(self, tickers, interval_seconds=60):
        """Start the complete streaming pipeline for all data sources"""
        if self.running:
            logger.warning("Streaming pipeline is already running")
            return False
        
        logger.info(f"Starting streaming pipeline for tickers: {tickers}")
        
        # Create Kafka topics
        self.create_topics(tickers)
        
        # Set running flag
        self.running = True
        
        # Start stock data streams for each ticker
        for ticker in tickers:
            # Stock data and indicators thread
            stock_thread = threading.Thread(
                target=self.fetch_and_publish_stock_data,
                args=(ticker, interval_seconds),
                daemon=True
            )
            stock_thread.start()
            self.threads.append(stock_thread)
            
            # Social sentiment thread
            sentiment_thread = threading.Thread(
                target=self.fetch_and_publish_social_sentiment,
                args=(ticker, interval_seconds * 10),  # Less frequent updates
                daemon=True
            )
            sentiment_thread.start()
            self.threads.append(sentiment_thread)
            
            # News thread
            news_thread = threading.Thread(
                target=self.fetch_and_publish_news,
                args=(ticker, interval_seconds * 20),  # Less frequent updates
                daemon=True
            )
            news_thread.start()
            self.threads.append(news_thread)
        
        # Economic data thread (once for all tickers)
        economic_thread = threading.Thread(
            target=self.fetch_and_publish_economic_data,
            args=(interval_seconds * 240,),  # Much less frequent updates
            daemon=True
        )
        economic_thread.start()
        self.threads.append(economic_thread)
        
        # Start consumers
        consumer = StockDataConsumer(bootstrap_servers=self.bootstrap_servers)
        consumer_thread = threading.Thread(
            target=consumer.consume_multiple_tickers,
            args=(tickers,),
            daemon=True
        )
        consumer_thread.start()
        self.threads.append(consumer_thread)
        
        logger.info("Streaming pipeline started successfully")
        return True
    
    def stop_streaming_pipeline(self):
        """Stop the streaming pipeline"""
        logger.info("Stopping streaming pipeline")
        self.running = False
        
        # Close producer if it exists
        if self.producer and hasattr(self.producer, 'producer') and self.producer.producer:
            self.producer.producer.close()
            logger.info("Kafka producer closed")
        
        # Threads will stop when running flag is set to False
        logger.info("Streaming pipeline stopped")
        return True


def run_kafka_pipeline(tickers, duration_minutes=None):
    """
    Run the Kafka streaming pipeline for the specified tickers
    
    Args:
        tickers (list): List of stock tickers to stream
        duration_minutes (int, optional): How long to run the pipeline in minutes
    """
    manager = KafkaIntegrationManager()
    
    try:
        # Start the streaming pipeline
        if manager.start_streaming_pipeline(tickers):
            logger.info(f"Kafka pipeline running for tickers: {tickers}")
            
            if duration_minutes:
                # Run for specified duration
                logger.info(f"Pipeline will run for {duration_minutes} minutes")
                time.sleep(duration_minutes * 60)
                manager.stop_streaming_pipeline()
                logger.info("Pipeline stopped after scheduled duration")
            else:
                # Run indefinitely until interrupted
                logger.info("Pipeline running indefinitely. Press Ctrl+C to stop.")
                while True:
                    time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        manager.stop_streaming_pipeline()
    except Exception as e:
        logger.error(f"Error in Kafka pipeline: {str(e)}")
        manager.stop_streaming_pipeline()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Streaming Pipeline for Financial Data")
    parser.add_argument("tickers", nargs="+", help="Stock ticker symbols (e.g., AAPL TSLA MSFT)")
    parser.add_argument("--duration", type=int, help="Duration to run in minutes")
    parser.add_argument("--interval", type=int, default=60, help="Data update interval in seconds")
    
    args = parser.parse_args()
    
    run_kafka_pipeline(args.tickers, args.duration)
