#!/usr/bin/env python
"""
Real-time Financial Data Streaming Pipeline

This script runs the enhanced financial data pipeline with Kafka integration
for real-time data streaming and processing of all financial data sources.
"""

import os
import sys
import time
import signal
import argparse
import logging
from datetime import datetime
from data_ingestion.kafka_integration import KafkaIntegrationManager
from data_ingestion.enhanced_pipeline import EnhancedDataPipeline

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"streaming_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('streaming_pipeline')

# Global variables for signal handling
kafka_manager = None
running = True

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global running
    logger.info("Received shutdown signal, stopping pipeline...")
    running = False
    
    # Stop Kafka manager if it exists
    if kafka_manager:
        kafka_manager.stop_streaming_pipeline()
    
    logger.info("Pipeline shutdown complete")
    sys.exit(0)

def main():
    """Main entry point for the streaming pipeline"""
    global kafka_manager
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Real-time Financial Data Streaming Pipeline")
    parser.add_argument("tickers", nargs="+", help="Stock ticker symbols (e.g., AAPL TSLA MSFT)")
    parser.add_argument("--initial-load", action="store_true", 
                        help="Run the enhanced pipeline first to load historical data")
    parser.add_argument("--duration", type=int, 
                        help="Duration to run the streaming pipeline in minutes")
    parser.add_argument("--interval", type=int, default=60, 
                        help="Data update interval in seconds")
    parser.add_argument("--skip-news", action="store_true", 
                        help="Skip news data collection")
    parser.add_argument("--skip-economic", action="store_true", 
                        help="Skip economic indicators")
    parser.add_argument("--skip-twitter", action="store_true", 
                        help="Skip Twitter sentiment analysis")
    parser.add_argument("--skip-reddit", action="store_true", 
                        help="Skip Reddit sentiment analysis")
    
    args = parser.parse_args()
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info(f"Starting real-time financial data pipeline for: {', '.join(args.tickers)}")
    logger.info(f"Update interval: {args.interval} seconds")
    
    # Step 1: Initial data load if requested
    if args.initial_load:
        logger.info("Running initial historical data load...")
        pipeline = EnhancedDataPipeline()
        
        for ticker in args.tickers:
            logger.info(f"Loading historical data for {ticker}")
            pipeline.run_pipeline(
                ticker,
                include_news=not args.skip_news,
                include_economic=not args.skip_economic,
                include_twitter=not args.skip_twitter,
                include_reddit=not args.skip_reddit
            )
    
    # Step 2: Start the Kafka streaming pipeline
    logger.info("Initializing Kafka integration manager...")
    kafka_manager = KafkaIntegrationManager()
    
    logger.info("Starting Kafka streaming pipeline...")
    kafka_manager.start_streaming_pipeline(
        args.tickers,
        interval_seconds=args.interval
    )
    
    # Run for specified duration or indefinitely
    if args.duration:
        logger.info(f"Pipeline will run for {args.duration} minutes")
        end_time = time.time() + (args.duration * 60)
        
        while time.time() < end_time and running:
            time.sleep(1)
            
        # Stop the pipeline if we're still running
        if running:
            logger.info("Duration completed, stopping pipeline...")
            kafka_manager.stop_streaming_pipeline()
    else:
        logger.info("Pipeline running indefinitely. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        while running:
            time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        if kafka_manager:
            kafka_manager.stop_streaming_pipeline()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in streaming pipeline: {str(e)}")
        if kafka_manager:
            kafka_manager.stop_streaming_pipeline()
        sys.exit(1)
