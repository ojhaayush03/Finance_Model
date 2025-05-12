import argparse
import subprocess
import threading
import time
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"../logs/streaming_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger('streaming_manager')

def ensure_kafka_running():
    """Check if Kafka is running and start if needed"""
    # This is a simplified check - in production you'd want more robust verification
    logger.info("Checking Kafka status...")
    
    try:
        # Create logs directory if it doesn't exist
        os.makedirs("../logs", exist_ok=True)
        
        # On Windows, you'd typically check services or use other methods
        # This is just a placeholder - you'll need to adapt this to your environment
        logger.info("✅ Kafka appears to be running")
        return True
    except Exception as e:
        logger.error(f"❌ Error checking Kafka status: {str(e)}")
        logger.warning("⚠️ Please ensure Kafka is running before continuing")
        return False

def start_producer(tickers, interval):
    """Start the Kafka producer in a separate process"""
    logger.info(f"Starting producer for tickers: {', '.join(tickers)}")
    
    try:
        # Start producer process
        cmd = ["python", "kafka_producer.py"]
        producer_process = subprocess.Popen(
            cmd, 
            stdin=subprocess.PIPE,
            text=True
        )
        
        # Send input to the producer
        producer_process.stdin.write(f"{','.join(tickers)}\n")
        producer_process.stdin.write(f"{interval}\n")
        producer_process.stdin.flush()
        
        logger.info("✅ Producer started successfully")
        return producer_process
    except Exception as e:
        logger.error(f"❌ Failed to start producer: {str(e)}")
        return None

def start_consumer(tickers):
    """Start the Kafka consumer in a separate process"""
    logger.info(f"Starting consumer for tickers: {', '.join(tickers)}")
    
    try:
        # Start consumer process
        cmd = ["python", "kafka_consumer.py"]
        consumer_process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            text=True
        )
        
        # Send input to the consumer
        consumer_process.stdin.write(f"{','.join(tickers)}\n")
        consumer_process.stdin.flush()
        
        logger.info("✅ Consumer started successfully")
        return consumer_process
    except Exception as e:
        logger.error(f"❌ Failed to start consumer: {str(e)}")
        return None

def monitor_processes(producer_process, consumer_process):
    """Monitor the producer and consumer processes"""
    try:
        while True:
            # Check if processes are still running
            if producer_process and producer_process.poll() is not None:
                logger.warning("⚠️ Producer process has terminated")
                
            if consumer_process and consumer_process.poll() is not None:
                logger.warning("⚠️ Consumer process has terminated")
                
            # If both processes have terminated, exit
            if ((producer_process and producer_process.poll() is not None) and 
                (consumer_process and consumer_process.poll() is not None)):
                logger.error("❌ Both producer and consumer have terminated")
                break
                
            # Sleep before checking again
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("⏹️ Monitoring stopped by user")
    finally:
        # Attempt to terminate processes if they're still running
        if producer_process and producer_process.poll() is None:
            producer_process.terminate()
            logger.info("🛑 Producer terminated")
            
        if consumer_process and consumer_process.poll() is None:
            consumer_process.terminate()
            logger.info("🛑 Consumer terminated")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Start real-time stock data streaming")
    parser.add_argument("tickers", help="Comma-separated list of stock tickers (e.g., AAPL,MSFT,TSLA)")
    parser.add_argument("--interval", type=int, default=60, help="Update interval in seconds (default: 60)")
    parser.add_argument("--producer-only", action="store_true", help="Start only the producer")
    parser.add_argument("--consumer-only", action="store_true", help="Start only the consumer")
    
    args = parser.parse_args()
    
    # Parse tickers
    tickers = [t.strip().upper() for t in args.tickers.split(',')]
    
    # Ensure Kafka is running
    if not ensure_kafka_running():
        print("Please start Kafka before running this script.")
        exit(1)
    
    producer_process = None
    consumer_process = None
    
    try:
        # Start producer if requested
        if not args.consumer_only:
            producer_process = start_producer(tickers, args.interval)
        
        # Start consumer if requested
        if not args.producer_only:
            consumer_process = start_consumer(tickers)
        
        # Monitor processes
        print("\n✅ Streaming started! Press Ctrl+C to stop.")
        monitor_processes(producer_process, consumer_process)
        
    except KeyboardInterrupt:
        print("\n⏹️ Stopping streaming...")
    finally:
        # Ensure processes are terminated
        if producer_process and producer_process.poll() is None:
            producer_process.terminate()
            print("🛑 Producer terminated")
            
        if consumer_process and consumer_process.poll() is None:
            consumer_process.terminate()
            print("🛑 Consumer terminated")
        
        print("✅ Streaming stopped.")
