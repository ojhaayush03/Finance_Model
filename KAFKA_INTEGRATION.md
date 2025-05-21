# Kafka Integration for Financial Trends Project

This document explains how to use the Kafka integration in the Financial Trends Project for real-time data streaming.

## Overview

The Kafka integration enables real-time streaming of financial data from various sources:

- Stock price data
- Technical indicators
- Financial news
- Social media sentiment (Twitter/X and Reddit)
- Economic indicators

All data is streamed through Kafka topics and can be consumed by various applications for real-time analytics, visualization, and processing.

## Architecture

The Kafka integration consists of the following components:

1. **Kafka Producers**: Publish data to Kafka topics
2. **Kafka Consumers**: Consume data from Kafka topics and store it in MongoDB
3. **Kafka Topics**: Organized by data type and ticker symbol
4. **MongoDB**: Persistent storage for all consumed data

## Docker Setup

The Kafka integration is designed to run in a Docker environment. The `docker-compose.yml` file sets up:

- Zookeeper (for Kafka coordination)
- Kafka broker
- Kafka UI (for monitoring)
- MongoDB
- MongoDB Express (for database management)
- Financial Trends application containers

### Starting the Docker Environment

To start the Docker environment:

```bash
docker-compose up -d
```

This will start all the necessary services including Kafka, MongoDB, and the application containers.

## Testing the Kafka Integration

A test script is provided to verify that the Kafka integration is working correctly:

```bash
# Run inside the Docker container
docker exec -it financial-trends-app python test_kafka_integration.py AAPL MSFT TSLA
```

This script will:
1. Create Kafka topics for the specified tickers
2. Publish test data to these topics
3. Consume the data and verify it's correctly stored in MongoDB

### Test Options

The test script supports several options:

```
usage: test_kafka_integration.py [-h] [--skip-topics] [--skip-producer] [--skip-consumer] [--consumer-duration CONSUMER_DURATION] [--kafka-host KAFKA_HOST] [--kafka-port KAFKA_PORT] [--force] tickers [tickers ...]

Kafka Integration Test Script

positional arguments:
  tickers               Stock ticker symbols (e.g., AAPL TSLA MSFT)

options:
  -h, --help            show this help message and exit
  --skip-topics         Skip topic creation
  --skip-producer       Skip producer test
  --skip-consumer       Skip consumer test
  --consumer-duration CONSUMER_DURATION
                        Duration to run the consumer test in seconds
  --kafka-host KAFKA_HOST
                        Kafka bootstrap server host
  --kafka-port KAFKA_PORT
                        Kafka bootstrap server port
  --force               Force run even if Kafka is not available
```

## Running the Streaming Pipeline

To run the full streaming pipeline:

```bash
# Run inside the Docker container
docker exec -it financial-trends-app python run_streaming_pipeline.py AAPL MSFT TSLA
```

### Pipeline Options

```
usage: run_streaming_pipeline.py [-h] [--initial-load] [--duration DURATION] [--interval INTERVAL] [--skip-news] [--skip-economic] [--skip-twitter] [--skip-reddit] tickers [tickers ...]

Real-time Financial Data Streaming Pipeline

positional arguments:
  tickers               Stock ticker symbols (e.g., AAPL TSLA MSFT)

options:
  -h, --help            show this help message and exit
  --initial-load        Run the enhanced pipeline first to load historical data
  --duration DURATION   Duration to run the streaming pipeline in minutes
  --interval INTERVAL   Data update interval in seconds
  --skip-news           Skip news data collection
  --skip-economic       Skip economic indicators
  --skip-twitter        Skip Twitter sentiment analysis
  --skip-reddit         Skip Reddit sentiment analysis
```

## Monitoring

### Kafka UI

The Kafka UI is available at http://localhost:8080 when running Docker. It provides a web interface to:

- View Kafka topics
- Monitor message production and consumption
- Inspect message contents
- View broker metrics

### MongoDB Express

MongoDB Express is available at http://localhost:8081 when running Docker. It provides a web interface to:

- View MongoDB collections
- Query and filter data
- Export data
- Manage indexes

## Troubleshooting

### Kafka Connection Issues

If you encounter connection issues with Kafka:

1. Ensure Docker containers are running:
   ```bash
   docker-compose ps
   ```

2. Check Kafka logs:
   ```bash
   docker-compose logs kafka
   ```

3. Verify network connectivity:
   ```bash
   docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

### Consumer Lag

If consumers are falling behind producers:

1. Check consumer lag in Kafka UI
2. Consider increasing consumer parallelism by adjusting the number of partitions
3. Optimize MongoDB write operations

## API Reference

### KafkaIntegrationManager

The `KafkaIntegrationManager` class in `kafka_integration.py` is the main entry point for the Kafka integration. It provides methods to:

- Create Kafka topics
- Start and stop the streaming pipeline
- Manage producers and consumers

### FinancialDataProducer

The `FinancialDataProducer` class in `kafka_producer.py` provides methods to publish different types of financial data:

- `produce_stock_data(ticker, data)`
- `produce_indicator_data(ticker, indicator_name, data)`
- `produce_news_data(ticker, news_data)`
- `produce_twitter_sentiment(ticker, sentiment_data)`
- `produce_reddit_sentiment(ticker, sentiment_data)`
- `produce_economic_data(indicator_name, data)`

### StockDataConsumer

The `StockDataConsumer` class in `kafka_consumer.py` provides methods to consume data from Kafka topics and store it in MongoDB:

- `consume_multiple_tickers(tickers)`
- `stop_consumers()`
