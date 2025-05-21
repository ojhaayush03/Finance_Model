# Kafka Integration Guide for Financial Trends Project

This guide explains how to set up and run the Financial Trends Project with Kafka integration for real-time data streaming.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for local development)
- API keys configured in `config.py`

## Architecture Overview

The enhanced architecture includes:

1. **Kafka Streaming Layer**
   - Real-time data ingestion from financial APIs
   - Event-driven processing pipeline
   - Topic-based message routing

2. **Containerized Components**
   - Kafka and Zookeeper containers
   - MongoDB for data storage
   - Web interfaces for monitoring (Kafka UI and MongoDB Express)
   - Application containers for data loading and streaming

## Getting Started

### Option 1: Running with Docker Compose (Recommended)

The entire stack can be launched with a single command:

```bash
docker-compose up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka (ports 9092, 29092)
- Kafka UI (port 8080)
- MongoDB (port 27017)
- MongoDB Express (port 8081)
- Financial data loader container
- Financial streaming container

To view logs:
```bash
docker-compose logs -f financial-streaming
```

To stop all services:
```bash
docker-compose down
```

### Option 2: Running Components Individually

#### 1. Start Kafka and MongoDB

If you have Kafka and MongoDB running locally:

```bash
# Start Kafka (if not using Docker)
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# Start MongoDB (if not using Docker)
mongod --dbpath /data/db
```

#### 2. Set Up Kafka Topics

```bash
python setup_kafka_environment.py AAPL MSFT TSLA
```

#### 3. Run the Streaming Pipeline

```bash
# Load historical data first
python run_streaming_pipeline.py AAPL MSFT TSLA --initial-load

# Then start the streaming pipeline
python run_streaming_pipeline.py AAPL MSFT TSLA
```

## Monitoring and Management

### Kafka UI

Access the Kafka UI at http://localhost:8080 to:
- Monitor topics and messages
- View consumer groups
- Check broker status

### MongoDB Express

Access MongoDB Express at http://localhost:8081 to:
- Browse collections
- Query documents
- Monitor database performance

## Data Flow

1. **Data Ingestion**
   - Stock data is fetched from Alpha Vantage
   - Published to Kafka topics (`stock_data_<ticker>`)

2. **Processing**
   - Technical indicators are calculated
   - Results published to indicator topics (`indicators_<ticker>`)

3. **Sentiment Analysis**
   - Social media data is analyzed
   - Results published to sentiment topics (`sentiment_<ticker>`)

4. **Storage**
   - Kafka consumers store processed data in MongoDB
   - Data is available for querying and visualization

## Kafka Topics

| Topic Prefix | Description | Example |
|--------------|-------------|---------|
| stock_data_  | Raw stock price data | stock_data_aapl |
| indicators_  | Technical indicators | indicators_aapl |
| sentiment_   | Social media sentiment | sentiment_aapl |
| news_        | Financial news articles | news_aapl |
| economic_    | Economic indicators | economic_general |

## Troubleshooting

### Common Issues

1. **Kafka Connection Refused**
   - Ensure Kafka is running and ports are accessible
   - Check network settings if using Docker

2. **Missing Topics**
   - Run `setup_kafka_environment.py` to create required topics
   - Check topic names in Kafka UI

3. **MongoDB Connection Issues**
   - Verify MongoDB is running on port 27017
   - Check connection string in config.py

### Logs

- Application logs are stored in the `logs` directory
- Container logs can be viewed with `docker-compose logs`

## Next Steps

After setting up the Kafka integration, consider:

1. **Creating a Dashboard**
   - Use Flask/Dash to visualize streaming data
   - Connect to Kafka for real-time updates

2. **Scaling the System**
   - Add more Kafka brokers for higher throughput
   - Implement Kafka Connect for additional data sources

3. **Adding Machine Learning**
   - Process streaming data for predictions
   - Implement online learning models
