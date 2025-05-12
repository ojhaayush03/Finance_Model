# Financial Trends Analysis Project

A comprehensive data pipeline for fetching, processing, and analyzing financial market data from multiple sources including stock prices, economic indicators, news, and social media sentiment.

## Project Overview

This project provides an enhanced data pipeline that integrates multiple financial data sources to create a rich dataset for analysis and visualization. The system is designed to be resilient against API rate limits and service disruptions through intelligent caching and fallback mechanisms.

### Key Features

1. **Multi-Source Data Integration**
   - Stock price data from Alpha Vantage API
   - Economic indicators from FRED API
   - Social media sentiment from Twitter and Reddit APIs
   - Financial news from NewsAPI (integration ready)

2. **Advanced Data Processing**
   - Technical indicators calculation (SMA, EMA, RSI, volatility, etc.)
   - Sentiment analysis of social media posts
   - Data enrichment and correlation analysis

3. **Resilient Data Collection**
   - Intelligent caching to avoid API rate limits
   - Retry mechanisms with exponential backoff
   - Mock data generation for testing and demos

4. **Efficient Data Storage**
   - MongoDB integration with optimized upsert operations
   - CSV storage for raw and processed data
   - Data versioning and validation

5. **Real-Time Capabilities**
   - Kafka producer/consumer implementation for real-time data streaming
   - Event-driven architecture ready for deployment

## Architecture

The project follows a modular architecture with the following components:

### Data Ingestion Layer
- **Stock Data Collector**: Fetches historical and intraday stock data
- **Economic Indicators Collector**: Retrieves macroeconomic data from FRED
- **Social Sentiment Analyzer**: Processes Twitter and Reddit data for sentiment
- **News Collector**: Gathers relevant financial news articles

### Processing Layer
- **Technical Indicator Processor**: Calculates financial metrics and indicators
- **Sentiment Analysis Engine**: Determines sentiment polarity and subjectivity
- **Data Enrichment Service**: Combines data from multiple sources

### Storage Layer
- **MongoDB Client**: Handles database operations with optimized queries
- **File System Manager**: Manages CSV storage and caching

### Streaming Layer (Ready for Implementation)
- **Kafka Producer**: Publishes real-time financial data to topics
- **Kafka Consumer**: Subscribes to data streams for processing

## Setup and Installation

### Prerequisites
- Python 3.8+
- MongoDB server
- Kafka (optional, for real-time streaming)

### Installation Steps

1. Clone the repository

2. Create and activate a virtual environment:
```bash
python -m venv venv

# On Windows
.\venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure API keys in config.py:
```python
# Example configuration
ALPHA_VANTAGE_API_KEY = "your_key_here"
FRED_API_KEY = "your_key_here"
TWITTER_BEARER_TOKEN = "your_token_here"
REDDIT_CLIENT_ID = "your_id_here"
REDDIT_CLIENT_SECRET = "your_secret_here"
NEWS_API_KEY = "your_key_here"
```

5. Ensure MongoDB is running on your system

## Usage

### Running the Enhanced Pipeline

The enhanced pipeline can be run with various options to include or exclude specific data sources:

```bash
python data_ingestion/enhanced_pipeline.py AAPL --skip-news
```

Command line options:
- `ticker`: Stock symbol to analyze (required)
- `--skip-news`: Skip news data collection
- `--skip-economic`: Skip economic indicators
- `--skip-twitter`: Skip Twitter sentiment analysis
- `--skip-reddit`: Skip Reddit sentiment analysis

### Real-Time Data Streaming (Kafka)

To use the Kafka streaming capabilities:

1. Start the Kafka producer to stream real-time stock data:
```bash
python data_ingestion/kafka_producer.py
```

2. Start the Kafka consumer to process and store the streaming data:
```bash
python data_ingestion/kafka_consumer.py
```

## Data Structure

### File Storage
- **Raw Data**: Stock price data in CSV format (`raw_data/TICKER_stock_data.csv`)
- **Processed Data**: Technical indicators in CSV format (`processed_data/TICKER_indicators.csv`)
- **Logs**: Pipeline execution logs (`logs/pipeline_TIMESTAMP.log`)

### MongoDB Collections
- **TICKER_stock_data**: Historical stock price data with technical indicators
- **TICKER_reddit**: Reddit posts and sentiment analysis
- **TICKER_twitter**: Twitter posts and sentiment analysis
- **economic_indicators**: Macroeconomic indicators from FRED
- **news_articles**: Financial news articles and metadata

## API Rate Limits and Handling

The system implements several strategies to handle API rate limits:

1. **Caching**: Stock data is cached and reused if less than 24 hours old
2. **Retry Mechanism**: Exponential backoff for Twitter API to handle rate limits
3. **Mock Data**: For FRED API, mock data is generated when API key issues occur
4. **Selective Fetching**: Options to skip certain data sources when needed

## Future Enhancements

1. **Visualization Dashboard**: Interactive dashboard using Flask, Dash, or Streamlit
2. **Machine Learning Models**: Predictive models for price forecasting
3. **Advanced Analytics**: Correlation analysis between different data sources
4. **Alerting System**: Real-time alerts for significant market movements
5. **Full Kafka Integration**: Complete event-driven architecture for real-time processing

## Troubleshooting

### Common Issues

1. **API Rate Limits**: If you encounter rate limit errors, try increasing the cache duration or using the skip options
2. **MongoDB Connection**: Ensure MongoDB is running on the default port (27017)
3. **Missing Data**: Check API keys in config.py and verify they have the necessary permissions

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
