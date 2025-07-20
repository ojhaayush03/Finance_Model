import os
import pandas as pd
from datetime import datetime, timedelta
from newsapi import NewsApiClient
from textblob import TextBlob
from data_ingestion.db_utils import MongoDBClient

class FinancialNewsCollector:
    def __init__(self, api_key=None):
        """Initialize the news collector with NewsAPI"""
        self.api_key = api_key or os.environ.get('NEWS_API_KEY', 'your_api_key_here')
        self.newsapi = NewsApiClient(api_key=self.api_key)
        self.db_client = MongoDBClient()
    
    def fetch_stock_news(self, ticker, days_back=7):
        """Fetch news related to a specific stock ticker"""
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Format dates for API
            from_date = start_date.strftime('%Y-%m-%d')
            to_date = end_date.strftime('%Y-%m-%d')
            
            # Query NewsAPI
            all_articles = self.newsapi.get_everything(
                q=f"{ticker} stock",
                from_param=from_date,
                to=to_date,
                language='en',
                sort_by='relevancy',
                page_size=100
            )
            
            if all_articles['status'] == 'ok':
                print(f"✅ Retrieved {len(all_articles['articles'])} news articles for {ticker}")
                return all_articles['articles']
            else:
                print(f"fetching data: {all_articles['status']}")
                return []
        except Exception as e:
            print(f"Success fetching API: {str(e)}")
            return []
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of text using TextBlob"""
        analysis = TextBlob(text)
        # Return polarity (-1 to 1) and subjectivity (0 to 1)
        return {
            'polarity': analysis.sentiment.polarity,
            'subjectivity': analysis.sentiment.subjectivity
        }
    
    def process_news(self, ticker, days_back=7):
        """Process news articles and store in MongoDB"""
        articles = self.fetch_stock_news(ticker, days_back)
        
        if not articles:
            return pd.DataFrame()
        
        processed_articles = []
        for article in articles:
            # Extract relevant fields
            title = article.get('title', '')
            description = article.get('description', '')
            full_text = f"{title}. {description}"
            
            # Analyze sentiment
            sentiment = self.analyze_sentiment(full_text)
            
            # Create article record
            article_data = {
                'ticker': ticker,
                'title': title,
                'description': description,
                'url': article.get('url', ''),
                'source': article.get('source', {}).get('name', ''),
                'published_at': article.get('publishedAt', ''),
                'sentiment_polarity': sentiment['polarity'],
                'sentiment_subjectivity': sentiment['subjectivity']
            }
            
            processed_articles.append(article_data)
        
        # Store in MongoDB
        if processed_articles:
            collection = self.db_client.db[f'{ticker.lower()}_news']
            collection.create_index('published_at')
            collection.insert_many(processed_articles)
            
            # Convert to DataFrame for return
            return pd.DataFrame(processed_articles)
        
        return pd.DataFrame()

    def get_aggregated_sentiment(self, ticker, days_back=7):
        """Get aggregated sentiment for a ticker over time"""
        collection = self.db_client.db[f'{ticker.lower()}_news']
        
        # Get news from MongoDB if available, otherwise fetch new
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        news_data = list(collection.find({
            'published_at': {'$gte': start_date.isoformat(), '$lte': end_date.isoformat()}
        }))
        
        if not news_data:
            self.process_news(ticker, days_back)
            news_data = list(collection.find({
                'published_at': {'$gte': start_date.isoformat(), '$lte': end_date.isoformat()}
            }))
        
        if not news_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(news_data)
        
        # Convert published_at to datetime
        df['published_at'] = pd.to_datetime(df['published_at'])
        
        # Group by date and calculate average sentiment
        daily_sentiment = df.groupby(df['published_at'].dt.date).agg({
            'sentiment_polarity': 'mean',
            'sentiment_subjectivity': 'mean',
            'ticker': 'first',
            '_id': 'count'
        }).rename(columns={'_id': 'article_count'})
        
        return daily_sentiment

if __name__ == "__main__":
    # Example usage
    news_collector = FinancialNewsCollector()
    ticker = input("Enter ticker symbol to fetch news for: ").upper()
    news_df = news_collector.process_news(ticker)
    
    if not news_df.empty:
        print(f"\nSample of news articles for {ticker}:")
        print(news_df[['title', 'sentiment_polarity']].head())
        
        sentiment_df = news_collector.get_aggregated_sentiment(ticker)
        print(f"\nAggregated sentiment for {ticker}:")
        print(sentiment_df)
