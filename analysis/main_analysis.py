#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Financial Trends Analysis Main Script

This script serves as the main entry point for running various financial data analyses:
- Correlation analysis between stock prices, sentiment, and economic indicators
- Predictive modeling for stock price movements
- Anomaly detection for unusual patterns in price, volume, and sentiment

Usage:
    python main_analysis.py --ticker AAPL --analysis correlation prediction anomaly
"""

import os
import sys
import logging
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import analysis modules
from analysis.correlation_analysis import CorrelationAnalyzer
from analysis.predictive_models import StockPricePredictor
from analysis.anomaly_detection import AnomalyDetector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'analysis.log'))
    ]
)
logger = logging.getLogger(__name__)


class FinancialAnalysisPipeline:
    """Main class for running financial data analysis pipeline"""
    
    def __init__(self, output_dir=None):
        """Initialize the analysis pipeline"""
        self.correlation_analyzer = CorrelationAnalyzer()
        self.predictive_modeler = StockPricePredictor()
        self.anomaly_detector = AnomalyDetector()
        
        # Set up output directory
        if output_dir:
            self.output_dir = output_dir
        else:
            project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.output_dir = os.path.join(project_dir, "analysis_results")
        
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Analysis results will be saved to {self.output_dir}")
    
    def run_correlation_analysis(self, ticker, days=90, save_plots=True):
        """
        Run correlation analysis between stock data and other factors
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to analyze
            save_plots: Whether to save correlation plots
        """
        logger.info(f"Running correlation analysis for {ticker}")
        
        # Fetch data
        stock_data = self.correlation_analyzer.fetch_stock_data(ticker, days)
        twitter_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "twitter", days)
        reddit_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "reddit", days)
        economic_data = self.correlation_analyzer.fetch_economic_data(days)
        news_data = self.correlation_analyzer.fetch_news_data(ticker, days)
        
        # Merge datasets
        merged_data = self.correlation_analyzer.merge_datasets(
            stock_data, twitter_data, reddit_data, economic_data, news_data
        )
        
        if merged_data is None or merged_data.empty:
            logger.error("Failed to create merged dataset for correlation analysis")
            return None
        
        # Calculate correlations
        correlation_matrix = self.correlation_analyzer.calculate_correlations(merged_data)
        
        # Visualize correlations
        if save_plots:
            save_path = os.path.join(self.output_dir, f"{ticker}_correlations.png")
            self.correlation_analyzer.visualize_correlations(
                correlation_matrix, 
                title=f"{ticker} Correlations with External Factors",
                save_path=save_path
            )
        else:
            self.correlation_analyzer.visualize_correlations(
                correlation_matrix,
                title=f"{ticker} Correlations with External Factors"
            )
        
        return correlation_matrix
    
    def run_predictive_modeling(self, ticker, days=180, target_days=5, save_model=True):
        """
        Train and evaluate predictive models for stock price movement
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to use
            target_days: Number of days ahead to predict
            save_model: Whether to save the trained model
        """
        logger.info(f"Running predictive modeling for {ticker}")
        
        # Prepare data
        X_train, X_test, y_train, y_test = self.predictive_modeler.prepare_data(
            ticker, days=days, target_days=target_days
        )
        
        if X_train is None:
            logger.error("Failed to prepare data for predictive modeling")
            return None
        
        # Train models
        models, scores = self.predictive_modeler.train_models(X_train, y_train, X_test, y_test)
        
        # Get best model
        best_model, best_score = self.predictive_modeler.get_best_model(models, scores)
        logger.info(f"Best model: {type(best_model).__name__} with score: {best_score:.4f}")
        
        # Save model
        if save_model:
            model_path = os.path.join(self.output_dir, f"{ticker}_price_model.pkl")
            self.predictive_modeler.save_model(best_model, model_path)
            
            # Generate feature importance plot
            importance_path = os.path.join(self.output_dir, f"{ticker}_feature_importance.png")
            self.predictive_modeler.plot_feature_importance(
                best_model, X_train.columns, save_path=importance_path
            )
            
            # Generate SHAP values plot if applicable
            shap_path = os.path.join(self.output_dir, f"{ticker}_shap_values.png")
            self.predictive_modeler.plot_shap_values(
                best_model, X_test, save_path=shap_path
            )
        
        # Make predictions
        future_predictions = self.predictive_modeler.predict_future(
            ticker, best_model, days=30, target_days=target_days
        )
        
        return best_model, future_predictions
    
    def run_anomaly_detection(self, ticker, days=90, methods=None):
        """
        Run anomaly detection for stock price, volume, and sentiment
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to use
            methods: List of anomaly detection methods to use
        """
        logger.info(f"Running anomaly detection for {ticker}")
        
        if methods is None:
            methods = ['price', 'volume', 'sentiment', 'correlation']
        
        results = {}
        
        # Detect price anomalies
        if 'price' in methods:
            price_anomalies = self.anomaly_detector.detect_price_anomalies(
                ticker, days=days, method='iforest'
            )
            
            if price_anomalies is not None:
                results['price_anomalies'] = price_anomalies
                
                # Visualize price anomalies
                save_path = os.path.join(self.output_dir, f"{ticker}_price_anomalies.png")
                self.anomaly_detector.visualize_price_anomalies(
                    price_anomalies, ticker, save_path=save_path
                )
        
        # Detect volume anomalies
        if 'volume' in methods:
            volume_anomalies = self.anomaly_detector.detect_volume_anomalies(
                ticker, days=days
            )
            
            if volume_anomalies is not None:
                results['volume_anomalies'] = volume_anomalies
        
        # Detect sentiment anomalies
        if 'sentiment' in methods:
            # Twitter sentiment anomalies
            twitter_anomalies = self.anomaly_detector.detect_sentiment_anomalies(
                ticker, source='twitter', days=days
            )
            
            if twitter_anomalies is not None:
                results['twitter_anomalies'] = twitter_anomalies
                
                # Visualize Twitter sentiment anomalies
                save_path = os.path.join(self.output_dir, f"{ticker}_twitter_anomalies.png")
                self.anomaly_detector.visualize_sentiment_anomalies(
                    twitter_anomalies, ticker, 'twitter', save_path=save_path
                )
            
            # Reddit sentiment anomalies
            reddit_anomalies = self.anomaly_detector.detect_sentiment_anomalies(
                ticker, source='reddit', days=days
            )
            
            if reddit_anomalies is not None:
                results['reddit_anomalies'] = reddit_anomalies
                
                # Visualize Reddit sentiment anomalies
                save_path = os.path.join(self.output_dir, f"{ticker}_reddit_anomalies.png")
                self.anomaly_detector.visualize_sentiment_anomalies(
                    reddit_anomalies, ticker, 'reddit', save_path=save_path
                )
        
        # Detect correlation breakdowns
        if 'correlation' in methods:
            correlation_anomalies = self.anomaly_detector.detect_correlation_breakdown(
                ticker, days=days
            )
            
            if correlation_anomalies is not None:
                results['correlation_anomalies'] = correlation_anomalies
        
        return results


def main():
    """Main function to run the analysis pipeline"""
    parser = argparse.ArgumentParser(description="Financial Data Analysis Pipeline")
    parser.add_argument("--ticker", required=True, help="Stock ticker symbol (e.g., AAPL, TSLA)")
    parser.add_argument("--days", type=int, default=90, help="Number of days of historical data to use")
    parser.add_argument("--analysis", nargs="+", choices=['correlation', 'prediction', 'anomaly', 'all'],
                        default=['all'], help="Analysis types to run")
    parser.add_argument("--output-dir", help="Directory to save analysis results")
    parser.add_argument("--target-days", type=int, default=5, 
                        help="Number of days ahead to predict (for predictive modeling)")
    parser.add_argument("--anomaly-methods", nargs="+", 
                        choices=['price', 'volume', 'sentiment', 'correlation', 'all'],
                        default=['all'], help="Anomaly detection methods to use")
    
    args = parser.parse_args()
    
    # Process arguments
    ticker = args.ticker.upper()
    days = args.days
    
    # Process analysis types
    analysis_types = args.analysis
    if 'all' in analysis_types:
        analysis_types = ['correlation', 'prediction', 'anomaly']
    
    # Process anomaly detection methods
    anomaly_methods = args.anomaly_methods
    if 'all' in anomaly_methods:
        anomaly_methods = ['price', 'volume', 'sentiment', 'correlation']
    
    # Create analysis pipeline
    pipeline = FinancialAnalysisPipeline(output_dir=args.output_dir)
    
    # Run selected analyses
    if 'correlation' in analysis_types:
        pipeline.run_correlation_analysis(ticker, days=days)
    
    if 'prediction' in analysis_types:
        pipeline.run_predictive_modeling(ticker, days=days, target_days=args.target_days)
    
    if 'anomaly' in analysis_types:
        pipeline.run_anomaly_detection(ticker, days=days, methods=anomaly_methods)
    
    logger.info("Analysis pipeline completed successfully")


if __name__ == "__main__":
    main()
