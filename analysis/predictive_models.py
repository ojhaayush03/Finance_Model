"""
Predictive Models Module

This module builds and evaluates machine learning models to predict stock price movements based on:
- Technical indicators
- Social sentiment (Twitter, Reddit)
- Economic indicators
- News sentiment

It provides functions to train, evaluate, and save predictive models.
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
from datetime import datetime, timedelta
import logging

# Machine learning imports
from sklearn.model_selection import train_test_split, TimeSeriesSplit, GridSearchCV
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression, Ridge, Lasso
import xgboost as xgb
import shap

# Add project root to path to import config and other modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import get_config
from analysis.correlation_analysis import CorrelationAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('predictive_models')

class StockPricePredictor:
    def __init__(self):
        """Initialize the stock price predictor"""
        self.correlation_analyzer = CorrelationAnalyzer()
        
        # Create directories for saving models and results
        project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.models_dir = os.path.join(project_dir, "models")
        self.results_dir = os.path.join(project_dir, "analysis_results")
        
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)
        
        logger.info("Stock price predictor initialized")
    
    def prepare_features_target(self, data, target_column='close', prediction_horizon=1):
        """
        Prepare features and target variables for prediction
        
        Args:
            data: DataFrame with merged data from different sources
            target_column: Column to predict (default: 'close' for closing price)
            prediction_horizon: Number of days ahead to predict (default: 1)
            
        Returns:
            X: Feature matrix
            y: Target vector
            feature_names: List of feature names
        """
        if data is None or data.empty:
            logger.error("No data available for feature preparation")
            return None, None, None
        
        # Sort by date to ensure correct time sequence
        data = data.sort_values('date')
        
        # Create target variable (future price)
        data[f'future_{target_column}_{prediction_horizon}d'] = data[target_column].shift(-prediction_horizon)
        
        # Drop rows with NaN in target (will be at the end due to shift)
        data = data.dropna(subset=[f'future_{target_column}_{prediction_horizon}d'])
        
        # Create additional technical features
        data = self._add_technical_features(data, target_column)
        
        # Drop date column and any remaining NaN values
        data = data.drop(columns=['date']).dropna()
        
        # Split features and target
        y = data[f'future_{target_column}_{prediction_horizon}d']
        X = data.drop(columns=[target_column, f'future_{target_column}_{prediction_horizon}d'])
        
        feature_names = X.columns.tolist()
        
        logger.info(f"Prepared {X.shape[1]} features and {len(y)} samples for prediction")
        return X, y, feature_names
    
    def _add_technical_features(self, data, price_column):
        """Add technical indicators as features"""
        # Make a copy to avoid modifying the original dataframe
        df = data.copy()
        
        # Simple moving averages
        df['sma_5'] = df[price_column].rolling(window=5).mean()
        df['sma_10'] = df[price_column].rolling(window=10).mean()
        df['sma_20'] = df[price_column].rolling(window=20).mean()
        
        # Exponential moving averages
        df['ema_5'] = df[price_column].ewm(span=5, adjust=False).mean()
        df['ema_10'] = df[price_column].ewm(span=10, adjust=False).mean()
        df['ema_20'] = df[price_column].ewm(span=20, adjust=False).mean()
        
        # Price momentum
        df['momentum_1d'] = df[price_column].pct_change(periods=1)
        df['momentum_5d'] = df[price_column].pct_change(periods=5)
        df['momentum_10d'] = df[price_column].pct_change(periods=10)
        
        # Volatility (standard deviation over rolling window)
        df['volatility_5d'] = df[price_column].rolling(window=5).std()
        df['volatility_10d'] = df[price_column].rolling(window=10).std()
        
        # Relative Strength Index (RSI)
        delta = df[price_column].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        
        # Avoid division by zero
        loss = loss.replace(0, np.nan)
        rs = gain / loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        # Moving Average Convergence Divergence (MACD)
        df['macd'] = df[price_column].ewm(span=12, adjust=False).mean() - df[price_column].ewm(span=26, adjust=False).mean()
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # Volume features
        if 'volume' in df.columns:
            df['volume_change'] = df['volume'].pct_change()
            df['volume_ma_5'] = df['volume'].rolling(window=5).mean()
            df['price_volume_ratio'] = df[price_column] / df['volume']
        
        return df
    
    def train_models(self, ticker, days=90, prediction_horizon=1, target_column='close'):
        """
        Train multiple predictive models for stock price prediction
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of historical data to use
            prediction_horizon: Number of days ahead to predict
            target_column: Column to predict (default: 'close')
            
        Returns:
            Dictionary of trained models and their performance metrics
        """
        logger.info(f"Training predictive models for {ticker} with {days} days of data")
        
        # Fetch and merge data from different sources
        stock_data = self.correlation_analyzer.fetch_stock_data(ticker, days)
        twitter_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "twitter", days)
        reddit_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "reddit", days)
        economic_data = self.correlation_analyzer.fetch_economic_data(days)
        news_data = self.correlation_analyzer.fetch_news_data(ticker, days)
        
        merged_data = self.correlation_analyzer.merge_datasets(
            stock_data, twitter_data, reddit_data, economic_data, news_data
        )
        
        if merged_data is None or merged_data.empty:
            logger.error("Failed to create merged dataset for model training")
            return None
        
        # Prepare features and target
        X, y, feature_names = self.prepare_features_target(
            merged_data, target_column, prediction_horizon
        )
        
        if X is None or y is None:
            logger.error("Failed to prepare features and target for model training")
            return None
        
        # Use time series split for validation
        tscv = TimeSeriesSplit(n_splits=5)
        
        # Define models to train
        models = {
            'linear': LinearRegression(),
            'ridge': Ridge(),
            'lasso': Lasso(),
            'random_forest': RandomForestRegressor(n_estimators=100, random_state=42),
            'gradient_boosting': GradientBoostingRegressor(n_estimators=100, random_state=42),
            'xgboost': xgb.XGBRegressor(n_estimators=100, random_state=42)
        }
        
        # Train and evaluate models
        results = {}
        best_model = None
        best_score = float('inf')  # Lower MSE is better
        
        for name, model in models.items():
            logger.info(f"Training {name} model...")
            
            # Create a pipeline with scaling
            pipeline = Pipeline([
                ('scaler', StandardScaler()),
                ('model', model)
            ])
            
            # Train and evaluate using time series cross-validation
            cv_scores = []
            
            for train_idx, test_idx in tscv.split(X):
                X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
                y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
                
                pipeline.fit(X_train, y_train)
                y_pred = pipeline.predict(X_test)
                
                mse = mean_squared_error(y_test, y_pred)
                cv_scores.append(mse)
            
            # Calculate average metrics
            avg_mse = np.mean(cv_scores)
            
            # Train on full dataset for final model
            pipeline.fit(X, y)
            
            # Save results
            results[name] = {
                'model': pipeline,
                'mse': avg_mse,
                'rmse': np.sqrt(avg_mse),
                'feature_names': feature_names
            }
            
            logger.info(f"{name} model - MSE: {avg_mse:.4f}, RMSE: {np.sqrt(avg_mse):.4f}")
            
            # Track best model
            if avg_mse < best_score:
                best_score = avg_mse
                best_model = name
        
        logger.info(f"Best model: {best_model} with MSE: {best_score:.4f}")
        
        # Save the best model
        if best_model:
            best_pipeline = results[best_model]['model']
            model_path = os.path.join(self.models_dir, f"{ticker}_price_predictor_{prediction_horizon}d.pkl")
            
            with open(model_path, 'wb') as f:
                pickle.dump(best_pipeline, f)
            
            logger.info(f"Best model saved to {model_path}")
            
            # Generate feature importance for the best model
            if best_model in ['random_forest', 'gradient_boosting', 'xgboost']:
                self._plot_feature_importance(
                    results[best_model]['model']['model'], 
                    feature_names,
                    f"{ticker} {prediction_horizon}-Day Price Prediction Feature Importance",
                    os.path.join(self.results_dir, f"{ticker}_feature_importance_{prediction_horizon}d.png")
                )
                
                # Generate SHAP values for the best model
                self._plot_shap_values(
                    results[best_model]['model']['model'],
                    X,
                    feature_names,
                    f"{ticker} {prediction_horizon}-Day Price Prediction SHAP Values",
                    os.path.join(self.results_dir, f"{ticker}_shap_values_{prediction_horizon}d.png")
                )
        
        return results
    
    def _plot_feature_importance(self, model, feature_names, title, save_path=None):
        """Plot feature importance for tree-based models"""
        try:
            # Get feature importances
            importances = model.feature_importances_
            
            # Sort features by importance
            indices = np.argsort(importances)[::-1]
            
            # Plot
            plt.figure(figsize=(12, 8))
            plt.title(title)
            plt.bar(range(len(importances)), importances[indices])
            plt.xticks(range(len(importances)), [feature_names[i] for i in indices], rotation=90)
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"Feature importance plot saved to {save_path}")
            else:
                plt.show()
                
        except Exception as e:
            logger.error(f"Error plotting feature importance: {str(e)}")
    
    def _plot_shap_values(self, model, X, feature_names, title, save_path=None):
        """Plot SHAP values to explain model predictions"""
        try:
            # Create SHAP explainer
            explainer = shap.Explainer(model)
            shap_values = explainer(X)
            
            # Plot
            plt.figure(figsize=(12, 8))
            plt.title(title)
            shap.summary_plot(shap_values, X, feature_names=feature_names, show=False)
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path)
                logger.info(f"SHAP values plot saved to {save_path}")
            else:
                plt.show()
                
        except Exception as e:
            logger.error(f"Error plotting SHAP values: {str(e)}")
    
    def predict_future_prices(self, ticker, days_ahead=5, model_path=None):
        """
        Predict future prices for a given ticker
        
        Args:
            ticker: Stock ticker symbol
            days_ahead: Number of days to predict into the future
            model_path: Path to saved model (if None, will use default path)
            
        Returns:
            DataFrame with predicted prices
        """
        logger.info(f"Predicting future prices for {ticker} {days_ahead} days ahead")
        
        # Load the model if path is provided, otherwise use default path
        if model_path is None:
            model_path = os.path.join(self.models_dir, f"{ticker}_price_predictor_1d.pkl")
        
        if not os.path.exists(model_path):
            logger.error(f"Model file not found at {model_path}")
            return None
        
        try:
            with open(model_path, 'rb') as f:
                model = pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            return None
        
        # Fetch the most recent data
        stock_data = self.correlation_analyzer.fetch_stock_data(ticker, 30)  # Get last 30 days
        twitter_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "twitter", 30)
        reddit_data = self.correlation_analyzer.fetch_sentiment_data(ticker, "reddit", 30)
        economic_data = self.correlation_analyzer.fetch_economic_data(30)
        news_data = self.correlation_analyzer.fetch_news_data(ticker, 30)
        
        merged_data = self.correlation_analyzer.merge_datasets(
            stock_data, twitter_data, reddit_data, economic_data, news_data
        )
        
        if merged_data is None or merged_data.empty:
            logger.error("Failed to create merged dataset for prediction")
            return None
        
        # Sort by date
        merged_data = merged_data.sort_values('date')
        
        # Create predictions dataframe
        predictions = pd.DataFrame()
        predictions['date'] = [merged_data['date'].max() + timedelta(days=i+1) for i in range(days_ahead)]
        predictions['predicted_close'] = np.nan
        
        # Get the most recent data point
        latest_data = merged_data.iloc[-1:].copy()
        
        # Make predictions for each day ahead
        for i in range(days_ahead):
            # Prepare features
            latest_data = self._add_technical_features(latest_data, 'close')
            
            # Drop unnecessary columns
            X = latest_data.drop(columns=['date', 'close'])
            
            # Handle missing features or extra features
            model_features = model.feature_names_in_
            
            # Add missing columns with zeros
            for feature in model_features:
                if feature not in X.columns:
                    X[feature] = 0
            
            # Keep only the features used by the model
            X = X[model_features]
            
            # Make prediction
            predicted_price = model.predict(X)[0]
            
            # Update predictions
            predictions.loc[i, 'predicted_close'] = predicted_price
            
            # Update latest data for next prediction
            latest_data['close'] = predicted_price
        
        logger.info(f"Generated {days_ahead} days of price predictions for {ticker}")
        return predictions

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Stock Price Prediction Models")
    parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL, TSLA)")
    parser.add_argument("--days", type=int, default=90, help="Number of days of historical data to use")
    parser.add_argument("--horizon", type=int, default=1, help="Prediction horizon in days")
    parser.add_argument("--predict", action="store_true", help="Make predictions for future days")
    parser.add_argument("--predict-days", type=int, default=5, help="Number of days to predict ahead")
    
    args = parser.parse_args()
    
    predictor = StockPricePredictor()
    
    # Train models if not just predicting
    if not args.predict:
        results = predictor.train_models(
            args.ticker.upper(), 
            days=args.days, 
            prediction_horizon=args.horizon
        )
    
    # Make predictions if requested
    if args.predict:
        predictions = predictor.predict_future_prices(
            args.ticker.upper(),
            days_ahead=args.predict_days
        )
        
        if predictions is not None:
            print(f"\nPredicted prices for {args.ticker.upper()} for the next {args.predict_days} days:")
            print(predictions[['date', 'predicted_close']])
