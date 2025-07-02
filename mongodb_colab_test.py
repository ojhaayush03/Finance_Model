#!/usr/bin/env python3
"""
MongoDB Connection Test for Google Colab

This script tests the connection to a MongoDB database and retrieves sample data
from various collections to verify data access.
"""

import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns

# Function to get MongoDB connection string
def get_mongodb_uri():
    # Replace with your actual MongoDB connection string
    # For Google Colab, you'll need to enter this manually or use a secrets manager
    return input("Enter your MongoDB connection string: ")

# Connect to MongoDB
def connect_to_mongodb(uri):
    try:
        client = MongoClient(uri)
        # Test the connection
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

# List available databases and collections
def explore_database(client):
    print("\n📊 Available databases:")
    dbs = client.list_database_names()
    for db in dbs:
        print(f"  - {db}")
        
    # Ask user which database to explore
    db_name = input("\nEnter database name to explore: ")
    db = client[db_name]
    
    print(f"\n📁 Collections in {db_name}:")
    collections = db.list_collection_names()
    for i, collection in enumerate(collections):
        print(f"  {i+1}. {collection}")
    
    return db, collections

# Sample data from a collection
def sample_collection(db, collection_name, limit=5):
    try:
        collection = db[collection_name]
        count = collection.count_documents({})
        print(f"\n📝 Collection '{collection_name}' has {count} documents")
        
        if count > 0:
            print(f"\nSample data (up to {limit} documents):")
            cursor = collection.find().limit(limit)
            data = list(cursor)
            
            # Convert to DataFrame for better display
            df = pd.DataFrame(data)
            if not df.empty:
                # Drop _id column for cleaner display
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)
                print(df)
                return df
            else:
                print("No data found in collection.")
                return None
        else:
            print("Collection is empty.")
            return None
    except Exception as e:
        print(f"Error sampling collection: {e}")
        return None

# Visualize time series data if available
def visualize_time_series(df, date_col, value_col):
    if df is None or df.empty or date_col not in df.columns or value_col not in df.columns:
        print("Cannot visualize: Missing required data or columns")
        return
    
    try:
        # Convert date column to datetime
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Sort by date
        df = df.sort_values(by=date_col)
        
        # Create plot
        plt.figure(figsize=(12, 6))
        plt.plot(df[date_col], df[value_col], marker='o', linestyle='-')
        plt.title(f'{value_col} over time')
        plt.xlabel('Date')
        plt.ylabel(value_col)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Error visualizing data: {e}")

# Main function
def main():
    print("🔍 MongoDB Connection Test for Financial Trends Project")
    print("-" * 50)
    
    # Get connection string and connect
    uri = get_mongodb_uri()
    client = connect_to_mongodb(uri)
    
    if client:
        # Explore database structure
        db, collections = explore_database(client)
        
        while True:
            # Let user choose a collection to sample
            try:
                choice = input("\nEnter collection number to sample (or 'q' to quit): ")
                
                if choice.lower() == 'q':
                    break
                    
                idx = int(choice) - 1
                if 0 <= idx < len(collections):
                    collection_name = collections[idx]
                    df = sample_collection(db, collection_name)
                    
                    # If we have data, try to visualize it
                    if df is not None and not df.empty:
                        # Check if this looks like time series data
                        date_cols = [col for col in df.columns if 'date' in col.lower() 
                                    or 'time' in col.lower() or 'created' in col.lower()]
                        
                        if date_cols and len(df.columns) > 1:
                            date_col = date_cols[0]
                            
                            # Find potential value columns (numeric)
                            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                            
                            if numeric_cols:
                                print("\nAvailable numeric columns for visualization:")
                                for i, col in enumerate(numeric_cols):
                                    print(f"  {i+1}. {col}")
                                    
                                val_choice = input("\nEnter column number to visualize (or 's' to skip): ")
                                
                                if val_choice.lower() != 's':
                                    try:
                                        val_idx = int(val_choice) - 1
                                        if 0 <= val_idx < len(numeric_cols):
                                            visualize_time_series(df, date_col, numeric_cols[val_idx])
                                    except:
                                        print("Invalid choice, skipping visualization.")
                else:
                    print("Invalid collection number.")
            except Exception as e:
                print(f"Error: {e}")
        
        print("\nClosing connection...")
        client.close()
        print("Done! Connection closed.")
    
    print("\nThank you for testing the MongoDB connection!")

if __name__ == "__main__":
    # This will run when executed directly
    main()
    
    # For Google Colab, you might want to add instructions
    print("\n" + "*" * 80)
    print("INSTRUCTIONS FOR GOOGLE COLAB:")
    print("1. Upload this script to your Colab notebook")
    print("2. Run the script using: %run mongodb_colab_test.py")
    print("3. Enter your MongoDB connection string when prompted")
    print("4. Follow the interactive prompts to explore your data")
    print("*" * 80)
