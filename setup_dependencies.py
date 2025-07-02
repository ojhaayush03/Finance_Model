#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Setup script to install all dependencies for the financial trends project
This script can be run both locally and in Google Colab
"""

import sys
import subprocess
import pkg_resources
import os

def install_package(package):
    """Install a package if it's not already installed"""
    try:
        pkg_resources.get_distribution(package)
        print(f"✓ {package} is already installed")
    except pkg_resources.DistributionNotFound:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"✓ {package} installed successfully")

def setup_dependencies():
    """Install all required dependencies"""
    print("Setting up dependencies for Financial Trends Project...")
    
    # Required packages
    packages = [
        "streamlit",
        "pandas",
        "matplotlib",
        "seaborn",
        "pyspark",
        "pymongo",
        "dnspython",  # Required for MongoDB connection
        "nltk",
        "scikit-learn",
        "numpy",
        "textblob",
    ]
    
    # Install each package
    for package in packages:
        install_package(package)
    
    # Download NLTK data
    print("\nDownloading NLTK data...")
    import nltk
    try:
        nltk.data.find('vader_lexicon')
        print("✓ NLTK vader_lexicon already downloaded")
    except LookupError:
        nltk.download('vader_lexicon')
        print("✓ NLTK vader_lexicon downloaded successfully")
    
    print("\nAll dependencies installed successfully!")
    print("\nTo run the Streamlit app locally, use the command:")
    print("streamlit run streamlit_app.py")

if __name__ == "__main__":
    setup_dependencies()
