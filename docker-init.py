#!/usr/bin/env python
"""
Docker Initialization Script for Financial Trends Project

This script fixes import paths and prepares the environment for running in Docker.
It creates symbolic links to ensure modules can be imported correctly.
"""

import os
import sys
import shutil
import importlib
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('docker_init')

def fix_import_paths():
    """Create symbolic links to ensure imports work correctly in Docker"""
    logger.info("Fixing import paths for Docker environment...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_ingestion_dir = os.path.join(project_root, 'data_ingestion')
    
    # Create symbolic links for key modules
    modules_to_link = [
        'db_utils.py',
        'kafka_consumer.py',
        'kafka_producer.py',
        'kafka_integration.py',
        'enhanced_pipeline.py',
        'economic_indicators.py',
        'social_sentiment.py',
        'news_api.py',
        'reddit_api.py',
        'fetch_stock_data.py',
        'process_indicators.py'
    ]
    
    for module in modules_to_link:
        source_path = os.path.join(data_ingestion_dir, module)
        target_path = os.path.join(project_root, module)
        
        if os.path.exists(source_path) and not os.path.exists(target_path):
            try:
                # Create a symbolic link
                os.symlink(source_path, target_path)
                logger.info(f"Created symbolic link: {target_path} -> {source_path}")
            except OSError:
                # If symlink fails (e.g., on Windows), copy the file
                shutil.copy2(source_path, target_path)
                logger.info(f"Copied file: {source_path} -> {target_path}")

def create_init_files():
    """Create __init__.py files to make directories importable"""
    logger.info("Creating __init__.py files...")
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Directories that need __init__.py files
    dirs_to_init = [
        project_root,
        os.path.join(project_root, 'data_ingestion')
    ]
    
    for directory in dirs_to_init:
        init_file = os.path.join(directory, '__init__.py')
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write('# This file makes the directory a Python package\n')
            logger.info(f"Created {init_file}")

def verify_imports():
    """Verify that key modules can be imported"""
    logger.info("Verifying imports...")
    
    modules_to_check = [
        'db_utils',
        'kafka_consumer',
        'kafka_producer',
        'kafka_integration'
    ]
    
    for module_name in modules_to_check:
        try:
            # Try to import the module
            module = importlib.import_module(module_name)
            logger.info(f"✅ Successfully imported {module_name}")
        except ImportError as e:
            logger.warning(f"❌ Failed to import {module_name}: {str(e)}")

def run_command(command):
    """Run a shell command and log the output"""
    logger.info(f"Running command: {command}")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info(f"Command output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr}")
        return False

def main():
    """Main initialization function"""
    logger.info("Starting Docker initialization...")
    
    # Fix import paths
    fix_import_paths()
    
    # Create __init__.py files
    create_init_files()
    
    # Verify imports
    verify_imports()
    
    logger.info("Docker initialization completed")
    
    # Run the command passed as arguments
    if len(sys.argv) > 1:
        command = ' '.join(sys.argv[1:])
        logger.info(f"Running command: {command}")
        os.system(command)

if __name__ == "__main__":
    main()
