#!/bin/bash
set -e

# Create necessary directories if they don't exist
mkdir -p /app/logs
mkdir -p /app/raw_data
mkdir -p /app/processed_data

# Add the application directory to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/app"

# Execute the command passed to the script
exec "$@"
