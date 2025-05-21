FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy requirements file
COPY requirements.txt .

# Install dependencies with better error handling
RUN pip install --no-cache-dir -r requirements.txt || \
    (echo "Dependency installation failed, retrying with more verbose output" && \
     pip install -v --no-cache-dir -r requirements.txt)

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p logs raw_data processed_data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Copy initialization script
COPY docker-init.py /app/

# Set entrypoint to run the initialization script first
ENTRYPOINT ["python", "/app/docker-init.py"]

# Default command (can be overridden)
CMD ["python", "run_streaming_pipeline.py", "--help"]
