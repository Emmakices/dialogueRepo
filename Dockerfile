# Start from a modern Python image
FROM python:3.11-slim AS base

# Set working directory
WORKDIR /app

# Copy application requirements and source code
COPY requirements.txt .
COPY src /app/src

# Run ls to verify file structure
RUN ls -alR /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install DuckDB CLI (v0.9.0) for database interaction
FROM base AS final

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    && wget https://github.com/duckdb/duckdb/releases/download/v0.9.1/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-amd64.zip

# Set up the DuckDB destination file in a mounted volume
VOLUME /data
ENV DESTINATION_DUCKDB_PATH=/data/replicated_patients.duckdb

# Run the application
CMD ["python", "src/connector.py"]