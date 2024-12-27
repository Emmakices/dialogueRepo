# Start from a modern Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /src

# Copy application code
COPY src src
COPY requirements.txt .

# Run ls to verify file structure
RUN ls -alR

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install DuckDB CLI (v0.9.0) for database interaction
FROM python:3.10-slim

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
