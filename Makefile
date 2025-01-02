# Build the Docker image
build:
	docker build -t toubib_connector .

# Start the API service using Docker Compose
start-api:
	docker-compose up -d

# Stop all services (API and Connector)
stop:
	docker-compose down

# Initialize the database schema locally
init-db:
	python init_db.py

# Run the connector locally to fetch data from the API
run:
	python connector.py

# Check the database content using DuckDB CLI (local pathway)
check-db:
	C:/Users/Eco/Desktop/duckdb.exe data/destination.duckdb

# Clean up data files
clean-data:
	Remove-Item -Path ./data/* -Force