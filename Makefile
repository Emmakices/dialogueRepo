build:
	docker build -t toubib_connector .

init-db:
	docker run --rm --interactive --tty \
	-v ./data:/data \
	toubib_connector python src/init_db.py

# Run the Docker container with the given API_BASEURL.
# The container runs in interactive mode (so you can see the logs), and will self-
run:
	docker run --rm --interactive --tty \
	--network toubib_network \
	-e API_BASEURL="http://toubib:8000" \
	-v ./data:/data \
	toubib_connector

run-api-service:
	docker compose up