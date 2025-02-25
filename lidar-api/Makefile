# Define Python version (optional)
PYTHON_VERSION = 3.11.5

# Sync dependencies
sync:
	@echo "Syncing dependencies"
	uv sync

run:
	uvicorn src.main:app --reload --host 0.0.0.0 --port 8000


docker-build:
	@echo "Building docker image"
	docker build -t lidar-api .

docker-run: docker-build
	@echo "Running docker image"
	@echo "Open your browser at http://localhost:8081"
	docker run -p 8081:8081 lidar-api:latest



format:
	black .

lint:
	flake8 .

test:
	uv run pytest

scout:
	docker scout cves local://lidar-api:latest