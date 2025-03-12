# Define Python version (optional)
PYTHON_VERSION = 3.11.5

# Sync dependencies
sync:
	@echo "Syncing dependencies"
	uv sync

run:
	uv run uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 --log-level debug --access-log --use-colors

run-worker:
	REDIS_HOST=localhost uv run celery -A src.celery_worker worker --loglevel=info



docker-build:
	@echo "Building docker image"
	docker build -t lidar-api .

docker-run: docker-build
	@echo "Running docker image"
	@echo "Open your browser at http://localhost:8081"
	docker run -p 8081:8081 lidar-api:latest



format:
	black src

lint:
	flake8 src --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 src --count --exit-zero --max-complexity=15 --max-line-length=127 --statistics

test:
	uv run pytest

scout:
	docker scout cves local://lidar-api:latest