install:
	uv pip install -r requirements.txt

run:
	uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

format:
	black .

lint:
	flake8 .

test:
	pytest
