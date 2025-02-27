from src.tasks.celery_tasks import celery_app

# This allows running the worker with: celery -A celery_worker worker --loglevel=info
if __name__ == "__main__":
    celery_app.start()
