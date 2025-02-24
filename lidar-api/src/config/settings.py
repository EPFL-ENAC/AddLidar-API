import os


class Settings:
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.docker_image = os.getenv("DOCKER_IMAGE", "lidardatamanager:latest")
        self.docker_volume = os.getenv("DOCKER_VOLUME", "/path/to/dummy/folder")
        self.api_prefix = os.getenv("API_PREFIX", "/api")
        self.port = int(os.getenv("PORT", 8000))


settings = Settings()
