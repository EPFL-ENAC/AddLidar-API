from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    DOCKER_IMAGE: str = "lidardatamanager:latest"
    DOCKER_VOLUME: str = "/data"
    API_PREFIX: str = "/api"
    PORT: int = 8000
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
