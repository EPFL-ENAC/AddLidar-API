from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    IMAGE_NAME: str = "lidardatamanager"
    IMAGE_TAG: str = "latest"
    GH_USERNAME: str = "ghcr.io"
    GH_PAT: str = "ghp_1234567890abcdef"
    REGISTRY: str = "ghcr.io"
    ROOT_VOLUME: str = "/data"
    API_PREFIX: str = "/api"
    PORT: int = 8000
    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
