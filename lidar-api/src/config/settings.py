from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    IMAGE_NAME: str = "ghcr.io/epfl-enac/lidardatamanager"
    IMAGE_TAG: str = "latest"
    ROOT_VOLUME: str = ""  # Will be set based on PVC
    NAMESPACE: str = "default"
    MOUNT_PATH: str = "/data"
    PVC_NAME: str = "lidar-data-pvc"  # Default to our created PVC
    JOB_TIMEOUT: int = 300  # Timeout in seconds for job completion
    DEFAULT_ROOT: str = "/data"  # Default root path based on environment

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
