from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    IMAGE_NAME: str = "ghcr.io/epfl-enac/lidardatamanager"
    IMAGE_TAG: str = "latest"
    NAMESPACE: str = "epfl-cryos-addlidar-potree-dev"
    MOUNT_PATH: str = "/data"
    OUTPUT_PATH: str = "/output"
    PVC_OUTPUT_NAME: str = "lidar-data-output-pvc"  # Default to our created PVC
    PVC_NAME: str = "lidar-data-pvc"  # Default to our created PVC
    JOB_TIMEOUT: int = 300  # Timeout in seconds for job completion
    DEFAULT_OUTPUT_ROOT: str = "/output"  # Default root path based on environment

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
