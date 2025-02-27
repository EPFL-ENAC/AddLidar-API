from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    IMAGE_NAME: str = "lidardatamanager"
    IMAGE_TAG: str = "latest"
    GH_USERNAME: str = "ghcr.io"
    GH_PAT: str = "ghp_1234567890abcdef"
    REGISTRY: str = "ghcr.io"
    # Default ROOT_VOLUME path based on environment
    ROOT_VOLUME: str = "/data"
    API_PREFIX: str = "/api"
    PORT: int = 8000
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # If we're in local development and ROOT_VOLUME is still the default /data
        # Set it to the local path for easier debugging
        if self.ENVIRONMENT == "development" and self.ROOT_VOLUME == "/data":
            local_path = "/Users/pierreguilbert/Works/git/github/EPFL-ENAC/AddLidar-API/lidar-api/data"
            if os.path.exists(local_path):
                self.ROOT_VOLUME = local_path
    
    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()
