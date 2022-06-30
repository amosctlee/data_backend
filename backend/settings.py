from pydantic import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    DB_URI: str

    class Config:
        env_file = Path(__file__).parent / ".env"


settings = Settings()