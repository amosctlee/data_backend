from pydantic import BaseSettings


class Settings(BaseSettings):
    DB_URI: str

    class Config:
        env_file = ".env"


settings = Settings()