from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds"
    JWT_SECRET_KEY: SecretStr = SecretStr("dev-secret-change-in-production")
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_TTL_SECONDS: int = 900  # 15 minutes
    REFRESH_TOKEN_TTL_DAYS: int = 7
    LOGIN_MAX_FAILURES: int = 5
    LOGIN_LOCKOUT_MINUTES: int = 15

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()


def get_settings() -> Settings:
    return settings
