from typing import Self

import email_validator
from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings

PLACEHOLDER_JWT_SECRET = "dev-secret-change-in-production"


class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    DATABASE_URL: str = "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds"
    JWT_SECRET_KEY: SecretStr = SecretStr(PLACEHOLDER_JWT_SECRET)
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_TTL_SECONDS: int = 900  # 15 minutes
    REFRESH_TOKEN_TTL_DAYS: int = 7
    LOGIN_MAX_FAILURES: int = 5
    LOGIN_LOCKOUT_MINUTES: int = 15
    # JSON list in the environment, e.g. CORS_ORIGINS='["https://app.example.com"]'.
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:8081"]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @field_validator("CORS_ORIGINS")
    @classmethod
    def _no_wildcard(cls, origins: list[str]) -> list[str]:
        # Credentials are allowed, and a wildcard origin must never be combined with them.
        if any(origin.strip() == "*" for origin in origins):
            raise ValueError("CORS_ORIGINS must list explicit origins; '*' is not allowed")
        return origins

    @model_validator(mode="after")
    def _production_needs_real_secret(self) -> Self:
        if (
            self.ENVIRONMENT.lower() == "production"
            and self.JWT_SECRET_KEY.get_secret_value() == PLACEHOLDER_JWT_SECRET
        ):
            raise ValueError(
                "JWT_SECRET_KEY must be set to a real secret when ENVIRONMENT=production"
            )
        return self


settings = Settings()


def configure_email_validation(current: Settings) -> None:
    """Accept reserved domains such as ``hds.test`` everywhere except production.

    The seeded development accounts use ``@hds.test``; email-validator rejects that TLD unless
    told it is in a test environment. Production keeps the strict check.
    """
    email_validator.TEST_ENVIRONMENT = current.ENVIRONMENT.lower() != "production"


def get_settings() -> Settings:
    return settings
