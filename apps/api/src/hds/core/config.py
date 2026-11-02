"""Application settings read from the environment (and ``.env`` in development).

Layer: core. Imports nothing from other ``hds`` packages; every other layer may import it.

Secrets such as ``JWT_SECRET_KEY`` come only from the environment or a secret store and are
never committed (Constitution VI). Validators here fail the process at start-up rather than
letting an unsafe production configuration run.
"""

from typing import Self

import email_validator
from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings

PLACEHOLDER_JWT_SECRET = "dev-secret-change-in-production"
MIN_JWT_SECRET_BYTES = 32  # RFC 7518 §3.2: an HS256 key should be at least 256 bits


class Settings(BaseSettings):
    """Typed configuration for the API process.

    Attributes:
        ENVIRONMENT: ``production`` switches on the strict secret and e-mail checks.
        DATABASE_URL: Async SQLAlchemy URL of the application database.
        JWT_SECRET_KEY: HMAC key for access tokens; the placeholder is rejected in production.
        JWT_ALGORITHM: Signature algorithm; the only one accepted when decoding.
        ACCESS_TOKEN_TTL_SECONDS: Access-token lifetime; Constitution VI caps it at 15 minutes.
        REFRESH_TOKEN_TTL_DAYS: Refresh-token lifetime before sign-in is required again.
        LOGIN_MAX_FAILURES: Consecutive failed sign-ins before the account locks.
        LOGIN_LOCKOUT_MINUTES: How long a locked account stays locked.
        CORS_ORIGINS: Explicit allowed origins; credentials are allowed, so ``*`` is rejected.

    Raises:
        ValueError: On construction, if ``CORS_ORIGINS`` contains ``*``, or in production if
            the JWT secret is the placeholder or shorter than 32 bytes.
    """

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
        if self.ENVIRONMENT.lower() != "production":
            return self
        secret = self.JWT_SECRET_KEY.get_secret_value()
        if secret == PLACEHOLDER_JWT_SECRET:
            raise ValueError(
                "JWT_SECRET_KEY must be set to a real secret when ENVIRONMENT=production"
            )
        if len(secret.encode()) < MIN_JWT_SECRET_BYTES:
            raise ValueError(
                f"JWT_SECRET_KEY must be at least {MIN_JWT_SECRET_BYTES} bytes "
                "when ENVIRONMENT=production"
            )
        return self


settings = Settings()


def configure_email_validation(current: Settings) -> None:
    """Allow reserved test domains in every environment except production.

    The seeded development accounts use ``@hds.test``; email-validator rejects that TLD unless
    told it is in a test environment. Production keeps the strict check.

    Side effects:
        Sets the module-level ``email_validator.TEST_ENVIRONMENT`` flag.

    Args:
        current: The settings whose ``ENVIRONMENT`` decides the mode.
    """
    email_validator.TEST_ENVIRONMENT = current.ENVIRONMENT.lower() != "production"


def get_settings() -> Settings:
    """Return the process-wide settings.

    Exposed as a function so tests can override it through FastAPI's dependency overrides
    instead of patching the module global.

    Returns:
        The ``Settings`` instance built at import time.
    """
    return settings
