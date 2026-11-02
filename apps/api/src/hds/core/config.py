"""Application settings read from the environment (and ``.env`` in development).

Layer: core. Imports nothing from other ``hds`` packages; every other layer may import it.

Secrets such as ``JWT_SECRET_KEY`` come only from the environment or a secret store and are
never committed (Constitution VI). Validators here fail the process at start-up rather than
letting an unsafe configuration run. ``ENVIRONMENT`` and ``JWT_SECRET_KEY`` have no defaults, so a
deployment that forgets them stops instead of starting with a published key. Only the
``development`` and ``test`` environments may use a weak secret or the development mail outbox;
every other value (``production``, ``staging``, ...) gets the strict checks.
"""

from pathlib import Path
from typing import Self

import email_validator
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings

# The placeholder value that older configuration files and examples published. It is public,
# not a secret: the settings use it only to REFUSE a signing key that equals it.
PUBLISHED_PLACEHOLDER = "dev-secret-change-in-production"
MIN_JWT_SECRET_BYTES = 32  # RFC 7518 §3.2: an HS256 key should be at least 256 bits

#: The only environments allowed to use a weak secret and the development mail outbox.
DEVELOPMENT_ENVIRONMENTS = frozenset({"development", "test"})

#: Mail backends that really deliver messages. None exists yet, so a non-development
#: environment cannot start until a provider feature registers one (specs/002 research D6).
REAL_MAIL_BACKENDS: frozenset[str] = frozenset()

#: The development-only backend that stores a redacted record of each message in a local file.
OUTBOX_BACKEND = "outbox"

_DEFAULT_OUTBOX_PATH = str(Path(__file__).resolve().parents[3] / ".dev-mail" / "outbox.jsonl")


class Settings(BaseSettings):
    """Typed configuration for the API process.

    Attributes:
        ENVIRONMENT: Required. ``development`` and ``test`` are development environments; any
            other value switches on the strict secret, e-mail and mail-backend checks.
        DATABASE_URL: Async SQLAlchemy URL of the application database.
        JWT_SECRET_KEY: Required. HMAC key for access tokens; outside development it must not be
            the placeholder and must be at least 32 bytes.
        JWT_ALGORITHM: Signature algorithm; the only one accepted when decoding.
        ACCESS_TOKEN_TTL_SECONDS: Access-token lifetime; Constitution VI caps it at 15 minutes.
        REFRESH_TOKEN_TTL_DAYS: Refresh-token lifetime before sign-in is required again.
        LOGIN_MAX_FAILURES: Consecutive failed sign-ins before the account locks.
        LOGIN_RATE_LIMIT_PER_NETWORK: Failed sign-ins allowed from one network address inside one
            window of ``LOGIN_LOCKOUT_MINUTES``; 0 turns this limit off.
        LOGIN_THROTTLE_RETENTION_HOURS: Age at which an unlocked throttle row may be removed.
        TRUSTED_PROXY_COUNT: Reverse proxies in front of the API. At 0 the forwarded header is
            ignored. A wrong value cannot be detected at start-up.
        HASH_CONCURRENCY: Password hashes that may run at once in one worker.
        HASH_QUEUE_LIMIT: Password hashes that may wait; beyond that a request is refused (503).
        LOGIN_LOCKOUT_MINUTES: How long a locked account stays locked.
        REFRESH_REUSE_GRACE_SECONDS: After a refresh token is used, a second use within this
            many seconds is refused without ending the user's sessions (an honest retry); a
            later one ends them all. Must be positive.
        CORS_ORIGINS: Explicit allowed origins; credentials are allowed, so ``*`` is rejected.
        MAIL_BACKEND: How messages are delivered. Defaults to ``outbox`` in a development
            environment; outside development it must name a real backend.
        MAIL_OUTBOX_PATH: File the ``outbox`` backend appends its redacted records to.

    Raises:
        ValueError: On construction (reported as a validation error that names the setting but
            never prints a setting's value): ``ENVIRONMENT`` or ``JWT_SECRET_KEY`` missing, or
            the secret empty;
            ``CORS_ORIGINS`` containing ``*``; outside development, a placeholder or too-short
            secret, or a missing or development-only mail backend.
    """

    ENVIRONMENT: str
    DATABASE_URL: str = "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds"
    JWT_SECRET_KEY: SecretStr
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_TTL_SECONDS: int = 900  # 15 minutes
    REFRESH_TOKEN_TTL_DAYS: int = 7
    LOGIN_MAX_FAILURES: int = 5
    LOGIN_LOCKOUT_MINUTES: int = 15
    REFRESH_REUSE_GRACE_SECONDS: int = Field(default=10, gt=0)
    LOGIN_RATE_LIMIT_PER_NETWORK: int = Field(default=30, ge=0)
    LOGIN_THROTTLE_RETENTION_HOURS: int = Field(default=24, gt=0)
    TRUSTED_PROXY_COUNT: int = Field(default=0, ge=0)
    HASH_CONCURRENCY: int = Field(default=2, ge=1)
    HASH_QUEUE_LIMIT: int = Field(default=8, ge=0)
    # JSON list in the environment, e.g. CORS_ORIGINS='["https://app.example.com"]'.
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:8081"]
    MAIL_BACKEND: str | None = None
    MAIL_OUTBOX_PATH: str = _DEFAULT_OUTBOX_PATH

    # hide_input_in_errors keeps the raw environment, secret included, out of the error text:
    # without it a failed start prints the whole input dictionary. extra="ignore" lets the one
    # settings file be shared with docker compose (POSTGRES_*) and the tests (TEST_DATABASE_URL);
    # the default, "forbid", made the documented copy-the-example step fail on those keys.
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "hide_input_in_errors": True,
        "extra": "ignore",
    }

    @property
    def is_development(self) -> bool:
        """Whether ``ENVIRONMENT`` is a development environment (``development`` or ``test``)."""
        return self.ENVIRONMENT.lower() in DEVELOPMENT_ENVIRONMENTS

    @field_validator("CORS_ORIGINS")
    @classmethod
    def _no_wildcard(cls, origins: list[str]) -> list[str]:
        # Credentials are allowed, and a wildcard origin must never be combined with them.
        if any(origin.strip() == "*" for origin in origins):
            raise ValueError("CORS_ORIGINS must list explicit origins; '*' is not allowed")
        return origins

    @model_validator(mode="after")
    def _refuse_unsafe_start(self) -> Self:
        if not self.JWT_SECRET_KEY.get_secret_value().strip():
            raise ValueError("JWT_SECRET_KEY must not be empty")
        if self.is_development:
            if self.MAIL_BACKEND is None:
                self.MAIL_BACKEND = OUTBOX_BACKEND
            if self.MAIL_BACKEND != OUTBOX_BACKEND and self.MAIL_BACKEND not in REAL_MAIL_BACKENDS:
                raise ValueError("MAIL_BACKEND does not name a known mail backend")
            return self
        secret = self.JWT_SECRET_KEY.get_secret_value()
        if secret == PUBLISHED_PLACEHOLDER:
            raise ValueError(
                "JWT_SECRET_KEY must be set to a real secret unless ENVIRONMENT is "
                "development or test"
            )
        if len(secret.encode()) < MIN_JWT_SECRET_BYTES:
            raise ValueError(
                f"JWT_SECRET_KEY must be at least {MIN_JWT_SECRET_BYTES} bytes "
                "unless ENVIRONMENT is development or test"
            )
        if self.MAIL_BACKEND not in REAL_MAIL_BACKENDS:
            raise ValueError(
                "MAIL_BACKEND must name a real mail backend unless ENVIRONMENT is development "
                "or test; the 'outbox' backend is for development only and no real backend is "
                "available yet"
            )
        return self


# ENVIRONMENT and JWT_SECRET_KEY are required and come from the environment, which the type
# checker cannot see; a missing one stops the process here with a validation error.
settings = Settings()  # type: ignore[call-arg]


def configure_email_validation(current: Settings) -> None:
    """Allow reserved test domains in development environments only.

    The seeded development accounts use ``@hds.test``; email-validator rejects that TLD unless
    told it is in a test environment. Every environment outside development keeps the strict
    check.

    Side effects:
        Sets the module-level ``email_validator.TEST_ENVIRONMENT`` flag.

    Args:
        current: The settings whose ``ENVIRONMENT`` decides the mode.
    """
    email_validator.TEST_ENVIRONMENT = current.is_development


def get_settings() -> Settings:
    """Return the process-wide settings.

    Exposed as a function so tests can override it through FastAPI's dependency overrides
    instead of patching the module global.

    Returns:
        The ``Settings`` instance built at import time.
    """
    return settings
