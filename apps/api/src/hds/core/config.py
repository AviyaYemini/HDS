"""Application settings read from the environment (and ``.env`` in development).

Layer: core. Imports nothing from other ``hds`` packages; every other layer may import it.

Secrets such as ``JWT_SECRET_KEY`` come only from the environment or a secret store and are
never committed (Constitution VI). Validators here fail the process at start-up rather than
letting an unsafe configuration run. ``ENVIRONMENT`` and ``JWT_SECRET_KEY`` have no defaults, so a
deployment that forgets them stops instead of starting with a published key. Only the
``development`` and ``test`` environments may use a weak secret or the development mail outbox;
every other value (``production``, ``staging``, ...) gets the strict checks, which also refuse any
key or database password published in this repository (``KNOWN_DEVELOPMENT_VALUES``).
"""

import hashlib
import hmac
import os
import re
from pathlib import Path
from typing import Self

import email_validator
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

# The placeholder value that older configuration files and examples published. It is public,
# not a secret: the settings use it only to REFUSE a signing key that equals it.
PUBLISHED_PLACEHOLDER = "dev-secret-change-in-production"
MIN_JWT_SECRET_BYTES = 32  # RFC 7518 §3.2: an HS256 key should be at least 256 bits

#: Every key and password value this repository publishes in a configuration file:
#: ``.env.example``, compose, the workflows, scripts, Makefiles, tool configs and the test settings
#: (specs/006 FR-026). They are not secrets, so outside development none may be the signing key, the
#: throttle key or the database password. Compared after trimming and lower-casing. A test reads
#: those files and fails when one of their values is missing here, so add any new example value at
#: the same time.
KNOWN_DEVELOPMENT_VALUES: frozenset[str] = frozenset(
    {
        PUBLISHED_PLACEHOLDER,
        "change-me",  # .env.example
        "hds_dev",  # infra/docker-compose.yml default, and the default DATABASE_URL below
        "ci-only-not-a-real-secret-0123456789",  # .github/workflows/ci.yml
        "hds_test",  # .github/workflows/ci.yml
        "hds_check",  # .github/workflows/ci.yml
        "hds_scratch",  # .github/workflows/ci.yml
        "hds_e2e",  # .github/workflows/ci.yml
        "test-only-jwt-secret-0123456789-abcdef",  # apps/api/tests/conftest.py
        "e2e-only-not-a-secret-0123456789-abcdefgh",  # apps/client/playwright.config.ts
        "hds",  # apps/api/scripts/contract_cli.sh (the scratch database's password)
        "unused-by-down",  # Makefile (the stand-in key for `make down`)
        "dev1234!",  # the development seed's password (seed_dev.py, quickstart_check.sh)
    }
)

#: The only environments allowed to use a weak secret and the development mail outbox.
DEVELOPMENT_ENVIRONMENTS = frozenset({"development", "test"})

#: The backend that delivers through an SMTP relay over STARTTLS (specs/006 research R9).
SMTP_BACKEND = "smtp"

#: Mail backends that really deliver messages; a non-development environment must use one
#: (specs/002 research D6). SMTP is the first (specs/006 research R9).
REAL_MAIL_BACKENDS: frozenset[str] = frozenset({SMTP_BACKEND})

#: The development-only backend that stores a redacted record of each message in a local file.
OUTBOX_BACKEND = "outbox"

# A sender is an address, bare or as ``Name <address>``; anything else would go out as the null
# sender ``MAIL FROM:<>`` (review R20).
_ADDRESS = r"[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+"
_SENDER = re.compile(rf"\s*(?:[^<>@]*<\s*{_ADDRESS}\s*>|{_ADDRESS})\s*")

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
        REFRESH_TOKEN_TTL_DAYS: Refresh-token lifetime before sign-in is required again, for a web
            session (the default kind).
        REFRESH_TOKEN_TTL_DAYS_NATIVE: The same for a phone session (``client_kind`` ``native``),
            renewed on every refresh; 30 days by the owner's choice (007 FR-010, ADR 016).
        LOGIN_MAX_FAILURES: Consecutive failed sign-ins before the account locks.
        THROTTLE_SECRET_KEY: Keys the HMAC of the addresses in the throttle table. Without it a key
            is derived from ``JWT_SECRET_KEY``. Changing either restarts every count.
        LOGIN_RATE_LIMIT_PER_NETWORK: Failed sign-ins allowed from one network address inside one
            window of ``LOGIN_LOCKOUT_MINUTES``; 0 turns this limit off.
        LOGIN_THROTTLE_RETENTION_HOURS: Age at which an unlocked throttle row may be removed.
        RESET_REQUEST_LIMIT_PER_ADDRESS: Password-reset requests one address may make inside one
            reset-link lifetime; 0 turns this limit off.
        RESET_REQUEST_LIMIT_PER_NETWORK: The same for one network address, whatever the addresses
            submitted; 0 turns this limit off.
        RESET_JOB_CONCURRENCY: Reset jobs that may run at once in one worker.
        RESET_JOB_QUEUE_LIMIT: Reset jobs that may wait; beyond that a job is dropped.
        RESET_JOB_TIMEOUT_SECONDS: How long one reset job may run before it is stopped.
        TRUSTED_PROXY_COUNT: Reverse proxies in front of the API. At 0 the forwarded header is
            ignored. A wrong value cannot be detected at start-up.
        HASH_CONCURRENCY: Password hashes that may run at once in one worker.
        HASH_QUEUE_LIMIT: Password hashes that may wait; beyond that a request is refused (503).
        SCHEDULE_TIME_LIMIT_SECONDS: How long a schedule job's solver may search (004 FR-010),
            1 to 60; the worker's 5-minute lease covers the longest.
        LOGIN_LOCKOUT_MINUTES: How long a locked account stays locked.
        REFRESH_REUSE_GRACE_SECONDS: After a refresh token is used, a second use within this
            many seconds is refused without ending the user's sessions (an honest retry); a
            later one ends them all. Must be positive.
        CORS_ORIGINS: Explicit allowed origins; credentials are allowed, so ``*`` is rejected.
        MAIL_BACKEND: How messages are delivered. Defaults to ``outbox`` in a development
            environment; outside development it must name a real backend.
        MAIL_OUTBOX_PATH: File the ``outbox`` backend appends its redacted records to.
        SMTP_HOST: The relay for ``MAIL_BACKEND=smtp``; required with it.
        SMTP_PORT: The relay's port, 587 (submission) by default.
        SMTP_USERNAME: The relay sign-in; none signs in without one.
        SMTP_PASSWORD: Its password, a secret: never logged, never in an error.
        MAIL_FROM: The sender address of every message; required with ``MAIL_BACKEND=smtp``.
        SMTP_STARTTLS: Whether the connection must be upgraded to TLS before anything is sent.
            It must stay on outside development; off only for a local relay in development.
        HDS_VERSION: The version this process runs, baked into the image by its build (the
            commit SHA); reported by the health checks and the ``hds_build_info`` metric.
        METRICS_PORT: The operations port serving the metrics (and, for the worker, its health
            checks), never the public port (specs/006 FR-009). 0, the default, serves none:
            compose and the chart set 9100, so a process started by hand never fights over it.

    Raises:
        ValueError: On construction (reported as a validation error that names the setting but
            never prints a setting's value): ``ENVIRONMENT`` or ``JWT_SECRET_KEY`` missing, or
            the secret empty;
            ``CORS_ORIGINS`` containing ``*``; ``MAIL_BACKEND=smtp`` without ``SMTP_HOST`` or
            ``MAIL_FROM``; outside development, a placeholder or too-short secret, a missing or
            development-only mail backend, or ``SMTP_STARTTLS`` off.
    """

    ENVIRONMENT: str
    DATABASE_URL: str = "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds"
    JWT_SECRET_KEY: SecretStr
    JWT_ALGORITHM: str = "HS256"
    THROTTLE_SECRET_KEY: SecretStr | None = None
    ACCESS_TOKEN_TTL_SECONDS: int = 900  # 15 minutes
    REFRESH_TOKEN_TTL_DAYS: int = 7
    REFRESH_TOKEN_TTL_DAYS_NATIVE: int = Field(default=30, gt=0)
    LOGIN_MAX_FAILURES: int = 5
    LOGIN_LOCKOUT_MINUTES: int = 15
    REFRESH_REUSE_GRACE_SECONDS: int = Field(default=10, gt=0)
    LOGIN_RATE_LIMIT_PER_NETWORK: int = Field(default=30, ge=0)
    LOGIN_THROTTLE_RETENTION_HOURS: int = Field(default=24, gt=0)
    RESET_REQUEST_LIMIT_PER_ADDRESS: int = Field(default=5, ge=0)
    RESET_REQUEST_LIMIT_PER_NETWORK: int = Field(default=30, ge=0)
    RESET_JOB_CONCURRENCY: int = Field(default=2, ge=1)
    RESET_JOB_QUEUE_LIMIT: int = Field(default=100, ge=0)
    RESET_JOB_TIMEOUT_SECONDS: int = Field(default=30, gt=0)
    TRUSTED_PROXY_COUNT: int = Field(default=0, ge=0)
    HASH_CONCURRENCY: int = Field(default=2, ge=1)
    HASH_QUEUE_LIMIT: int = Field(default=8, ge=0)
    SCHEDULE_TIME_LIMIT_SECONDS: int = Field(default=20, gt=0, le=60)
    # JSON list in the environment, e.g. CORS_ORIGINS='["https://app.example.com"]'.
    CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:8081"]
    MAIL_BACKEND: str | None = None
    MAIL_OUTBOX_PATH: str = _DEFAULT_OUTBOX_PATH
    SMTP_HOST: str | None = None
    SMTP_PORT: int = Field(default=587, gt=0, le=65535)
    SMTP_USERNAME: str | None = None
    SMTP_PASSWORD: SecretStr | None = None
    MAIL_FROM: str | None = None
    SMTP_STARTTLS: bool = True
    HDS_VERSION: str = "dev"
    METRICS_PORT: int = Field(default=0, ge=0, le=65535)

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

    @property
    def throttle_key(self) -> bytes:
        """The HMAC key for throttle rows: the setting, or one derived from the signing secret.

        The derivation keeps the signing secret itself out of a second use.
        """
        if self.THROTTLE_SECRET_KEY is not None:
            return self.THROTTLE_SECRET_KEY.get_secret_value().encode()
        signing = self.JWT_SECRET_KEY.get_secret_value().encode()
        return hmac.new(signing, b"hds.login-throttle", hashlib.sha256).digest()

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
            self._refuse_incomplete_smtp()
            return self
        # Also refuses PUBLISHED_PLACEHOLDER, which is on the list (compared ignoring case).
        self._refuse_published_values()
        secret = self.JWT_SECRET_KEY.get_secret_value()
        if len(secret.encode()) < MIN_JWT_SECRET_BYTES:
            raise ValueError(
                f"JWT_SECRET_KEY must be at least {MIN_JWT_SECRET_BYTES} bytes "
                "unless ENVIRONMENT is development or test"
            )
        if self.THROTTLE_SECRET_KEY is not None and len(self.throttle_key) < MIN_JWT_SECRET_BYTES:
            raise ValueError(
                f"THROTTLE_SECRET_KEY must be at least {MIN_JWT_SECRET_BYTES} bytes "
                "unless ENVIRONMENT is development or test"
            )
        if self.MAIL_BACKEND not in REAL_MAIL_BACKENDS:
            raise ValueError(
                "MAIL_BACKEND must name a real mail backend (smtp) unless ENVIRONMENT is "
                "development or test; the 'outbox' backend is for development only"
            )
        self._refuse_incomplete_smtp()
        if self.MAIL_BACKEND == SMTP_BACKEND and not self.SMTP_STARTTLS:
            raise ValueError(
                "SMTP_STARTTLS must be true unless ENVIRONMENT is development or test: mail "
                "carries account links and must not cross the network in clear"
            )
        return self

    def _refuse_incomplete_smtp(self) -> None:
        """Refuse ``MAIL_BACKEND=smtp`` without its host, with no sender address, or half a sign-in.

        Raises:
            ValueError: Naming the setting at fault, never its value.
        """
        if self.MAIL_BACKEND != SMTP_BACKEND:
            return
        for name, value in (("SMTP_HOST", self.SMTP_HOST), ("MAIL_FROM", self.MAIL_FROM)):
            if not (value or "").strip():
                raise ValueError(f"{name} must be set when MAIL_BACKEND is smtp")
        if not _SENDER.fullmatch(self.MAIL_FROM or ""):
            raise ValueError("MAIL_FROM must be an address, bare or as 'Name <address>'")
        has_password = self.SMTP_PASSWORD is not None and self.SMTP_PASSWORD.get_secret_value()
        if bool(self.SMTP_USERNAME) != bool(has_password):
            raise ValueError(
                "SMTP_USERNAME and SMTP_PASSWORD must be set together, or neither for a relay "
                "that needs no sign-in"
            )

    def _refuse_published_values(self) -> None:
        """Refuse a key or database password that this repository publishes (specs/006 FR-026).

        The database password is read wherever the driver takes one: the URL's user part, its
        ``password`` query option, and ``PGPASSWORD`` when the URL has none.

        Raises:
            ValueError: Naming the setting, never the value; also when ``DATABASE_URL`` cannot be
                read, since its password cannot be checked then.
        """
        try:
            url = make_url(self.DATABASE_URL)
        except ArgumentError:
            raise ValueError("DATABASE_URL is not a database URL SQLAlchemy can read") from None
        option = url.query.get("password")
        candidates: list[tuple[str, str | tuple[str, ...] | None]] = [
            ("JWT_SECRET_KEY", self.JWT_SECRET_KEY.get_secret_value()),
            (
                "THROTTLE_SECRET_KEY",
                self.THROTTLE_SECRET_KEY.get_secret_value() if self.THROTTLE_SECRET_KEY else None,
            ),
            ("DATABASE_URL", url.password),
            ("DATABASE_URL", option),
            ("PGPASSWORD", os.environ.get("PGPASSWORD")),
            (
                "SMTP_PASSWORD",
                self.SMTP_PASSWORD.get_secret_value() if self.SMTP_PASSWORD else None,
            ),
        ]
        for name, found in candidates:
            for value in (found,) if isinstance(found, str) or found is None else found:
                if value is not None and value.strip().lower() in KNOWN_DEVELOPMENT_VALUES:
                    raise ValueError(
                        f"{name} must not use a value published in this repository unless "
                        "ENVIRONMENT is development or test; set a value of your own"
                    )


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
