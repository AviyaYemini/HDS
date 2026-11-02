"""T138 (CORS origins) and T139 (production refuses the placeholder JWT secret)."""

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError

from hds.core.config import PLACEHOLDER_JWT_SECRET, Settings
from hds.main import create_app


def test_default_cors_origins() -> None:
    assert Settings().CORS_ORIGINS == ["http://localhost:3000", "http://localhost:8081"]


def test_cors_origins_read_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CORS_ORIGINS", '["https://app.example.com"]')
    assert Settings().CORS_ORIGINS == ["https://app.example.com"]


def test_wildcard_origin_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CORS_ORIGINS", '["*"]')
    with pytest.raises(ValidationError):
        Settings()


def _preflight(origin: str) -> dict[str, str]:
    client = TestClient(create_app())
    response = client.options(
        "/health",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )
    return dict(response.headers)


def test_allowed_origin_gets_cors_headers() -> None:
    headers = _preflight("http://localhost:3000")
    assert headers.get("access-control-allow-origin") == "http://localhost:3000"
    assert headers.get("access-control-allow-credentials") == "true"


def test_unknown_origin_gets_no_cors_headers() -> None:
    headers = _preflight("https://evil.example.com")
    assert "access-control-allow-origin" not in headers


def test_production_with_placeholder_secret_refuses_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("JWT_SECRET_KEY", PLACEHOLDER_JWT_SECRET)
    with pytest.raises(ValidationError, match="JWT_SECRET_KEY"):
        Settings()


def test_production_with_real_secret_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("JWT_SECRET_KEY", "x" * 48)
    assert Settings().JWT_SECRET_KEY == SecretStr("x" * 48)


def test_development_with_placeholder_secret_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("JWT_SECRET_KEY", PLACEHOLDER_JWT_SECRET)
    Settings()


def test_reserved_email_domains_allowed_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The seeded `*@hds.test` accounts must be able to sign in on a development server."""
    import email_validator

    from hds.core.config import configure_email_validation

    monkeypatch.setattr(email_validator, "TEST_ENVIRONMENT", False)
    configure_email_validation(Settings(ENVIRONMENT="development"))
    assert email_validator.TEST_ENVIRONMENT is True


def test_reserved_email_domains_rejected_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import email_validator

    from hds.core.config import configure_email_validation

    monkeypatch.setattr(email_validator, "TEST_ENVIRONMENT", False)
    configure_email_validation(
        Settings(ENVIRONMENT="production", JWT_SECRET_KEY=SecretStr("x" * 48))
    )
    assert email_validator.TEST_ENVIRONMENT is False


@pytest.mark.parametrize("secret", ["short", "x" * 31])
def test_production_refuses_a_secret_shorter_than_32_bytes(
    monkeypatch: pytest.MonkeyPatch, secret: str
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("JWT_SECRET_KEY", secret)
    with pytest.raises(ValidationError, match="32"):
        Settings()


def test_production_accepts_exactly_32_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("JWT_SECRET_KEY", "x" * 32)
    Settings()


def test_multibyte_secret_is_measured_in_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("JWT_SECRET_KEY", "ש" * 16)  # 16 characters, 32 UTF-8 bytes
    Settings()
