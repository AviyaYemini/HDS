"""The settings refuse an unsafe start-up (T217, specs/002 research D5 and D6).

``ENVIRONMENT`` and ``JWT_SECRET_KEY`` have no defaults. Only ``development`` and ``test`` may use
a weak secret; every other environment needs a real secret and a real mail backend. Whatever
goes wrong, the error names the setting and never prints a secret.

Covers: FR-201, FR-202, FR-204.
"""

import pytest
from pydantic import ValidationError

from hds.core.config import PUBLISHED_PLACEHOLDER, Settings

STRONG = "strong-distinct-secret-value-0123456789abcdef"  # 44 bytes, not the placeholder
SHORT = "short-distinct-secret"  # 21 bytes
STRICT_ENVIRONMENTS = ["production", "staging", "qa", "PRODUCTION"]


def build(monkeypatch: pytest.MonkeyPatch, **env: str) -> Settings:
    """Build settings from exactly the given environment variables (no ``.env`` file)."""
    for name in ("ENVIRONMENT", "JWT_SECRET_KEY", "MAIL_BACKEND", "MAIL_OUTBOX_PATH"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return Settings(_env_file=None)


def refusal(monkeypatch: pytest.MonkeyPatch, **env: str) -> str:
    """Build settings that must be refused and return the full error text."""
    with pytest.raises(ValidationError) as caught:
        build(monkeypatch, **env)
    return f"{caught.value!s} {caught.value!r}"


def test_environment_must_be_set(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "ENVIRONMENT" in refusal(monkeypatch, JWT_SECRET_KEY=STRONG)


def test_secret_must_be_set(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "JWT_SECRET_KEY" in refusal(monkeypatch, ENVIRONMENT="development")


def test_development_environments_are_exactly_development_and_test() -> None:
    from hds.core.config import DEVELOPMENT_ENVIRONMENTS

    assert DEVELOPMENT_ENVIRONMENTS == frozenset({"development", "test"})


@pytest.mark.parametrize("environment", ["development", "test"])
@pytest.mark.parametrize("secret", [PUBLISHED_PLACEHOLDER, SHORT])
def test_development_and_test_accept_a_weak_secret(
    monkeypatch: pytest.MonkeyPatch, environment: str, secret: str
) -> None:
    settings = build(monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY=secret)
    assert settings.JWT_SECRET_KEY.get_secret_value() == secret


@pytest.mark.parametrize("environment", ["development", "test"])
def test_development_defaults_to_the_outbox_mail_backend(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    settings = build(monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY=SHORT)
    assert settings.MAIL_BACKEND == "outbox"


@pytest.mark.parametrize("environment", STRICT_ENVIRONMENTS)
def test_strict_environments_reject_the_placeholder_secret(
    monkeypatch: pytest.MonkeyPatch, environment: str, real_mail_backend: None
) -> None:
    text = refusal(monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY=PUBLISHED_PLACEHOLDER)
    assert "JWT_SECRET_KEY" in text


@pytest.mark.parametrize("environment", STRICT_ENVIRONMENTS)
def test_strict_environments_reject_a_secret_shorter_than_32_bytes(
    monkeypatch: pytest.MonkeyPatch, environment: str, real_mail_backend: None
) -> None:
    text = refusal(monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY="x" * 31)
    assert "JWT_SECRET_KEY" in text and "32" in text


@pytest.mark.parametrize("environment", STRICT_ENVIRONMENTS)
def test_strict_environments_start_with_a_strong_secret_and_a_real_backend(
    monkeypatch: pytest.MonkeyPatch, environment: str, real_mail_backend: None
) -> None:
    settings = build(
        monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY=STRONG, MAIL_BACKEND="test-smtp"
    )
    assert settings.MAIL_BACKEND == "test-smtp"


@pytest.mark.parametrize("environment", STRICT_ENVIRONMENTS)
def test_strict_environments_refuse_to_start_without_a_mail_backend(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    text = refusal(monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY=STRONG)
    assert "MAIL_BACKEND" in text


@pytest.mark.parametrize("environment", STRICT_ENVIRONMENTS)
def test_strict_environments_refuse_the_development_only_outbox(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    text = refusal(
        monkeypatch, ENVIRONMENT=environment, JWT_SECRET_KEY=STRONG, MAIL_BACKEND="outbox"
    )
    assert "MAIL_BACKEND" in text


@pytest.mark.parametrize(
    "env",
    [
        {"JWT_SECRET_KEY": STRONG},  # ENVIRONMENT missing
        {"ENVIRONMENT": "production", "JWT_SECRET_KEY": SHORT},  # secret too short
        {"ENVIRONMENT": "production", "JWT_SECRET_KEY": STRONG},  # no mail backend
        {"ENVIRONMENT": "staging", "JWT_SECRET_KEY": STRONG, "MAIL_BACKEND": "outbox"},
    ],
    ids=["no-environment", "short-secret", "no-backend", "outbox-in-staging"],
)
def test_error_text_never_contains_the_secret(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str]
) -> None:
    text = refusal(monkeypatch, **env)
    assert env["JWT_SECRET_KEY"] not in text
