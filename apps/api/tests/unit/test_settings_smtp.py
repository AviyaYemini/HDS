"""A production start with the SMTP backend needs its host and sender, and STARTTLS (006 T025).

``smtp`` is now a real mail backend, so a non-development environment can start with it. Without
``SMTP_HOST`` or ``MAIL_FROM`` it is refused, as with STARTTLS switched off outside development, and
the error names the setting and never prints a value (contracts/ops.md section 4, FR-026).

Covers: 006 research R9; contracts/ops.md section 4.
"""

import pytest
from pydantic import ValidationError

from hds.core.config import KNOWN_DEVELOPMENT_VALUES, REAL_MAIL_BACKENDS, Settings
from tests.settings_support import make_settings

pytestmark = pytest.mark.usefixtures("distinct_database_password")

STRONG = "strong-distinct-secret-value-0123456789abcdef"
RELAY_PASSWORD = "relay-password-4b1e9c2a7d"
SMTP_SETTINGS = (
    "MAIL_BACKEND", "SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "MAIL_FROM",
    "SMTP_STARTTLS", "ENVIRONMENT", "JWT_SECRET_KEY", "MAIL_OUTBOX_PATH",
)  # fmt: skip
PRODUCTION = {
    "ENVIRONMENT": "production",
    "JWT_SECRET_KEY": STRONG,
    "MAIL_BACKEND": "smtp",
    "SMTP_HOST": "smtp.example.org",
    "MAIL_FROM": "HDS <no-reply@example.org>",
    "SMTP_USERNAME": "hds",
    "SMTP_PASSWORD": RELAY_PASSWORD,
}


def build(monkeypatch: pytest.MonkeyPatch, **env: str) -> Settings:
    for name in SMTP_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return make_settings(_env_file=None)


def refusal(monkeypatch: pytest.MonkeyPatch, **env: str) -> str:
    with pytest.raises(ValidationError) as caught:
        build(monkeypatch, **env)
    return f"{caught.value!s} {caught.value!r}"


def without(name: str) -> dict[str, str]:
    return {k: v for k, v in PRODUCTION.items() if k != name}


def test_smtp_is_a_real_backend() -> None:
    assert "smtp" in REAL_MAIL_BACKENDS


def test_production_starts_with_smtp_when_its_host_and_sender_are_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build(monkeypatch, **PRODUCTION)
    assert settings.MAIL_BACKEND == "smtp"
    assert settings.SMTP_PORT == 587
    assert settings.SMTP_STARTTLS is True
    assert settings.SMTP_PASSWORD is not None
    assert RELAY_PASSWORD not in repr(settings)  # a SecretStr


@pytest.mark.parametrize("missing", ["SMTP_HOST", "MAIL_FROM"])
def test_production_with_smtp_is_refused_without_its_host_or_sender(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    text = refusal(monkeypatch, **without(missing))
    assert missing in text
    assert RELAY_PASSWORD not in text


def test_production_with_starttls_off_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    text = refusal(monkeypatch, **PRODUCTION, SMTP_STARTTLS="false")
    assert "SMTP_STARTTLS" in text
    assert RELAY_PASSWORD not in text


def test_development_may_switch_starttls_off_for_a_local_relay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = build(monkeypatch, **{**PRODUCTION, "ENVIRONMENT": "development"}, SMTP_STARTTLS="0")
    assert settings.SMTP_STARTTLS is False


def test_a_published_relay_password_is_refused_outside_development(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = "dev1234!"  # the development seed's password
    assert published in KNOWN_DEVELOPMENT_VALUES
    text = refusal(monkeypatch, **{**PRODUCTION, "SMTP_PASSWORD": published})
    assert "SMTP_PASSWORD" in text
    assert published not in text.lower()


@pytest.mark.parametrize("sender", ["HDS", "HDS no-reply@example.org", "no-reply@", "<>"])
def test_a_sender_with_no_address_is_refused(monkeypatch: pytest.MonkeyPatch, sender: str) -> None:
    # Review R20: otherwise every message goes out with the null sender, MAIL FROM:<>.
    text = refusal(monkeypatch, **{**PRODUCTION, "MAIL_FROM": sender})
    assert "MAIL_FROM" in text


@pytest.mark.parametrize("missing", ["SMTP_USERNAME", "SMTP_PASSWORD"])
def test_half_a_sign_in_is_refused(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    # Review R21: a user name with no password, or a password with no user name.
    text = refusal(monkeypatch, **without(missing))
    assert "SMTP_USERNAME" in text and "SMTP_PASSWORD" in text
    assert RELAY_PASSWORD not in text


def test_a_relay_without_a_sign_in_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {k: v for k, v in PRODUCTION.items() if k not in ("SMTP_USERNAME", "SMTP_PASSWORD")}
    assert build(monkeypatch, **env).SMTP_USERNAME is None
