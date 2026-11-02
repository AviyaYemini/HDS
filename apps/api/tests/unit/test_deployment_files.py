"""The compose file and the example settings ship no usable secret (T230, T231, FR-204).

A deployment that forgets to set its environment or signing key must stop, not start with a key
that is published in this repository. These tests read the files as a new contributor would find
them and check what happens if they are used unchanged.

Covers: FR-201, FR-204.
"""

from pathlib import Path

import pytest
import yaml
from dotenv import dotenv_values
from pydantic import ValidationError

from hds.core.config import PUBLISHED_PLACEHOLDER, Settings

ROOT = Path(__file__).resolve().parents[4]
COMPOSE = ROOT / "infra" / "docker-compose.yml"
EXAMPLE = ROOT / ".env.example"


@pytest.fixture
def api_environment() -> dict[str, str]:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]["api"]["environment"]


def test_compose_sets_the_environment_explicitly(api_environment: dict[str, str]) -> None:
    assert api_environment["ENVIRONMENT"] == "development"


def test_compose_requires_the_signing_secret_and_gives_no_default(
    api_environment: dict[str, str],
) -> None:
    value = api_environment["JWT_SECRET_KEY"]
    assert ":?" in value, "compose must fail when the secret is unset or empty"
    assert ":-" not in value, "compose must not fall back to a default secret"


def test_compose_does_not_contain_the_published_placeholder() -> None:
    assert PUBLISHED_PLACEHOLDER not in COMPOSE.read_text(encoding="utf-8")


def test_example_settings_name_the_environment_and_leave_the_secret_empty() -> None:
    values = dotenv_values(EXAMPLE)
    assert values["ENVIRONMENT"] == "development"
    assert values["JWT_SECRET_KEY"] == ""  # nothing usable is published


def test_example_settings_publish_no_known_secret() -> None:
    assert PUBLISHED_PLACEHOLDER not in EXAMPLE.read_text(encoding="utf-8")


def test_copying_the_example_unchanged_does_not_start(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ENVIRONMENT", "JWT_SECRET_KEY", "MAIL_BACKEND", "MAIL_OUTBOX_PATH"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValidationError) as caught:
        Settings(_env_file=EXAMPLE)
    assert "JWT_SECRET_KEY" in str(caught.value)


def test_the_example_starts_once_a_secret_is_generated(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ENVIRONMENT", "JWT_SECRET_KEY", "MAIL_BACKEND", "MAIL_OUTBOX_PATH"):
        monkeypatch.delenv(name, raising=False)
    settings = Settings(_env_file=EXAMPLE, JWT_SECRET_KEY="a-generated-secret-0123456789abcdef")
    assert settings.is_development
