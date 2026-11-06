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
from tests.settings_support import make_settings

ROOT = Path(__file__).resolve().parents[4]
COMPOSE = ROOT / "infra" / "docker-compose.yml"
EXAMPLE = ROOT / ".env.example"


@pytest.fixture
def api_environment() -> dict[str, str]:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    environment: dict[str, str] = compose["services"]["api"]["environment"]
    return environment


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
        make_settings(_env_file=EXAMPLE)
    assert "JWT_SECRET_KEY" in str(caught.value)


def test_the_example_starts_once_a_secret_is_generated(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ENVIRONMENT", "JWT_SECRET_KEY", "MAIL_BACKEND", "MAIL_OUTBOX_PATH"):
        monkeypatch.delenv(name, raising=False)
    settings = make_settings(
        _env_file=EXAMPLE, JWT_SECRET_KEY="a-generated-secret-0123456789abcdef"
    )
    assert settings.is_development


API_README = ROOT / "apps" / "api" / "README.md"


def test_example_settings_set_every_required_setting() -> None:
    """A setting with no default must be an active line in the example, so copying it is enough."""
    required = {name for name, field in Settings.model_fields.items() if field.is_required()}
    assert {"ENVIRONMENT", "JWT_SECRET_KEY"} <= required
    assert required <= set(dotenv_values(EXAMPLE)), "a required setting is missing from the example"


def test_example_settings_and_the_readme_mention_every_setting() -> None:
    example = EXAMPLE.read_text(encoding="utf-8")
    readme = API_README.read_text(encoding="utf-8")
    for name in Settings.model_fields:
        assert name in example, f"{name} is not in .env.example (not even as a comment)"
        assert f"`{name}`" in readme, f"{name} is not in the configuration table of apps/api/README"


def test_the_database_is_reported_healthy_only_once_it_takes_tcp() -> None:
    # T032a (PR 9's review): on the first start the image's set-up server answers on the socket
    # only, so a socket probe let `migrate` start before the real server listened.
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    probe = " ".join(compose["services"]["postgres"]["healthcheck"]["test"])
    assert "pg_isready" in probe
    assert "-h 127.0.0.1" in probe


def test_the_worker_has_time_to_finish_a_solve_when_compose_stops_it() -> None:
    # Review R36: Docker's default 10 s is shorter than a solve (up to 60 s) plus storing; the
    # chart gives 90 s, and compose matches it.
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["services"]["worker"]["stop_grace_period"] == "90s"
