"""Outside development, the settings refuse every secret value the repository publishes (T003).

A shared environment must never start with a key or password that anyone can read in this
repository's configuration files: ``.env.example``, compose, the workflows, scripts, Makefiles, tool
configs and the test settings. The check compares after trimming and ignoring case, and covers
``JWT_SECRET_KEY``, ``THROTTLE_SECRET_KEY`` and the database password wherever the driver reads one
(the URL, its ``password`` option, ``PGPASSWORD``). It names the setting, never the value. A second
test reads those files, so a value added there later cannot slip past the list.

Covers: specs/006-deploy-operations FR-026 (and FR-024).
"""

import re
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import make_url

from hds.core.config import KNOWN_DEVELOPMENT_VALUES, Settings
from tests.settings_support import make_settings

ROOT = Path(__file__).resolve().parents[4]
# Every tracked configuration file: workflows, compose, scripts, ini and toml files, Makefiles,
# Dockerfiles, JavaScript tool configs, the env example and the API's test settings. Test bodies,
# docs and source code are left out: they hold made-up values that tests use as strong secrets.
_CONFIGURATION = re.compile(
    r"\.(ya?ml|sh|ini|toml)$|(^|/)\.env\.example$|(^|/)Makefile$|(^|/)Dockerfile[^/]*$"
    r"|\.config\.(ts|js|mjs)$|^apps/api/tests/(conftest|run_database)\.py$"
)
PUBLISHING_FILES = [
    ROOT / name
    for name in subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    if _CONFIGURATION.search(name) and (ROOT / name).is_file()
]
STRONG = "strong-distinct-secret-value-0123456789abcdef"
STRONG_URL = "postgresql+asyncpg://hds:a-strong-distinct-db-password-0123@db:5432/hds"
SAMPLES = ["hds_dev", "hds_scratch", "hds_e2e", "change-me", "ci-only-not-a-real-secret-0123456789"]


def build(monkeypatch: pytest.MonkeyPatch, **env: str) -> Settings:
    """Build settings from exactly the given environment (no ``.env`` file)."""
    for name in (
        "ENVIRONMENT",
        "JWT_SECRET_KEY",
        "THROTTLE_SECRET_KEY",
        "DATABASE_URL",
        "PGPASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return make_settings(_env_file=None)


def refusal(monkeypatch: pytest.MonkeyPatch, **env: str) -> str:
    """Build settings that must be refused; return the whole error text."""
    with pytest.raises(ValidationError) as caught:
        build(monkeypatch, **env)
    return f"{caught.value!s} {caught.value!r}"


def url_with(password: str) -> str:
    """A database URL whose password is ``password``."""
    return f"postgresql+asyncpg://hds:{password}@db:5432/hds"


@pytest.mark.parametrize("value", SAMPLES)
def test_a_published_value_is_refused_as_the_signing_key(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None, value: str
) -> None:
    text = refusal(
        monkeypatch, ENVIRONMENT="production", JWT_SECRET_KEY=value, DATABASE_URL=STRONG_URL
    )
    assert "JWT_SECRET_KEY" in text
    assert "published" in text
    assert value not in text


@pytest.mark.parametrize("value", SAMPLES)
def test_a_published_value_is_refused_as_the_database_password(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None, value: str
) -> None:
    text = refusal(
        monkeypatch, ENVIRONMENT="production", JWT_SECRET_KEY=STRONG, DATABASE_URL=url_with(value)
    )
    assert "DATABASE_URL" in text
    assert value not in text


def test_a_published_value_is_refused_as_the_throttle_key(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None
) -> None:
    value = "ci-only-not-a-real-secret-0123456789"
    text = refusal(
        monkeypatch,
        ENVIRONMENT="production",
        JWT_SECRET_KEY=STRONG,
        THROTTLE_SECRET_KEY=value,
        DATABASE_URL=STRONG_URL,
    )
    assert "THROTTLE_SECRET_KEY" in text
    assert value not in text


@pytest.mark.parametrize(
    "variant", ["  CI-ONLY-NOT-A-REAL-SECRET-0123456789 ", "Ci-Only-Not-A-Real-Secret-0123456789"]
)
def test_case_and_surrounding_space_do_not_hide_a_published_value(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None, variant: str
) -> None:
    text = refusal(
        monkeypatch, ENVIRONMENT="production", JWT_SECRET_KEY=variant, DATABASE_URL=STRONG_URL
    )
    assert "JWT_SECRET_KEY" in text
    assert variant.strip() not in text


def test_a_distinct_secret_and_password_start_in_production(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None
) -> None:
    settings = build(
        monkeypatch, ENVIRONMENT="production", JWT_SECRET_KEY=STRONG, DATABASE_URL=STRONG_URL
    )
    assert settings.ENVIRONMENT == "production"


@pytest.mark.parametrize("environment", ["development", "test"])
def test_development_environments_still_start_with_published_values(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    settings = build(
        monkeypatch,
        ENVIRONMENT=environment,
        JWT_SECRET_KEY="ci-only-not-a-real-secret-0123456789",
        DATABASE_URL=url_with("hds_dev"),
    )
    assert settings.ENVIRONMENT == environment


def test_a_published_password_passed_as_a_url_option_is_refused(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None
) -> None:
    url = "postgresql+asyncpg://hds@db:5432/hds?password=hds_dev"
    text = refusal(monkeypatch, ENVIRONMENT="production", JWT_SECRET_KEY=STRONG, DATABASE_URL=url)
    assert "DATABASE_URL" in text
    assert "hds_dev" not in text


def test_a_published_password_in_pgpassword_is_refused(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None
) -> None:
    text = refusal(
        monkeypatch,
        ENVIRONMENT="production",
        JWT_SECRET_KEY=STRONG,
        DATABASE_URL="postgresql+asyncpg://hds@db:5432/hds",
        PGPASSWORD="hds_dev",
    )
    assert "PGPASSWORD" in text
    assert "hds_dev" not in text


def test_an_unreadable_database_url_is_a_validation_error_naming_it(
    monkeypatch: pytest.MonkeyPatch, real_mail_backend: None
) -> None:
    text = refusal(
        monkeypatch,
        ENVIRONMENT="production",
        JWT_SECRET_KEY=STRONG,
        DATABASE_URL="postgres:/hds:pw@db/hds",
    )
    assert "DATABASE_URL" in text
    assert "pw" not in text.replace("password", "")


_ASSIGNMENT = re.compile(
    r"""(?P<name>\w*(?:PASSWORD|SECRET_KEY)\w*)["']?\s*[:=]\s*["']?(?P<value>[^\s"'}$#)]+)"""
)
_DEFAULT = re.compile(r"""\$\{\w*(?:PASSWORD|SECRET_KEY)\w*:-(?P<value>[^}]+)\}""")
# A quoted name and value passed as two arguments, as in os.environ.setdefault("NAME", "value").
_ARGUMENTS = re.compile(
    r"""["']\w*(?:PASSWORD|SECRET_KEY)\w*["']\s*,\s*["'](?P<value>[^"']+)["']"""
)
_EXPRESSION = re.compile(r"""\.\*|\[[^\]]*\]|\{\d|%s""")
_EXPANSION = re.compile(r"""\$\{[^}]*\}""")
_URL = re.compile(r"""[a-z+]+://[^:/@\s]+:(?P<value>[^@\s$]+)@""")


def published_values() -> set[str]:
    """Every password and key value written in the publishing files."""
    found: set[str] = set()
    for path in PUBLISHING_FILES:
        for line in path.read_text().splitlines():
            if line.lstrip().startswith("#") and "://" not in line:
                continue
            found.update(m["value"] for m in _DEFAULT.finditer(line))
            found.update(m["value"] for m in _ARGUMENTS.finditer(line))
            # Compose's ${VAR:-default} and ${VAR:?message} are read above, not as values.
            plain = _EXPANSION.sub("", line)
            found.update(m["value"] for m in _ASSIGNMENT.finditer(plain))
            for m in _URL.finditer(line):
                found.add(make_url(m[0] + "h/d").password or "")
    # A sed or regex pattern (``.*``, ``[0-9a-f]{64}``) or a format (``%s``) is an expression in a
    # script, not a published example value; every other value is checked, whatever it starts with.
    return {v for v in found if v and not v.startswith("$") and not _EXPRESSION.search(v)}


def test_every_value_the_repository_publishes_is_on_the_refused_list() -> None:
    values = published_values()
    assert {
        "hds_dev",
        "change-me",
        "ci-only-not-a-real-secret-0123456789",
        "test-only-jwt-secret-0123456789-abcdef",
        "e2e-only-not-a-secret-0123456789-abcdefgh",
        "hds",
    } <= values
    missing = {v for v in values if v.strip().lower() not in KNOWN_DEVELOPMENT_VALUES}
    assert not missing, f"add these to KNOWN_DEVELOPMENT_VALUES: {sorted(missing)}"


def test_every_listed_value_is_already_in_its_compared_form() -> None:
    # Values are compared after strip() and lower(); a capital or a space would never match.
    assert all(value == value.strip().lower() for value in KNOWN_DEVELOPMENT_VALUES)


def test_the_reader_skips_script_patterns_but_not_values_starting_with_punctuation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sample = tmp_path / "sample.sh"
    sample.write_text(
        "POSTGRES_PASSWORD=_hds_local\n"
        'sed "s/^JWT_SECRET_KEY=.*/JWT_SECRET_KEY=$key/"\n'
        "grep -Eq '^JWT_SECRET_KEY=[0-9a-f]{64}$' file\n"
    )
    monkeypatch.setattr("tests.unit.test_settings_known_values.PUBLISHING_FILES", [sample])
    assert published_values() == {"_hds_local"}
