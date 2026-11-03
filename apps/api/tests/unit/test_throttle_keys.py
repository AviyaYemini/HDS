"""Throttle keys cannot be recomputed from the database alone (decision of 2026-11-03).

A plain SHA-256 of an address is easy to confirm: anyone with a copy of the table can hash a list of
addresses and look them up. An HMAC needs the server secret too. Without the secret the table says
nothing about who failed to sign in; with it, a known address can still be checked, so the keys are
protected, not anonymous.

Covers: FR-221.
"""

import hashlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from hds.services.throttle import SignInThrottle, hash_key
from tests.settings_support import make_settings

ADDRESS = "someone@hds.test"


def test_the_key_is_not_a_plain_hash_of_the_value() -> None:
    assert hash_key(ADDRESS, b"secret") != hashlib.sha256(ADDRESS.encode()).hexdigest()


def test_the_key_is_stable_and_fits_the_column() -> None:
    assert hash_key(ADDRESS, b"secret") == hash_key(ADDRESS, b"secret")
    assert len(hash_key(ADDRESS, b"secret")) == 64


def test_a_different_secret_gives_a_different_key() -> None:
    assert hash_key(ADDRESS, b"secret") != hash_key(ADDRESS, b"other")


def test_the_configured_secret_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THROTTLE_SECRET_KEY", "a-dedicated-throttle-secret-0123456789")
    settings = make_settings()
    assert settings.throttle_key == b"a-dedicated-throttle-secret-0123456789"


def test_without_a_setting_the_key_is_derived_and_never_the_signing_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("THROTTLE_SECRET_KEY", raising=False)
    settings = make_settings(JWT_SECRET_KEY=SecretStr("a-signing-secret-0123456789-abcdefgh"))
    assert settings.throttle_key != b"a-signing-secret-0123456789-abcdefgh"
    assert len(settings.throttle_key) == 32
    other = make_settings(JWT_SECRET_KEY=SecretStr("another-signing-secret-0123456789-ab"))
    assert settings.throttle_key != other.throttle_key


def test_the_secret_never_appears_in_the_settings_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THROTTLE_SECRET_KEY", "a-dedicated-throttle-secret-0123456789")
    assert "a-dedicated-throttle-secret" not in repr(make_settings())


@pytest.mark.parametrize("environment", ["production", "staging"])
def test_a_short_secret_is_refused_outside_development(
    environment: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ENVIRONMENT", environment)
    monkeypatch.setenv("JWT_SECRET_KEY", "x" * 48)
    monkeypatch.setenv("MAIL_BACKEND", "outbox")
    monkeypatch.setenv("THROTTLE_SECRET_KEY", "short")
    with pytest.raises(ValueError, match="THROTTLE_SECRET_KEY"):
        make_settings()


async def test_an_address_is_counted_under_its_folded_form() -> None:
    """Over HTTP the request type folds case already; the throttle must not depend on that."""
    repo = AsyncMock()
    repo.get = AsyncMock(return_value=None)
    config = MagicMock(throttle_key=b"k" * 32)

    await SignInThrottle(repo, config).address_locked_until(" Someone@HDS.test ", datetime.now(UTC))

    assert repo.get.await_args.args == ("address", hash_key("someone@hds.test", b"k" * 32))
