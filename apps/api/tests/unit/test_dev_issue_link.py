"""The development-only script that shows a working link once (T221, research D6).

The mail outbox stores no usable token, so a developer testing by hand needs another way to get
one. The script issues a fresh token for an account and prints it to the terminal once. It must
refuse to run outside a development environment and must leave the token in no file and no log.

Covers: FR-203, FR-235.
"""

import hashlib
import importlib
import logging
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from hds.core.security import hash_password
from hds.models.base import Base
from hds.models.invitation_token import InvitationToken
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.invitation import InvitationService

Factory = async_sessionmaker[AsyncSession]


@pytest.fixture
def script() -> Any:
    return importlib.import_module("hds.scripts.dev_issue_link")


@pytest_asyncio.fixture
async def factory() -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    engine = create_async_engine(
        "sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        org = Organisation(id=uuid.uuid4(), name="Dev Org", timezone="UTC")
        session.add(org)
        accounts = (
            ("pending@hds.test", False, None, False),
            ("active@hds.test", True, datetime.now(UTC), False),
            ("deactivated@hds.test", False, datetime.now(UTC), False),
            ("deleted@hds.test", False, None, True),
        )
        for email, active, activated_at, deleted in accounts:
            session.add(
                User(
                    id=uuid.uuid4(),
                    organisation_id=org.id,
                    email=email,
                    password_hash=hash_password("Dev1234!"),
                    full_name=email,
                    role=UserRole.employee,
                    is_active=active,
                    is_deleted=deleted,
                    activated_at=activated_at,
                )
            )
        await session.commit()
    yield maker
    await engine.dispose()


def printed_token(out: str) -> str:
    return out.strip().split()[-1]


async def stored(factory: Factory, token: str) -> Any:
    async with factory() as session:
        return await InvitationTokenRepository(session).get_by_hash(
            hashlib.sha256(token.encode()).hexdigest()
        )


@pytest.mark.parametrize(
    "environment",
    ["production", "Production", "PRODUCTION", "staging", "prod", "", " development "],
)
async def test_refuses_to_run_outside_development_whatever_the_settings_say(
    environment: str, script: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """The script's own guard: only ``development`` and ``test`` pass, matched on the value given.

    ``run`` takes the environment as an argument and never reads the settings, so this holds even
    if the settings were somehow permissive. The database must not be opened.
    """

    def must_not_open() -> AsyncSession:
        raise AssertionError("the database must not be touched")

    code = await script.run(["pending@hds.test", "invite"], environment, must_not_open)

    captured = capsys.readouterr()
    assert code == 2
    assert captured.out == ""
    assert "development" in captured.err


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_the_command_line_entry_point_refuses_outside_development_without_touching_the_database(
    environment: str,
    script: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``main`` reads the environment from the settings and hands it to the guard."""

    def must_not_open() -> AsyncSession:
        raise AssertionError("the database must not be touched")

    class Disposable:
        async def dispose(self) -> None:
            return None

    monkeypatch.setattr(script, "get_settings", lambda: SimpleNamespace(ENVIRONMENT=environment))
    monkeypatch.setattr(script, "AsyncSessionLocal", must_not_open)
    monkeypatch.setattr(script, "engine", Disposable())

    assert script.main(["pending@hds.test", "invite"]) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and "development" in captured.err


@pytest.mark.parametrize("environment", ["development", "test", "Development", "TEST"])
async def test_runs_in_the_development_environments(
    environment: str, script: Any, factory: Factory, capsys: pytest.CaptureFixture[str]
) -> None:
    assert await script.run(["pending@hds.test", "invite"], environment, factory) == 0


async def test_issues_an_invitation_token_for_a_pending_account(
    script: Any, factory: Factory, capsys: pytest.CaptureFixture[str]
) -> None:
    code = await script.run(["pending@hds.test", "invite"], "development", factory)
    token = printed_token(capsys.readouterr().out)
    assert code == 0
    row = await stored(factory, token)
    assert row.purpose == "invite" and row.used_at is None
    expires = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=UTC)
    assert abs(expires - (datetime.now(UTC) + timedelta(days=7))) < timedelta(minutes=1)


@pytest.mark.parametrize(
    ("email", "message"),
    [("deactivated@hds.test", "not pending"), ("deleted@hds.test", "no account")],
)
async def test_an_invitation_is_refused_for_an_account_that_is_not_pending(
    email: str, message: str, script: Any, factory: Factory, capsys: pytest.CaptureFixture[str]
) -> None:
    code = await script.run([email, "invite"], "development", factory)

    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert message in captured.err
    async with factory() as session:
        stored_links = await session.execute(select(func.count()).select_from(InvitationToken))
    assert stored_links.scalar_one() == 0


async def test_the_token_for_a_pending_account_can_really_be_redeemed(
    script: Any, factory: Factory, capsys: pytest.CaptureFixture[str]
) -> None:
    await script.run(["pending@hds.test", "invite"], "development", factory)
    token = printed_token(capsys.readouterr().out)

    async with factory() as session:
        service = InvitationService(
            UserRepository(session),
            InvitationTokenRepository(session),
            AsyncMock(),
            RefreshTokenRepository(session),
            LoginThrottleRepository(session),
            MagicMock(),
        )
        await service.accept_invitation(raw_token=token, new_password="A-long-password-1")
        await session.commit()

    async with factory() as session:
        user = await UserRepository(session).get_by_email("pending@hds.test")
    assert user is not None and user.is_active and user.activated_at is not None


async def test_issues_a_reset_token_for_an_active_account(
    script: Any, factory: Factory, capsys: pytest.CaptureFixture[str]
) -> None:
    code = await script.run(["active@hds.test", "reset"], "test", factory)
    token = printed_token(capsys.readouterr().out)
    assert code == 0
    row = await stored(factory, token)
    assert row.purpose == "reset"
    expires = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=UTC)
    assert abs(expires - (datetime.now(UTC) + timedelta(minutes=60))) < timedelta(minutes=1)


async def test_the_token_is_printed_once_and_stored_only_as_a_hash(
    script: Any,
    factory: Factory,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.chdir(tmp_path)
    caplog.set_level(logging.DEBUG)
    await script.run(["pending@hds.test", "invite"], "development", factory)
    captured = capsys.readouterr()
    token = printed_token(captured.out)

    assert captured.out.count(token) == 1  # shown once
    assert token not in captured.err
    assert all(token not in r.getMessage() for r in caplog.records)  # no log line
    assert list(tmp_path.iterdir()) == []  # no file written anywhere we can see
    row = await stored(factory, token)
    assert row.token_hash == hashlib.sha256(token.encode()).hexdigest()
    assert token not in str(row.__dict__.values())  # the database holds the hash, not the token


@pytest.mark.parametrize(
    ("email", "purpose"),
    [
        ("nobody@hds.test", "invite"),  # no such account
        ("active@hds.test", "invite"),  # an invitation completes a pending account only
        ("pending@hds.test", "reset"),  # a reset needs an active account
    ],
)
async def test_nothing_is_issued_for_an_ineligible_account(
    script: Any, factory: Factory, capsys: pytest.CaptureFixture[str], email: str, purpose: str
) -> None:
    code = await script.run([email, purpose], "development", factory)
    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert captured.err  # a reason is given


async def test_an_unknown_purpose_is_a_usage_error(
    script: Any, factory: Factory, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as caught:
        await script.run(["pending@hds.test", "welcome"], "development", factory)
    assert caught.value.code == 2
