"""The development-only script that shows a working link once (T221, research D6).

The mail outbox stores no usable token, so a developer testing by hand needs another way to get
one. The script issues a fresh token for an account and prints it to the terminal once. It must
refuse to run outside a development environment and must leave the token in no file and no log.

Covers: FR-203.
"""

import hashlib
import importlib
import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from hds.core.security import hash_password
from hds.models.base import Base
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole
from hds.repositories.invitation_token import InvitationTokenRepository


@pytest.fixture
def script() -> Any:
    return importlib.import_module("hds.scripts.dev_issue_link")


@pytest_asyncio.fixture
async def factory():
    engine = create_async_engine(
        "sqlite+aiosqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        org = Organisation(id=uuid.uuid4(), name="Dev Org", timezone="UTC")
        session.add(org)
        for email, active in (("pending@hds.test", False), ("active@hds.test", True)):
            session.add(
                User(
                    id=uuid.uuid4(),
                    organization_id=org.id,
                    email=email,
                    password_hash=hash_password("Dev1234!"),
                    full_name=email,
                    role=UserRole.employee,
                    is_active=active,
                    is_deleted=False,
                )
            )
        await session.commit()
    yield maker
    await engine.dispose()


def printed_token(out: str) -> str:
    return out.strip().split()[-1]


async def stored(factory, token: str) -> Any:
    async with factory() as session:
        return await InvitationTokenRepository(session).get_by_hash(
            hashlib.sha256(token.encode()).hexdigest()
        )


async def test_refuses_to_run_outside_development(script, factory, capsys) -> None:
    def must_not_open() -> AsyncSession:
        raise AssertionError("the database must not be touched")

    code = await script.run(["pending@hds.test", "invite"], "production", must_not_open)
    captured = capsys.readouterr()
    assert code == 2
    assert captured.out == ""
    assert "development" in captured.err


async def test_issues_an_invitation_token_for_a_pending_account(script, factory, capsys) -> None:
    code = await script.run(["pending@hds.test", "invite"], "development", factory)
    token = printed_token(capsys.readouterr().out)
    assert code == 0
    row = await stored(factory, token)
    assert row.purpose == "invite" and row.used_at is None
    expires = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=UTC)
    assert abs(expires - (datetime.now(UTC) + timedelta(days=7))) < timedelta(minutes=1)


async def test_issues_a_reset_token_for_an_active_account(script, factory, capsys) -> None:
    code = await script.run(["active@hds.test", "reset"], "test", factory)
    token = printed_token(capsys.readouterr().out)
    assert code == 0
    row = await stored(factory, token)
    assert row.purpose == "reset"
    expires = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=UTC)
    assert abs(expires - (datetime.now(UTC) + timedelta(minutes=60))) < timedelta(minutes=1)


async def test_the_token_is_printed_once_and_stored_only_as_a_hash(
    script, factory, capsys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
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
    script, factory, capsys, email: str, purpose: str
) -> None:
    code = await script.run([email, purpose], "development", factory)
    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert captured.err  # a reason is given


async def test_an_unknown_purpose_is_a_usage_error(script, factory, capsys) -> None:
    with pytest.raises(SystemExit) as caught:
        await script.run(["pending@hds.test", "welcome"], "development", factory)
    assert caught.value.code == 2
