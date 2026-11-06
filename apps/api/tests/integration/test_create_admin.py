"""The first Admin of the shared environment comes from a one-shot command (006 T027, R13).

``python -m hds.scripts.create_admin --email a@example.com --organisation "Acme" --timezone
Asia/Jerusalem`` reads the password from standard input, creates the organisation and one active
Admin, and is refused once any user exists, for a password that breaks the password rules, and for
empty input. The password never appears in its output. The checks are read back through the
repositories and the sign-in's own password check.

Covers: 006 research R13; contracts/ops.md section 5 (`make ops-create-admin`).
"""

import io
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core import passwords
from hds.models.user import UserRole
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.user import UserRepository
from hds.scripts import create_admin

API_DIR = Path(__file__).resolve().parents[2]
PASSWORD = "a-first-admin-password-5e81"
ARGS = ["--email", "a@example.com", "--organisation", "Acme", "--timezone", "Asia/Jerusalem"]


@pytest_asyncio.fixture
async def factory(
    test_db: AsyncSession, migrated_test_db: str
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Sessions on the test database; ``test_db`` empties every table after the test."""
    engine = create_async_engine(migrated_test_db, poolclass=NullPool)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def run(
    factory: async_sessionmaker[AsyncSession], stdin: str, argv: list[str] = ARGS
) -> tuple[int, str]:
    """Run the command in this process; its exit status and everything it printed."""
    out = io.StringIO()
    status = await create_admin.run(argv, io.StringIO(stdin), out, factory)
    return status, out.getvalue()


async def users(factory: async_sessionmaker[AsyncSession]) -> list[tuple[str, UserRole, bool]]:
    async with factory() as session:
        found = await UserRepository(session).get_by_email("a@example.com")
        return [] if found is None else [(found.email, found.role, found.is_active)]


async def test_it_creates_the_organisation_and_one_admin(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    status, output = await run(factory, PASSWORD + "\n")
    assert status == 0, output
    assert await users(factory) == [("a@example.com", UserRole.admin, True)]
    async with factory() as session:
        admin = await UserRepository(session).get_by_email("a@example.com")
        assert admin is not None
        org = await OrganisationRepository(session).get_by_id(admin.organization_id)
        assert org is not None
        assert (org.name, org.timezone) == ("Acme", "Asia/Jerusalem")
        assert await passwords.verify_password(PASSWORD, admin.password_hash)
    assert PASSWORD not in output


async def test_it_is_refused_once_any_user_exists(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    assert (await run(factory, PASSWORD + "\n"))[0] == 0
    other = ["--email", "b@example.com", "--organisation", "Other", "--timezone", "UTC"]
    status, output = await run(factory, PASSWORD + "\n", other)
    assert status != 0
    assert "exist" in output
    assert PASSWORD not in output
    async with factory() as session:
        assert await UserRepository(session).get_by_email("b@example.com") is None


@pytest.mark.parametrize("stdin", ["short-pass\n", "", "\n"])
async def test_a_short_or_missing_password_is_refused_and_nothing_is_made(
    factory: async_sessionmaker[AsyncSession], stdin: str
) -> None:
    status, output = await run(factory, stdin)
    assert status != 0
    assert await users(factory) == []
    if stdin.strip():
        assert stdin.strip() not in output
    else:
        assert "no password" in output  # said as such, not as a short one


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--timezone", "Mars/Olympus"),
        ("--email", "not-an-address"),
        ("--organisation", "  "),
        # Review R19: a blank name, and one past the column, are refused before any insert.
        ("--name", "  "),
        ("--name", "x" * 256),
    ],
)
async def test_a_bad_argument_is_refused_and_nothing_is_made(
    factory: async_sessionmaker[AsyncSession], flag: str, value: str
) -> None:
    argv = list(ARGS)
    if flag == "--name":
        argv += [flag, value]
    else:
        argv[argv.index(flag) + 1] = value
    status, output = await run(factory, PASSWORD + "\n", argv)
    assert status == 1, output
    assert await users(factory) == []
    assert PASSWORD not in output
    assert "a@example.com" not in output


async def test_the_module_runs_from_the_command_line(
    factory: async_sessionmaker[AsyncSession], migrated_test_db: str
) -> None:
    env = {**os.environ, "DATABASE_URL": migrated_test_db, "ENVIRONMENT": "test"}
    env.setdefault("JWT_SECRET_KEY", "a-test-only-signing-key-for-create-admin")
    done = subprocess.run(
        [sys.executable, "-m", "hds.scripts.create_admin", *ARGS],
        input=PASSWORD + "\n",
        capture_output=True,
        text=True,
        cwd=API_DIR,
        env=env,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-1500:]
    assert PASSWORD not in done.stdout + done.stderr
    assert await users(factory) == [("a@example.com", UserRole.admin, True)]


async def test_help_exits_zero(factory: async_sessionmaker[AsyncSession]) -> None:
    # Review R22: --help is not bad usage.
    status, _ = await run(factory, "", ["--help"])
    assert status == 0
    status, _ = await run(factory, "", ["--email"])
    assert status == 2
