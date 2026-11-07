"""The migration command's start-up retries and failure messages (T036, review R58).

The migration Job may start while PostgreSQL restarts (a ``helm upgrade`` of the database release,
a node restart), and the Job retries only once. So ``migrate`` tries the first connection a few
times before it gives up. Once connected, an error is the migration's, not the network's, and the
message says so. A dropped connection (asyncpg's ``InterfaceError`` family) exits 1, as any
database failure does, rather than ending in a traceback.

Covers: 006 FR-016; research R5.
"""

from typing import Any

import asyncpg  # type: ignore[import-untyped]
import pytest

import hds.scripts.migrate as migrate
from hds.core.health import Migrations


async def no_wait(seconds: float) -> None:
    return None


async def test_the_first_connection_is_retried_until_the_database_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tries: list[str] = []
    connection = object()

    async def connect(dsn: str) -> object:
        tries.append(dsn)
        if len(tries) < 3:
            raise ConnectionRefusedError("refused")
        return connection

    monkeypatch.setattr("hds.scripts.migrate.asyncpg.connect", connect)
    monkeypatch.setattr("hds.scripts.migrate.asyncio.sleep", no_wait)
    assert await migrate._connect("postgresql://db/hds") is connection
    assert len(tries) == 3


async def test_a_database_still_starting_up_is_waited_for(monkeypatch: pytest.MonkeyPatch) -> None:
    tries: list[int] = []

    async def connect(dsn: str) -> object:
        tries.append(1)
        if len(tries) == 1:
            raise asyncpg.CannotConnectNowError("the database system is starting up")
        return object()

    monkeypatch.setattr("hds.scripts.migrate.asyncpg.connect", connect)
    monkeypatch.setattr("hds.scripts.migrate.asyncio.sleep", no_wait)
    await migrate._connect("postgresql://db/hds")
    assert len(tries) == 2


async def test_the_connection_is_given_up_after_the_last_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tries: list[int] = []

    async def connect(dsn: str) -> object:
        tries.append(1)
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr("hds.scripts.migrate.asyncpg.connect", connect)
    monkeypatch.setattr("hds.scripts.migrate.asyncio.sleep", no_wait)
    with pytest.raises(ConnectionRefusedError):
        await migrate._connect("postgresql://db/hds")
    assert len(tries) == migrate.CONNECT_ATTEMPTS


class AtHead:
    """A connection to a database already at head, which grants the lock at once."""

    closed = False

    async def fetchval(self, query: str, *args: object) -> bool:
        return True

    async def fetch(self, query: str) -> list[dict[str, str]]:
        return [{"version_num": "024_head"}]

    async def close(self) -> None:
        self.closed = True


async def test_the_migration_itself_retries_its_first_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tries: list[int] = []
    connection = AtHead()

    async def connect(dsn: str) -> AtHead:
        tries.append(1)
        if len(tries) == 1:
            raise ConnectionRefusedError("refused")
        return connection

    monkeypatch.setattr("hds.scripts.migrate.asyncpg.connect", connect)
    monkeypatch.setattr("hds.scripts.migrate.asyncio.sleep", no_wait)
    history = Migrations(known=frozenset({"024_head"}), head="024_head")
    assert await migrate.migrate("postgresql+asyncpg://hds@db/hds", history) == 0
    assert len(tries) == 2 and connection.closed


def run_main(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> int:
    async def fails(url: str, history: Any) -> int:
        raise error

    monkeypatch.setattr(migrate, "migrate", fails)
    monkeypatch.setattr(migrate, "load_migrations", lambda: None)
    monkeypatch.setattr(migrate, "configure_logging", lambda: None)
    with pytest.raises(SystemExit) as stop:
        migrate.main()
    return int(stop.value.code or 0)


def test_an_unreachable_database_is_reported_as_unreachable(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    assert run_main(monkeypatch, ConnectionRefusedError("refused")) == 1
    assert "could not be reached" in caplog.text


def test_an_error_from_the_database_is_reported_as_the_migration_failing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    assert run_main(monkeypatch, asyncpg.InsufficientPrivilegeError("denied")) == 1
    assert "the migration failed on the database" in caplog.text
    assert "could not be reached" not in caplog.text


def test_a_dropped_connection_exits_1(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Not a PostgresError: asyncpg raises it for a connection already closed under it.
    error = asyncpg.InterfaceError("cannot perform operation: connection is closed")
    assert run_main(monkeypatch, error) == 1
    assert "the migration failed on the database" in caplog.text
