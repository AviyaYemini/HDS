"""Alive and ready checks (T013): what each looks at, the 2-second limit and the "behind" rule.

Ready means the database answers and is not behind the code's migrations. A revision the code does
not know is newer, and each migration keeps the previous release working, so it counts as ready:
this is what lets old pods serve during an upgrade and after a rollback. A body never carries an
error's text.

Covers: specs/006-deploy-operations FR-006, FR-009, FR-010, FR-016; contracts/ops.md section 1.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy.exc import ProgrammingError

from hds.core.health import Migrations, load_migrations
from hds.repositories.health import HealthRepository
from hds.services.health import HealthService

MIGRATIONS = Migrations(known=frozenset({"001", "002", "003"}), head="003")
SECRET_TEXT = "password authentication failed for user hds at db.internal:5432"


class FakeProbe:
    """A health repository whose answers the test sets."""

    def __init__(
        self,
        revisions: list[str] | None = None,
        *,
        ping_error: Exception | None = None,
        ping_delay: float = 0.0,
    ) -> None:
        self.revisions_value = ["003"] if revisions is None else revisions
        self.ping_error = ping_error
        self.ping_delay = ping_delay
        self.calls = 0

    async def ping(self) -> None:
        self.calls += 1
        if self.ping_delay:
            await asyncio.sleep(self.ping_delay)
        if self.ping_error is not None:
            raise self.ping_error

    async def revisions(self) -> list[str]:
        self.calls += 1
        return self.revisions_value


def service(probe: FakeProbe, limit: timedelta = timedelta(seconds=2)) -> HealthService:
    """A health service whose every check uses ``probe``."""

    @asynccontextmanager
    async def open_probe() -> AsyncIterator[FakeProbe]:
        yield probe

    return HealthService(open_probe, MIGRATIONS, version="abc123", limit=limit)


async def test_ready_when_the_database_answers_at_the_head() -> None:
    ok, body = await service(FakeProbe(["003"])).ready()
    assert ok
    assert body == {"status": "ok", "version": "abc123"}


async def test_a_revision_the_code_does_not_know_is_newer_and_ready() -> None:
    ok, body = await service(FakeProbe(["004_from_a_newer_release"])).ready()
    assert ok, body


@pytest.mark.parametrize("revisions", [[], ["001"], ["002"]], ids=["empty", "first", "one-behind"])
async def test_behind_the_code_is_not_ready(revisions: list[str]) -> None:
    ok, body = await service(FakeProbe(revisions)).ready()
    assert not ok
    assert body == {"status": "not_ready", "version": "abc123", "failing": ["migrations"]}


async def test_a_missing_version_table_is_behind() -> None:
    probe = FakeProbe()
    probe.revisions_value = []  # the repository answers [] when alembic_version does not exist
    ok, body = await service(probe).ready()
    assert (ok, body["failing"]) == (False, ["migrations"])


async def test_a_database_error_is_not_ready_and_its_text_never_shows() -> None:
    ok, body = await service(FakeProbe(ping_error=OSError(SECRET_TEXT))).ready()
    assert not ok
    assert body["failing"] == ["database"]
    assert SECRET_TEXT not in str(body)
    assert "db.internal" not in str(body)


async def test_a_database_slower_than_the_limit_is_not_ready() -> None:
    # The limit is shrunk instead of waiting two real seconds; the rule is the same.
    loop = asyncio.get_running_loop()
    started = loop.time()
    ok, body = await service(FakeProbe(ping_delay=5.0), limit=timedelta(milliseconds=50)).ready()
    assert (ok, body["failing"]) == (False, ["database"])
    assert loop.time() - started < 1.0


async def test_live_never_touches_the_database() -> None:
    probe = FakeProbe(ping_error=OSError("down"))
    body = service(probe).live()
    assert body == {"status": "ok", "version": "abc123"}
    assert probe.calls == 0


def test_the_code_s_own_migrations_load_with_their_head() -> None:
    migrations = load_migrations()
    assert migrations.head in migrations.known
    assert "001" in {revision[:3] for revision in migrations.known}


async def test_a_session_that_hangs_on_close_still_answers_within_the_limit() -> None:
    # A black-holed database: the reads hang, and so does closing the session afterwards.
    @asynccontextmanager
    async def hanging() -> AsyncIterator[FakeProbe]:
        try:
            yield FakeProbe(ping_delay=30.0)
        finally:
            await asyncio.sleep(30.0)

    loop = asyncio.get_running_loop()
    started = loop.time()
    svc = HealthService(hanging, MIGRATIONS, version="v", limit=timedelta(milliseconds=50))
    ok, body = await svc.ready()
    assert (ok, body["failing"]) == (False, ["database"])
    assert loop.time() - started < 1.0


async def test_without_the_migration_check_only_the_database_counts() -> None:
    @asynccontextmanager
    async def open_probe() -> AsyncIterator[FakeProbe]:
        yield FakeProbe(["001"])  # behind

    svc = HealthService(open_probe, MIGRATIONS, version="v", check_migrations=False)
    assert (await svc.ready())[0]


def test_two_migration_heads_are_refused_at_load(tmp_path: Path) -> None:
    versions = tmp_path / "versions"
    versions.mkdir()
    (tmp_path / "script.py.mako").write_text("")
    for name in ("a", "b"):
        (versions / f"{name}.py").write_text(
            f'revision = "{name}"\ndown_revision = None\nbranch_labels = None\ndepends_on = None\n'
        )
    with pytest.raises(RuntimeError, match="single head"):
        load_migrations(tmp_path)


class DriverError(Exception):
    """A driver error with an SQLSTATE, as asyncpg's are."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


class FailingSession:
    """A session whose every statement raises ``ProgrammingError`` with ``sqlstate``."""

    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate
        self.rolled_back = False

    async def execute(self, statement: object) -> object:
        raise ProgrammingError("SELECT", {}, DriverError(self.sqlstate))

    async def rollback(self) -> None:
        self.rolled_back = True


async def test_only_a_missing_version_table_reads_as_no_revisions() -> None:
    missing = FailingSession("42P01")  # undefined_table
    assert await HealthRepository(missing).revisions() == []  # type: ignore[arg-type]
    denied = FailingSession("42501")  # insufficient_privilege: a database fault, not migrations
    with pytest.raises(ProgrammingError):
        await HealthRepository(denied).revisions()  # type: ignore[arg-type]
