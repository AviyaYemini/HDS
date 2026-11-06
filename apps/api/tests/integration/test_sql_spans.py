"""Real SQL statements make spans under their request or job, without bound values (review R29).

The SQLAlchemy instrumentation 0.66b1 declares support below SQLAlchemy 2.1 only and, on 2.1.6,
refused to instrument anything, so no SQL span existed and no test noticed. ``tracing.configure``
now skips that version check; these tests are what make the bypass safe. They run real statements
on the test database, through an engine made after ``configure`` with the instrumentation's own
wrapping, the way the application's engine is instrumented, and read the spans from memory:

1. a request that queries makes client spans that are children of the request's span;
2. a worker's run of a job makes SQL spans under the job's span;
3. a statement attribute holds placeholders, never a bound value: a distinctive address sent at
   sign-in appears in no attribute of any span;
4. a statement outside any request or job still starts no trace (review R5).

Covers: 006 FR-008a; review ledger R29, R5.
"""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import pytest_asyncio
import sqlalchemy.ext.asyncio as sa_asyncio
from httpx import AsyncClient
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from hds.core import trace_context, tracing
from hds.core.database import get_db, get_session_factory
from hds.main import app
from hds.models.user import User
from hds.worker import run_once
from tests.roster_support import headers_for
from tests.schedule_support import ask, staffed_project

ADDRESS = "distinctive.person.r29@example.com"


@pytest.fixture(scope="module")
def sink() -> Iterator[InMemorySpanExporter]:
    provider = tracing.configure("hds-test")
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield exporter
    exporter.shutdown()


@pytest_asyncio.fixture
async def traced(
    test_db: AsyncSession, migrated_test_db: str, sink: InMemorySpanExporter
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Sessions on an instrumented engine, which the app's requests use for this test."""
    # Looked up on the module now, after configure() wrapped it: the instrumentation's own path.
    engine = sa_asyncio.create_async_engine(migrated_test_db, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def db() -> AsyncIterator[AsyncSession]:
        async with factory() as session:
            yield session

    previous = dict(app.dependency_overrides)
    app.dependency_overrides[get_db] = db
    app.dependency_overrides[get_session_factory] = lambda: factory
    sink.clear()
    try:
        yield factory
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(previous)
        await engine.dispose()


def sql(spans: tuple[ReadableSpan, ...]) -> list[ReadableSpan]:
    return [s for s in spans if s.kind == SpanKind.CLIENT and s.attributes and _statement(s)]


def _statement(span: ReadableSpan) -> str:
    attributes = span.attributes or {}
    return str(attributes.get("db.statement") or attributes.get("db.query.text") or "")


async def test_a_request_s_queries_are_spans_inside_its_trace(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    traced: async_sessionmaker[AsyncSession],
    sink: InMemorySpanExporter,
) -> None:
    headers = await headers_for(async_client, "manager")
    sink.clear()
    assert (await async_client.get("/me", headers=headers)).status_code == 200
    spans = sink.get_finished_spans()
    [request] = [s for s in spans if s.kind == SpanKind.SERVER]
    statements = sql(spans)
    assert statements, [s.name for s in spans]
    assert request.context is not None
    ids = {s.context.span_id for s in spans if s.context is not None}
    for statement in statements:
        assert statement.context is not None and statement.parent is not None
        assert statement.context.trace_id == request.context.trace_id
        assert statement.parent.span_id in ids  # inside the request's tree


async def test_a_job_s_queries_are_spans_under_the_job_span(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    traced: async_sessionmaker[AsyncSession],
    sink: InMemorySpanExporter,
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()
    sink.clear()
    assert str(await run_once(traced)) == job["id"]
    spans = sink.get_finished_spans()
    [run] = [s for s in spans if s.name == trace_context.JOB_SPAN]
    assert run.context is not None
    inside = [s for s in sql(spans) if s.context and s.context.trace_id == run.context.trace_id]
    assert inside, [s.name for s in spans]
    assert any(s.parent is not None and s.parent.span_id == run.context.span_id for s in inside)


def every_value(span: ReadableSpan) -> list[Any]:
    values: list[Any] = list((span.attributes or {}).values())
    for event in span.events:
        values += list((event.attributes or {}).values())
    return values


async def test_statements_carry_placeholders_never_bound_values(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    traced: async_sessionmaker[AsyncSession],
    sink: InMemorySpanExporter,
) -> None:
    response = await async_client.post(
        "/auth/login", json={"email": ADDRESS, "password": "not-the-password-0000"}
    )
    assert response.status_code == 401
    spans = sink.get_finished_spans()
    statements = sql(spans)
    assert any("users" in _statement(s) for s in statements)  # the lookup by address ran
    for span in spans:
        for value in every_value(span):
            assert ADDRESS not in str(value), span.name


async def test_a_statement_outside_any_request_or_job_starts_no_trace(
    traced: async_sessionmaker[AsyncSession], sink: InMemorySpanExporter
) -> None:
    async with traced() as session:
        await session.execute(text("SELECT 1"))
    assert sink.get_finished_spans() == ()
    # The same statement inside a span is kept: the instrumentation is on.
    with trace_context.tracer().start_as_current_span("a job", kind=SpanKind.CONSUMER):
        async with traced() as session:
            await session.execute(text("SELECT 1"))
    assert sql(sink.get_finished_spans())
