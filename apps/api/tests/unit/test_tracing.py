"""One trace per schedule job, from the request that queued it through the worker (006 T022).

The API stores the current ``traceparent`` on the job when it queues it; the worker continues that
trace when it runs the job, and starts a new one when the row holds none or a malformed one. Every
JSON log line inside a request or a job names its trace and span; lines outside name neither.
Spans are exported over OTLP only when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set.

A request whose ``traceparent`` is malformed starts the API's own trace (the instrumentation does
not continue it), and the job stores that new trace, so the job is still one trace from the
request to the worker (FR-008a). The malformed header itself is never stored.

Covers: 006 FR-008a; data-model.md ``schedule_jobs.trace_context``; research R8.
"""

import asyncio
import json
import logging
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode

import hds.worker as worker
from hds.core import trace_context, tracing
from hds.core.logging import JsonFormatter, SensitiveDataFilter
from hds.models.user import UserRole
from hds.services.schedule_job import Claim, ScheduleJobService
from tests.unit.test_worker_outcomes import FakeJobs, fake_session, install

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"
TRACEPARENT = f"00-{TRACE_ID}-{PARENT_ID}-01"
LEASE = datetime(2026, 11, 6, 12, 5, tzinfo=UTC)


@pytest.fixture(scope="module")
def spans_sink() -> Iterator[InMemorySpanExporter]:
    """Every span the process finishes from here on, kept in memory."""
    provider = tracing.configure("hds-test")
    sink = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(sink))
    yield sink
    sink.shutdown()  # the processor cannot be removed; a shut exporter keeps nothing


@pytest.fixture
def spans(spans_sink: InMemorySpanExporter) -> InMemorySpanExporter:
    spans_sink.clear()
    return spans_sink


# ── queuing ──────────────────────────────────────────────────────────────────────────────────────


class CapturingJobs:
    """The job repository's ``create``, keeping what it was given."""

    def __init__(self) -> None:
        self.created: dict[str, Any] = {}

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> object:
        self.created = dict(data)
        return SimpleNamespace(id=uuid.uuid4())


class OneOrganisation:
    async def get_by_id(self, org_id: uuid.UUID) -> object:
        return SimpleNamespace(min_rest_hours=11, max_weekly_hours=48)


PROJECT = uuid.uuid4()


class OneProject:
    """One active project, which the manager below manages (a run's scope, 007 T108)."""

    async def active_projects(self, org_id: uuid.UUID, ids: object) -> list[object]:
        return [SimpleNamespace(id=PROJECT)]

    async def manages(self, user_id: uuid.UUID, ids: object) -> set[uuid.UUID]:
        return {PROJECT}


def queuing_service(jobs: CapturingJobs) -> ScheduleJobService:
    unused = cast(Any, None)
    project = cast(Any, OneProject())
    return ScheduleJobService(
        cast(Any, jobs),
        cast(Any, OneOrganisation()),
        project,
        unused,
        unused,
        unused,
        unused,
        unused,
        project,
    )


MANAGER = cast(Any, SimpleNamespace(id=uuid.uuid4(), organization_id=uuid.uuid4()))
MANAGER.role = UserRole.manager
REQUEST = {"first_day": date(2027, 6, 6), "last_day": date(2027, 6, 12), "project_ids": []}


def queuing_app(jobs: CapturingJobs) -> FastAPI:
    """A route that queues a job, on an app instrumented as the real one is."""
    app = FastAPI()

    @app.post("/queue")
    async def queue() -> dict[str, str]:
        await queuing_service(jobs).create(MANAGER, REQUEST)
        return {}

    tracing.configure("hds-test", app=app)
    return app


async def post_queue(jobs: CapturingJobs, headers: dict[str, str]) -> None:
    transport = httpx.ASGITransport(app=queuing_app(jobs))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.post("/queue", headers=headers)).status_code == 200


async def test_a_job_queued_by_a_traced_request_stores_that_trace(
    spans: InMemorySpanExporter,
) -> None:
    jobs = CapturingJobs()
    await post_queue(jobs, {"traceparent": TRACEPARENT})
    stored = jobs.created["trace_context"]
    assert trace_context.valid(stored)
    _, trace_id, span_id, _ = stored.split("-")
    assert trace_id == TRACE_ID
    assert span_id != PARENT_ID  # the API's own span in that trace, the worker's parent


@pytest.mark.parametrize(
    "header",
    ["00-xyz-01", f"00-{TRACE_ID.upper()}-{PARENT_ID}-01", f"00-{'0' * 32}-{PARENT_ID}-01"],
)
async def test_a_malformed_traceparent_is_never_stored(
    spans: InMemorySpanExporter, header: str
) -> None:
    jobs = CapturingJobs()
    await post_queue(jobs, {"traceparent": header})
    stored = jobs.created["trace_context"]
    assert stored != header
    assert trace_context.valid(stored)  # the API's own new trace
    assert stored.split("-")[1] not in header.lower()


async def test_a_job_queued_outside_any_trace_stores_null() -> None:
    jobs = CapturingJobs()
    await queuing_service(jobs).create(MANAGER, REQUEST)
    assert jobs.created["trace_context"] is None


@pytest.mark.parametrize(
    "value",
    [
        TRACEPARENT,
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
    ],
)
def test_the_traceparent_format_is_accepted(value: str) -> None:
    assert trace_context.valid(value)


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "00-xyz-01",
        f"01-{TRACE_ID}-{PARENT_ID}-01",  # only version 00 is known
        f"00-{TRACE_ID}-{PARENT_ID}-01-extra",
        f"00-{TRACE_ID.upper()}-{PARENT_ID}-01",
        f"00-{'0' * 32}-{PARENT_ID}-01",
        f"00-{TRACE_ID}-{'0' * 16}-01",
        f" {TRACEPARENT}",
    ],
)
def test_a_malformed_traceparent_is_refused(value: str | None) -> None:
    assert not trace_context.valid(value)


# ── the worker ───────────────────────────────────────────────────────────────────────────────────


def claim_with(trace_context: str | None) -> Claim:
    return Claim(
        uuid.uuid4(),
        uuid.uuid4(),
        date(2027, 6, 6),
        date(2027, 6, 12),
        (),
        "greedy",
        1,
        {},
        LEASE,
        trace_context,
    )


class TracedJobs(FakeJobs):
    """The scripted job service, handing out one claim with a given trace context."""

    def __init__(self, claim: Claim, **script: bool) -> None:
        super().__init__(**script)
        self.held = claim

    async def claim(self) -> Claim:
        return self.held


def job_span(spans: InMemorySpanExporter) -> ReadableSpan:
    found = [s for s in spans.get_finished_spans() if s.name == trace_context.JOB_SPAN]
    assert len(found) == 1, [s.name for s in spans.get_finished_spans()]
    return found[0]


async def run_job(monkeypatch: pytest.MonkeyPatch, trace_context: str | None) -> None:
    ops = install(monkeypatch, TracedJobs(claim_with(trace_context)))
    await worker.run_once(cast(Any, fake_session), ops)


async def test_the_worker_continues_the_trace_stored_on_the_job(
    monkeypatch: pytest.MonkeyPatch, spans: InMemorySpanExporter
) -> None:
    await run_job(monkeypatch, TRACEPARENT)
    span = job_span(spans)
    assert span.context is not None
    assert format(span.context.trace_id, "032x") == TRACE_ID
    assert span.parent is not None
    assert format(span.parent.span_id, "016x") == PARENT_ID


@pytest.mark.parametrize("stored", [None, f"01-{TRACE_ID}-{PARENT_ID}-01", "garbage"])
async def test_a_job_with_no_or_a_malformed_trace_starts_a_new_one(
    monkeypatch: pytest.MonkeyPatch, spans: InMemorySpanExporter, stored: str | None
) -> None:
    await run_job(monkeypatch, stored)
    span = job_span(spans)
    assert span.parent is None
    assert span.context is not None
    assert format(span.context.trace_id, "032x") != TRACE_ID


async def test_a_job_with_no_trace_never_joins_whatever_span_is_current(
    monkeypatch: pytest.MonkeyPatch, spans: InMemorySpanExporter
) -> None:
    with trace_context.tracer().start_as_current_span("unrelated") as outer:
        await run_job(monkeypatch, None)
    span = job_span(spans)
    assert span.parent is None
    assert span.context is not None
    assert span.context.trace_id != outer.get_span_context().trace_id


async def test_the_job_span_does_not_leak_into_the_worker_loop(
    monkeypatch: pytest.MonkeyPatch, spans: InMemorySpanExporter
) -> None:
    await run_job(monkeypatch, TRACEPARENT)
    assert not trace.get_current_span().get_span_context().is_valid


@pytest.mark.parametrize(
    ("script", "outcome"),
    [({}, "succeeded"), ({"solve_fails": True}, "failed"), ({"stored": False}, "superseded")],
)
async def test_the_job_span_names_the_outcome_and_a_failure_is_an_error(
    monkeypatch: pytest.MonkeyPatch,
    spans: InMemorySpanExporter,
    script: dict[str, bool],
    outcome: str,
) -> None:
    # Review R6: _run catches the failure, so the span must be told.
    ops = install(monkeypatch, TracedJobs(claim_with(TRACEPARENT), **script))
    await worker.run_once(cast(Any, fake_session), ops)
    span = job_span(spans)
    assert span.attributes is not None
    assert span.attributes["hds.schedule_job.outcome"] == outcome
    expected = StatusCode.ERROR if outcome == "failed" else StatusCode.UNSET
    assert span.status.status_code == expected


async def test_sql_outside_any_span_starts_no_trace_of_its_own(
    spans: InMemorySpanExporter,
) -> None:
    # Review R5: the worker's poll and the metrics' counts run outside a job; their statements
    # (client spans) must not become one-span traces. Inside a request or a job they stay.
    tracer = trace_context.tracer()
    with tracer.start_as_current_span("SELECT 1", kind=SpanKind.CLIENT):
        pass
    assert spans.get_finished_spans() == ()
    with tracer.start_as_current_span("a job", kind=SpanKind.CONSUMER):
        with tracer.start_as_current_span("SELECT 2", kind=SpanKind.CLIENT):
            pass
    assert sorted(s.name for s in spans.get_finished_spans()) == ["SELECT 2", "a job"]


def test_the_worker_flushes_its_spans_when_it_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    # Review R7: batched spans are lost unless the provider is shut down on the way out.
    stopped: list[bool] = []

    def interrupted(coroutine: Any) -> None:
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(tracing, "configure", lambda name: None)
    monkeypatch.setattr(tracing, "shutdown", lambda: stopped.append(True))
    monkeypatch.setattr(asyncio, "run", interrupted)
    monkeypatch.setattr("hds.core.database.get_session_factory", lambda: None)
    with pytest.raises(KeyboardInterrupt):
        worker.main()
    assert stopped == [True]


# ── redaction at export ──────────────────────────────────────────────────────────────────────────


def exported(**attributes: Any) -> ReadableSpan:
    """One span with ``attributes``, a failed call inside it, through the redacting exporter."""
    sink = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(tracing.RedactingExporter(sink)))
    with provider.get_tracer("t").start_as_current_span("GET /x", attributes=attributes) as span:
        span.add_event("exception", {"exception.message": "token=abc123 for dana@example.com"})
        span.set_status(Status(StatusCode.ERROR, "password=hunter2"))
    [found] = sink.get_finished_spans()
    return found


def test_urls_lose_their_query_strings_at_export() -> None:
    # Review R8: a search for a name would otherwise reach the collector.
    span = exported(
        **{
            "http.url": "http://api/api/v1/employees?search=Dana+Levi",
            "http.target": "/api/v1/employees?search=Dana+Levi",
            "url.full": "http://api/api/v1/employees?search=Dana+Levi",
            "url.query": "search=Dana+Levi",
            "http.route": "/api/v1/employees",
        }
    )
    assert span.attributes is not None
    assert span.attributes["http.url"] == "http://api/api/v1/employees"
    assert span.attributes["http.target"] == "/api/v1/employees"
    assert span.attributes["url.full"] == "http://api/api/v1/employees"
    assert "url.query" not in span.attributes
    assert span.attributes["http.route"] == "/api/v1/employees"


def test_text_in_attributes_events_and_the_status_is_redacted_at_export() -> None:
    span = exported(**{"db.statement": "SELECT 1 -- password=hunter2", "n": 3})
    assert span.attributes is not None
    assert "hunter2" not in str(span.attributes["db.statement"])
    assert span.attributes["n"] == 3
    [event] = span.events
    message = str(event.attributes["exception.message"]) if event.attributes else ""
    assert "abc123" not in message
    assert "dana@example.com" not in message
    assert span.status.status_code == StatusCode.ERROR
    assert "hunter2" not in (span.status.description or "")


# ── logs ─────────────────────────────────────────────────────────────────────────────────────────


class Lines(logging.Handler):
    """A handler that keeps each record as the JSON line the real handler would write."""

    def __init__(self) -> None:
        super().__init__()
        self.addFilter(SensitiveDataFilter())
        self.setFormatter(JsonFormatter())
        self.lines: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(json.loads(self.format(record)))


@pytest.fixture
def lines() -> Iterator[Lines]:
    handler = Lines()
    logger = logging.getLogger("hds")
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    yield handler
    logger.removeHandler(handler)
    logger.setLevel(previous)


def test_a_line_outside_any_trace_has_no_trace_fields(lines: Lines) -> None:
    logging.getLogger("hds.test").info("outside")
    assert "trace_id" not in lines.lines[0]
    assert "span_id" not in lines.lines[0]


def test_a_trace_field_passed_by_hand_outside_a_span_is_not_written(lines: Lines) -> None:
    # Review R14: only the current span's ids are written, never a stray attribute.
    logging.getLogger("hds.test").info("outside", extra={"trace_id": "x", "span_id": "y"})
    assert "trace_id" not in lines.lines[0]
    assert "span_id" not in lines.lines[0]


def test_a_line_inside_a_span_names_its_trace_and_span(
    lines: Lines, spans: InMemorySpanExporter
) -> None:
    with trace_context.tracer().start_as_current_span("inside") as span:
        logging.getLogger("hds.test").info("inside")
    context = span.get_span_context()
    assert lines.lines[0]["trace_id"] == format(context.trace_id, "032x")
    assert lines.lines[0]["span_id"] == format(context.span_id, "016x")


async def test_a_line_inside_a_request_names_the_request_s_trace(
    lines: Lines, spans: InMemorySpanExporter
) -> None:
    app = FastAPI()

    @app.get("/hello")
    async def hello() -> dict[str, str]:
        logging.getLogger("hds.test").info("inside a request")
        return {}

    tracing.configure("hds-test", app=app)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/hello", headers={"traceparent": TRACEPARENT})
    [line] = [x for x in lines.lines if x["message"] == "inside a request"]
    assert line["trace_id"] == TRACE_ID
    assert len(line["span_id"]) == 16


async def test_a_line_inside_a_job_names_the_job_s_trace(
    monkeypatch: pytest.MonkeyPatch, lines: Lines, spans: InMemorySpanExporter
) -> None:
    class LoggingJobs(TracedJobs):
        async def build(self, claim: Claim) -> object:
            logging.getLogger("hds.test").info("building")
            return object()

    ops = install(monkeypatch, LoggingJobs(claim_with(TRACEPARENT)))
    await worker.run_once(cast(Any, fake_session), ops)
    [line] = [x for x in lines.lines if x["message"] == "building"]
    assert line["trace_id"] == TRACE_ID
    span = job_span(spans)
    assert span.context is not None
    assert line["span_id"] == format(span.context.span_id, "016x")


# ── the probes ───────────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("path", "traced"),
    [
        ("/livez", False),
        ("/readyz", False),
        ("/health", False),
        # The instrumentation searches its patterns anywhere in the URL, so they are anchored
        # (review D1): a route that merely contains a probe's name is still traced.
        ("/api/v1/healthy", True),
        ("/api/v1/health-checks", True),
        ("/api/v1/livez/history", True),
    ],
)
async def test_only_the_health_probes_are_left_untraced(
    spans: InMemorySpanExporter, path: str, traced: bool
) -> None:
    app = FastAPI()

    @app.get(path)
    async def answer() -> dict[str, str]:
        return {}

    tracing.configure("hds-test", app=app)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        assert (await client.get(path)).status_code == 200
    assert bool(spans.get_finished_spans()) is traced


# ── export ───────────────────────────────────────────────────────────────────────────────────────


def test_without_an_endpoint_no_exporter_is_installed() -> None:
    assert tracing.span_processors({}) == []
    assert tracing.span_processors({"OTEL_EXPORTER_OTLP_ENDPOINT": ""}) == []


@pytest.mark.parametrize(
    "setting",
    # Review R11: the traces-only endpoint, which the exporter honours, turns export on too.
    ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"],
)
def test_with_an_endpoint_spans_go_over_otlp_in_batches_through_the_redaction(
    monkeypatch: pytest.MonkeyPatch, setting: str
) -> None:
    given: list[object] = []
    monkeypatch.setattr(tracing, "BatchSpanProcessor", lambda exporter: given.append(exporter))
    processors = tracing.span_processors({setting: "http://collector:4318"})
    assert len(processors) == 1
    [exporter] = given
    assert isinstance(exporter, tracing.RedactingExporter)
    exporter.shutdown()  # nothing was sent
