"""Tracing set-up: the provider, the instrumentation and the export (specs/006 FR-008a, R8).

Layer: core. Depends on ``hds.core.database`` (the engine it instruments), ``hds.core.logging``
(the redaction) and the OpenTelemetry SDK. Services use ``hds.core.trace_context`` instead, which
needs the OpenTelemetry API only.

``configure`` sets up the tracer provider once per process, with the SQLAlchemy instrumentation
on the application's engine, and instruments a FastAPI app when given one. Spans go over OTLP
(HTTP) only when an OTLP endpoint is set; otherwise they are made, so the logs can name them, and
dropped.

What leaves the process is redacted first (``RedactingExporter``): URLs lose their query strings,
and every text attribute, event and status message goes through the log redaction, since a trace
store is copied as widely as the logs are. SQL run outside any span (the worker's poll, the
metrics' job counts) starts no trace of its own, and the health probes are not traced.
"""

import os
from collections.abc import Mapping, Sequence
from typing import Any

from fastapi import FastAPI
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import Decision, ParentBased, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind, Status, StatusCode
from opentelemetry.util.types import Attributes

from hds.core.logging import redact

_ENDPOINTS = ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
# The probes answer every few seconds; a trace for each would bury the ones worth reading. The
# instrumentation searches each pattern anywhere in the full URL, so they are anchored to the whole
# path: a route that only contains a probe's name is still traced (review D1).
_UNTRACED_PATHS = r"^https?://[^/]+/(livez|readyz|health)/?(\?.*)?$"
# Attributes that hold a URL, kept without the query string, and one that is the query alone.
_URL_ATTRIBUTES = frozenset({"http.url", "http.target", "url.full"})
_QUERY_ATTRIBUTES = frozenset({"url.query"})

_provider: TracerProvider | None = None


class _NoOrphanStatements(Sampler):
    """Drop a client span (a SQL statement) that would start a trace; keep every other root.

    Under ``ParentBased`` it decides root spans only, so a statement inside a request or a job is
    kept with it, and one outside both is not a one-span trace of its own (review R5).
    """

    def should_sample(
        self,
        parent_context: otel_context.Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes: Attributes = None,
        links: Sequence[Link] | None = None,
        trace_state: trace.TraceState | None = None,
    ) -> SamplingResult:
        """Drop a root client span; record and sample any other root."""
        if kind == SpanKind.CLIENT:
            return SamplingResult(Decision.DROP)
        return SamplingResult(Decision.RECORD_AND_SAMPLE, attributes)

    def get_description(self) -> str:
        """The sampler's name, as the SDK reports it."""
        return "NoOrphanStatements"


#: Children follow their parent (the caller's sampled flag, as the SDK's default does); roots are
#: decided by ``_NoOrphanStatements``.
SAMPLER = ParentBased(root=_NoOrphanStatements())


def _clean(attributes: Mapping[str, Any] | None) -> dict[str, Any]:
    """``attributes`` without query strings, and with every text value redacted."""
    cleaned: dict[str, Any] = {}
    for key, value in (attributes or {}).items():
        if key in _QUERY_ATTRIBUTES:
            continue
        if key in _URL_ATTRIBUTES and isinstance(value, str):
            value = value.split("?", 1)[0]
        if isinstance(value, str):
            value = redact(value)
        elif isinstance(value, tuple | list) and all(isinstance(v, str) for v in value):
            value = tuple(redact(v) for v in value)
        cleaned[key] = value
    return cleaned


def _redacted(span: ReadableSpan) -> ReadableSpan:
    """A copy of ``span`` whose attributes, events and status message are redacted."""
    status = span.status
    if status.status_code is StatusCode.ERROR and status.description:
        status = Status(StatusCode.ERROR, redact(status.description))
    return ReadableSpan(
        name=span.name,
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=_clean(span.attributes),
        events=[Event(e.name, _clean(e.attributes), e.timestamp) for e in span.events],
        links=span.links,
        kind=span.kind,
        status=status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class RedactingExporter(SpanExporter):
    """Redact each span, then hand it to the real exporter (review R8)."""

    def __init__(self, inner: SpanExporter) -> None:
        """Wrap ``inner``, which sends the spans."""
        self._inner = inner

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Send redacted copies of ``spans``."""
        return self._inner.export([_redacted(span) for span in spans])

    def shutdown(self) -> None:
        """Shut the real exporter down."""
        self._inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Flush the real exporter."""
        return self._inner.force_flush(timeout_millis)


def span_processors(environ: Mapping[str, str]) -> list[SpanProcessor]:
    """The span processors for ``environ``: redacted OTLP over HTTP in batches, or none at all.

    With neither ``OTEL_EXPORTER_OTLP_ENDPOINT`` nor ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` set, no
    exporter is installed and nothing is sent; the exporter reads the endpoint and its other
    ``OTEL_*`` settings itself.
    """
    if not any(environ.get(name) for name in _ENDPOINTS):
        return []
    return [BatchSpanProcessor(RedactingExporter(OTLPSpanExporter()))]


def configure(service_name: str, app: FastAPI | None = None) -> TracerProvider:
    """Set up tracing for this process, and instrument ``app`` if one is given.

    The provider and the SQLAlchemy instrumentation are set up on the first call only; later calls
    return the same provider, so each app the tests build is instrumented without stacking
    providers. ``OTEL_SERVICE_NAME``, when set, names the service instead of ``service_name``.

    Args:
        service_name: ``hds-api`` or ``hds-worker``.
        app: A FastAPI app whose requests start or continue a trace.

    Returns:
        The process's tracer provider.
    """
    global _provider
    if _provider is None:
        from hds.core.database import engine

        name = os.environ.get("OTEL_SERVICE_NAME") or service_name
        _provider = TracerProvider(
            resource=Resource.create({"service.name": name}), sampler=SAMPLER
        )
        for processor in span_processors(os.environ):
            _provider.add_span_processor(processor)
        trace.set_tracer_provider(_provider)
        # skip_dep_check (review R29): 0.66b1 declares SQLAlchemy below 2.1 only, and on 2.1 it
        # refused to instrument anything. The engine events it hooks (before and after cursor
        # execute) are unchanged in 2.1, and tests/integration/test_sql_spans.py, which runs real
        # statements, is what makes the bypass safe. Remove it once a release declares 2.1.
        SQLAlchemyInstrumentor().instrument(
            engine=engine.sync_engine, tracer_provider=_provider, skip_dep_check=True
        )
    if app is not None:
        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=_provider,
            excluded_urls=_UNTRACED_PATHS,
            # The ASGI send and receive steps would double every request's spans for nothing.
            exclude_spans=["receive", "send"],
        )
    return _provider


def shutdown() -> None:
    """Send the spans still waiting in the batch, then stop (review R7). Safe to call unset."""
    if _provider is not None:
        _provider.shutdown()
