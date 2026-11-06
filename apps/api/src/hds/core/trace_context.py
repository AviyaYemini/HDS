"""The trace a schedule job carries from the API to the worker (specs/006 FR-008a, research R8).

Layer: core. Depends on the OpenTelemetry API only, so services can import it without loading
FastAPI, the instrumentation or an exporter; the set-up lives in ``hds.core.tracing``.

The API stores ``current_traceparent()`` on the job when it queues it, and the worker runs the job
in a span started from ``continue_from(row value)``. A value that is not a version-00 W3C
``traceparent`` is never continued: the worker starts a new trace for it, as for null.
"""

import re

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

#: The span each worker run of a schedule job is wrapped in.
JOB_SPAN = "schedule_job.run"

# Version 00 exactly, lower-case hex, and neither id all zeros (W3C Trace Context, data-model.md).
_TRACEPARENT = re.compile(r"00-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}")
_PROPAGATOR = TraceContextTextMapPropagator()


def valid(value: str | None) -> bool:
    """Whether ``value`` is a well-formed version-00 ``traceparent``."""
    return value is not None and _TRACEPARENT.fullmatch(value) is not None


def tracer() -> trace.Tracer:
    """The application's tracer."""
    return trace.get_tracer("hds")


def current_traceparent() -> str | None:
    """The ``traceparent`` of the current span, or ``None`` outside any trace.

    The propagator writes nothing for an invalid span context, so a malformed value cannot come
    out of it; the format check stays as the column's guard.
    """
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier)
    value = carrier.get("traceparent")
    return value if valid(value) else None


def continue_from(traceparent: str | None) -> otel_context.Context:
    """The context to start a span in: the trace of ``traceparent``, or a new trace.

    A null or malformed value gives an empty context, so the span starts a trace of its own rather
    than joining whatever happens to be current.
    """
    if traceparent is None or not valid(traceparent):
        return otel_context.Context()
    return _PROPAGATOR.extract({"traceparent": traceparent})
