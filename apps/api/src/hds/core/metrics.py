"""Prometheus metrics: RED per endpoint, the version, and the server on the operations port.

Layer: core. Depends on ``prometheus_client`` and the ASGI interface only.

Every HTTP request is counted by method, route template and status, and timed by method and route
(specs/006-deploy-operations FR-007). The route label is the template the router matched, such as
``/api/v1/shifts/{id}``, or ``unmatched``: never the path, so no id, name or e-mail address becomes
a label (FR-009). Errors are read from ``status``: 5xx are the server's, 4xx the client's.

Each app gets its own registry, so tests that build several apps do not add up each other's counts.
The registry is served on its own port (``METRICS_PORT``), never the public one, so an ingress
cannot publish it by mistake.
"""

import time
from collections.abc import Awaitable, Callable, MutableMapping
from datetime import timedelta
from threading import Thread
from typing import Any
from wsgiref.simple_server import WSGIServer

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

#: The route label of a request no route matched.
UNMATCHED = "unmatched"

_HTTP_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


def build_info(registry: CollectorRegistry, version: str) -> None:
    """Register ``hds_build_info{version}``, always 1, so the running version can be read (FR-010).

    Args:
        registry: Where to register it.
        version: The version this process runs.
    """
    Gauge(
        "hds_build_info", "The version this process runs.", ["version"], registry=registry
    ).labels(version=version).set(1)


class HttpMetrics:
    """The API's HTTP metrics, in a registry of their own."""

    def __init__(self, version: str) -> None:
        """Create the registry, the request counter, the duration histogram and the build info.

        Args:
            version: The version this process runs.
        """
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "hds_http_requests",
            "HTTP requests, by method, route template and status.",
            ["method", "route", "status"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "hds_http_request_duration_seconds",
            "How long HTTP requests took, by method and route template.",
            ["method", "route"],
            buckets=_HTTP_BUCKETS,
            registry=self.registry,
        )
        build_info(self.registry, version)

    def observe(self, method: str, route: str, status: int, elapsed: timedelta) -> None:
        """Record one finished request that took ``elapsed``."""
        self.requests.labels(method=method, route=route, status=str(status)).inc()
        self.duration.labels(method=method, route=route).observe(elapsed.total_seconds())


def route_template(scope: Scope) -> str:
    """The template of the route the router matched for ``scope``, or ``unmatched``.

    Starlette writes the matched route into the request's scope while routing, so this is read
    after the app has answered.
    """
    route = scope.get("route")
    template = getattr(route, "path_format", None) or getattr(route, "path", None)
    return template if isinstance(template, str) and template else UNMATCHED


class MetricsMiddleware:
    """Pure ASGI middleware that counts and times every HTTP request.

    Installed outermost, so it sees the final status, including the 500 for an error that no
    handler caught. Other traffic (lifespan, websockets) passes through uncounted.
    """

    def __init__(self, app: ASGIApp, metrics: HttpMetrics) -> None:
        """Wrap ``app``, recording into ``metrics``."""
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Pass the request on and record it when it finishes."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        status = 500
        started = time.perf_counter_ns()

        async def send_and_note(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_and_note)
        finally:
            self.metrics.observe(
                str(scope.get("method", "")),
                route_template(scope),
                status,
                timedelta(microseconds=(time.perf_counter_ns() - started) / 1000),
            )


#: Every state a schedule job can be in, each always reported (a missing state reads as 0).
JOB_STATES = ("queued", "running", "succeeded", "failed")

_JOB_BUCKETS = (1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0)


class JobMetrics:
    """The schedule worker's metrics: jobs by state and how long they took (FR-008)."""

    def __init__(self, version: str) -> None:
        """Create the registry, the state gauge, the duration histogram and the build info.

        Args:
            version: The version this process runs.
        """
        self.registry = CollectorRegistry()
        self.jobs = Gauge(
            "hds_schedule_jobs",
            "Schedule jobs by state, read from the table when scraped.",
            ["state"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "hds_schedule_job_duration_seconds",
            "How long schedule jobs took, from claim to stored result, by outcome.",
            ["outcome"],
            buckets=_JOB_BUCKETS,
            registry=self.registry,
        )
        build_info(self.registry, version)

    def set_counts(self, counts: dict[str, int]) -> None:
        """Set the state gauge from ``counts``; a state absent from it is 0."""
        for state in JOB_STATES:
            self.jobs.labels(state=state).set(counts.get(state, 0))

    def observe(self, outcome: str, elapsed: timedelta) -> None:
        """Record one finished job, ``succeeded`` or ``failed``, that took ``elapsed``."""
        self.duration.labels(outcome=outcome).observe(elapsed.total_seconds())


def serve(registry: CollectorRegistry, port: int) -> tuple[WSGIServer, Thread] | None:
    """Serve ``registry`` at ``/metrics`` on ``port`` in a background thread.

    Args:
        registry: What to serve.
        port: The operations port; 0 serves nothing.

    Returns:
        The server and its thread, to shut down at exit, or ``None`` when ``port`` is 0.
    """
    if port == 0:
        return None
    return start_http_server(port, registry=registry)
