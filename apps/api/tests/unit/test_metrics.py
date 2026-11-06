"""RED metrics per endpoint (T015): labels from route templates only, never a path or a name.

The middleware counts every request by method, route template and status, and times it. A label
never holds an id, an e-mail address or a query string: an unmatched path is ``unmatched``.
``hds_build_info`` carries the version.

Covers: specs/006-deploy-operations FR-007, FR-009, FR-010; contracts/ops.md section 2.
"""

import re
import uuid

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from prometheus_client import generate_latest

from hds.core.metrics import HttpMetrics, MetricsMiddleware

UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-")


def instrumented() -> tuple[FastAPI, HttpMetrics]:
    """A small app with the middleware, a route with a path parameter and a sign-in route."""
    app = FastAPI()
    metrics = HttpMetrics(version="abc123")

    @app.get("/api/v1/shifts/{id}")
    async def shift(id: uuid.UUID) -> dict[str, str]:
        return {"id": str(id)}

    @app.post("/auth/login")
    async def login() -> dict[str, str]:
        return {"ok": "yes"}

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("boom")

    app.add_middleware(MetricsMiddleware, metrics=metrics)
    return app, metrics


async def exercise(app: FastAPI) -> None:
    """Requests with ids, an unknown path, a body and a query string with an address."""
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        await client.get(f"/api/v1/shifts/{uuid.uuid4()}")
        await client.get(f"/api/v1/shifts/{uuid.uuid4()}")
        await client.get("/no/such/path/someone@example.com")
        await client.post(
            "/auth/login?email=someone@example.com", json={"email": "someone@example.com"}
        )
        await client.get("/boom")


def label_values(metrics: HttpMetrics) -> set[str]:
    """Every label value in the registry."""
    return {
        value
        for family in metrics.registry.collect()
        for sample in family.samples
        for value in sample.labels.values()
    }


async def test_routes_are_labelled_by_template_and_unmatched_paths_as_unmatched() -> None:
    app, metrics = instrumented()
    await exercise(app)
    get = metrics.registry.get_sample_value
    assert (
        get(
            "hds_http_requests_total",
            {"method": "GET", "route": "/api/v1/shifts/{id}", "status": "200"},
        )
        == 2
    )
    assert (
        get("hds_http_requests_total", {"method": "GET", "route": "unmatched", "status": "404"})
        == 1
    )
    assert (
        get("hds_http_requests_total", {"method": "POST", "route": "/auth/login", "status": "200"})
        == 1
    )


async def test_an_error_is_counted_as_a_500() -> None:
    app, metrics = instrumented()
    await exercise(app)
    assert (
        metrics.registry.get_sample_value(
            "hds_http_requests_total", {"method": "GET", "route": "/boom", "status": "500"}
        )
        == 1
    )


async def test_each_request_is_timed() -> None:
    app, metrics = instrumented()
    await exercise(app)
    count = metrics.registry.get_sample_value(
        "hds_http_request_duration_seconds_count",
        {"method": "GET", "route": "/api/v1/shifts/{id}"},
    )
    assert count == 2


async def test_no_label_holds_an_id_an_address_or_a_query() -> None:
    app, metrics = instrumented()
    await exercise(app)
    routes = {"/api/v1/shifts/{id}", "/auth/login", "/boom", "unmatched"}
    for value in label_values(metrics):
        assert not UUID_PATTERN.search(value), value
        assert "@" not in value and "?" not in value, value
    route_labels = {
        sample.labels["route"]
        for family in metrics.registry.collect()
        for sample in family.samples
        if "route" in sample.labels
    }
    assert route_labels <= routes


def test_build_info_carries_the_version() -> None:
    metrics = HttpMetrics(version="abc123")
    assert metrics.registry.get_sample_value("hds_build_info", {"version": "abc123"}) == 1
    assert b"hds_build_info" in generate_latest(metrics.registry)


@pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
async def test_non_http_traffic_passes_through_uncounted(scope_type: str) -> None:
    seen: list[str] = []

    async def inner(scope: dict[str, object], receive: object, send: object) -> None:
        seen.append(str(scope["type"]))

    metrics = HttpMetrics(version="v")
    middleware = MetricsMiddleware(inner, metrics=metrics)  # type: ignore[arg-type]
    await middleware({"type": scope_type}, None, None)  # type: ignore[arg-type]
    assert seen == [scope_type]
    assert metrics.registry.get_sample_value("hds_http_requests_total", {}) is None
