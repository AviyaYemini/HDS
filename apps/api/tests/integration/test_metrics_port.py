"""The API's metrics are on their own port, never the public one (T015).

Started through the app's lifespan, as uvicorn starts it, the metrics server answers on
``METRICS_PORT`` and ``/metrics`` on the public app is a 404, so the ingress can never publish them.

Covers: specs/006-deploy-operations FR-007, FR-009; contracts/ops.md section 2.
"""

import socket

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from hds.core.config import Settings
from hds.main import create_app


def free_port() -> int:
    """A port nothing listens on, from the operating system."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


async def test_the_metrics_answer_on_their_port_and_not_on_the_public_one() -> None:
    port = free_port()
    app = create_app(metrics_port=port)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://testserver"
        ) as public:
            await public.get("/livez")
            assert (await public.get("/metrics")).status_code == 404
        async with httpx.AsyncClient() as operator:
            scraped = await operator.get(f"http://127.0.0.1:{port}/metrics")
    assert scraped.status_code == 200
    assert "hds_build_info" in scraped.text
    assert 'route="/livez"' in scraped.text
    assert 'route="unmatched"' in scraped.text


async def test_with_the_port_at_zero_no_server_starts() -> None:
    app = create_app(metrics_port=0)
    async with app.router.lifespan_context(app):
        assert app.state.metrics_server is None


async def test_a_taken_port_stops_start_up_naming_the_setting() -> None:
    with socket.socket() as holder:
        holder.bind(("0.0.0.0", 0))
        holder.listen()
        port = int(holder.getsockname()[1])
        app = create_app(metrics_port=port)
        with pytest.raises(RuntimeError, match="METRICS_PORT"):
            async with app.router.lifespan_context(app):
                pass


def test_the_metrics_port_is_off_unless_set() -> None:
    assert Settings.model_fields["METRICS_PORT"].default == 0


async def test_a_broken_migration_history_stops_start_up(monkeypatch: pytest.MonkeyPatch) -> None:
    import hds.wiring as wiring

    def broken() -> None:
        raise RuntimeError("the migration history in /app/alembic has no single head")

    wiring.migrations.cache_clear()
    monkeypatch.setattr(wiring, "load_migrations", broken)
    try:
        app = create_app(metrics_port=0)
        with pytest.raises(RuntimeError, match="single head"):
            async with app.router.lifespan_context(app):
                pass
    finally:
        wiring.migrations.cache_clear()
