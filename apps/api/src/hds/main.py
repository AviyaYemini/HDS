"""Application entry point: builds the FastAPI app and mounts every router.

Layer: composition root. It is the one module allowed to import every layer; nothing imports
it except the ASGI server, the OpenAPI export script and the tests. Run it with
``uvicorn hds.main:app``.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from hds.core.config import configure_email_validation, get_settings
from hds.core.error_handler import register_exception_handlers
from hds.core.logging import configure_logging
from hds.core.metrics import HttpMetrics, MetricsMiddleware, serve
from hds.core.openapi import install_openapi
from hds.core.request_context import new_request_id, reset_request_id, set_request_id
from hds.core.tracing import configure as configure_tracing


def create_app(metrics_port: int | None = None) -> FastAPI:
    """Build and configure the application.

    Order matters: logging and e-mail validation are configured first, CORS and the request-id
    middleware wrap every route, the exception handlers and the OpenAPI wrapper are installed
    before the routers so their responses are documented. The OpenAPI document is served at
    ``/api/v1/openapi.json``.

    The metrics middleware is added last, so it is the outermost of the app's own and sees every
    final status. Its registry is served on the operations port by the lifespan, which uvicorn runs
    and the tests' in-process client does not (specs/006 FR-007, FR-009). The trace
    instrumentation wraps everything, so each request and its log lines are in a trace (FR-008a).

    Args:
        metrics_port: The operations port; ``METRICS_PORT`` by default, 0 for none.

    Returns:
        A new ``FastAPI`` instance; tests call this to get an app per test run.
    """
    configure_logging()
    configure_email_validation(get_settings())

    metrics = HttpMetrics(version=get_settings().HDS_VERSION)
    port = get_settings().METRICS_PORT if metrics_port is None else metrics_port

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Read the migrations, serve the metrics on the operations port, and stop them at exit.

        Both fail start-up loudly: a broken migration history with ``load_migrations``' error, a
        taken operations port with one naming ``METRICS_PORT``.
        """
        from hds.wiring import migrations

        migrations()
        try:
            app.state.metrics_server = serve(metrics.registry, port)
        except OSError as error:
            raise RuntimeError(
                f"METRICS_PORT {port} cannot be bound ({error.strerror}); set a free port, or 0"
            ) from error
        try:
            yield
        finally:
            if app.state.metrics_server is not None:
                server, thread = app.state.metrics_server
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    application = FastAPI(
        title="HDS API",
        version="0.1.0",
        docs_url="/docs",
        openapi_url="/api/v1/openapi.json",
        lifespan=lifespan,
    )
    application.state.metrics = metrics
    application.state.metrics_server = None

    application.add_middleware(
        CORSMiddleware,
        allow_origins=get_settings().CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @application.middleware("http")
    async def request_id_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        """Give every request an id, expose it in logs and in the ``X-Request-Id`` header.

        A well-formed incoming id is kept so a caller can correlate its own logs; anything else is
        replaced. The context variable is always reset, even when the handler raises.
        """
        request_id = new_request_id(request.headers.get("X-Request-Id"))
        token = set_request_id(request_id)
        try:
            response = await call_next(request)
        finally:
            reset_request_id(token)
        response.headers["X-Request-Id"] = request_id
        return response

    register_exception_handlers(application)
    install_openapi(application)
    application.add_middleware(MetricsMiddleware, metrics=metrics)
    # Outermost of all, so every request, and every log line inside it, is in its trace: one
    # continued from the request's traceparent, or a new one (specs/006 FR-008a).
    configure_tracing("hds-api", app=application)

    from hds.routers.auth import router as auth_router
    from hds.routers.availability import router as availability_router
    from hds.routers.clients import router as clients_router
    from hds.routers.employees import router as employees_router
    from hds.routers.groups import router as groups_router
    from hds.routers.health import router as health_router
    from hds.routers.invitations import router as invitations_router
    from hds.routers.me import router as me_router
    from hds.routers.me_availability import router as me_availability_router
    from hds.routers.me_shifts import router as me_shifts_router
    from hds.routers.my_pay import router as my_pay_router
    from hds.routers.organisation import router as organisation_router
    from hds.routers.pay_settings import router as pay_settings_router
    from hds.routers.project_managers import router as project_managers_router
    from hds.routers.projects import router as projects_router
    from hds.routers.qualifications import router as qualifications_router
    from hds.routers.reports import router as reports_router
    from hds.routers.requirements import router as requirements_router
    from hds.routers.schedule_jobs import router as schedule_jobs_router
    from hds.routers.scheduling_settings import router as scheduling_settings_router
    from hds.routers.shift_types import router as shift_types_router
    from hds.routers.shifts import router as shifts_router
    from hds.routers.sites import router as sites_router
    from hds.routers.special_days import router as special_days_router

    application.include_router(health_router)
    application.include_router(auth_router)
    application.include_router(invitations_router)
    application.include_router(me_router)
    application.include_router(employees_router)
    application.include_router(groups_router)
    application.include_router(qualifications_router)
    application.include_router(clients_router)
    application.include_router(sites_router)
    application.include_router(projects_router)
    application.include_router(availability_router)
    application.include_router(shift_types_router)
    application.include_router(shifts_router)
    application.include_router(me_shifts_router)
    application.include_router(me_availability_router)
    application.include_router(requirements_router)
    application.include_router(scheduling_settings_router)
    application.include_router(organisation_router)
    application.include_router(schedule_jobs_router)
    application.include_router(pay_settings_router)
    application.include_router(special_days_router)
    application.include_router(project_managers_router)
    application.include_router(my_pay_router)
    application.include_router(reports_router)

    return application


app = create_app()
