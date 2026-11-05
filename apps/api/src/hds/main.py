"""Application entry point: builds the FastAPI app and mounts every router.

Layer: composition root. It is the one module allowed to import every layer; nothing imports
it except the ASGI server, the OpenAPI export script and the tests. Run it with
``uvicorn hds.main:app``.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from hds.core.config import configure_email_validation, get_settings
from hds.core.error_handler import register_exception_handlers
from hds.core.logging import configure_logging
from hds.core.openapi import install_openapi
from hds.core.request_context import new_request_id, reset_request_id, set_request_id


def create_app() -> FastAPI:
    """Build and configure the application.

    Order matters: logging and e-mail validation are configured first, CORS and the request-id
    middleware wrap every route, the exception handlers and the OpenAPI wrapper are installed
    before the routers so their responses are documented. The OpenAPI document is served at
    ``/api/v1/openapi.json``.

    Returns:
        A new ``FastAPI`` instance; tests call this to get an app per test run.
    """
    configure_logging()
    configure_email_validation(get_settings())

    application = FastAPI(
        title="HDS API",
        version="0.1.0",
        docs_url="/docs",
        openapi_url="/api/v1/openapi.json",
    )

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

    @application.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        """Report liveness for container and load-balancer probes.

        Deliberately excluded from the OpenAPI document and open to anyone: it touches no
        database, so it says the process is up, not that the database is reachable.

        Returns:
            ``{"status": "ok"}``.
        """
        return {"status": "ok"}

    from hds.routers.auth import router as auth_router
    from hds.routers.availability import router as availability_router
    from hds.routers.clients import router as clients_router
    from hds.routers.employees import router as employees_router
    from hds.routers.groups import router as groups_router
    from hds.routers.invitations import router as invitations_router
    from hds.routers.me import router as me_router
    from hds.routers.me_shifts import router as me_shifts_router
    from hds.routers.projects import router as projects_router
    from hds.routers.qualifications import router as qualifications_router
    from hds.routers.shift_types import router as shift_types_router
    from hds.routers.shifts import router as shifts_router
    from hds.routers.sites import router as sites_router

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

    return application


app = create_app()
