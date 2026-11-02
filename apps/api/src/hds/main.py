import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from hds.core.error_handler import register_exception_handlers
from hds.core.logging import configure_logging
from hds.core.openapi import install_openapi


def create_app() -> FastAPI:
    configure_logging()

    application = FastAPI(
        title="HDS API",
        version="0.1.0",
        docs_url="/docs",
        openapi_url="/api/v1/openapi.json",
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @application.middleware("http")
    async def request_id_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = str(uuid.uuid4())
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        return response

    register_exception_handlers(application)
    install_openapi(application)

    @application.get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    from hds.routers.auth import router as auth_router
    from hds.routers.clients import router as clients_router
    from hds.routers.employees import router as employees_router
    from hds.routers.groups import router as groups_router
    from hds.routers.invitations import router as invitations_router
    from hds.routers.me import router as me_router
    from hds.routers.projects import router as projects_router
    from hds.routers.qualifications import router as qualifications_router
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

    return application


app = create_app()
