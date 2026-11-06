"""Health router: the alive and ready checks for probes and operators (specs/006 ops.md §1).

Layer: routers. Delegates to ``HealthService``, which ``hds.wiring`` builds; HTTP only. None of
these paths is in the OpenAPI document, none needs a sign-in, and the cluster's ingress routes none
of them: they serve probes and operators, not the client (as ``/health`` always has).
"""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from hds.core.config import get_settings
from hds.core.health import body
from hds.services.health import HealthService
from hds.wiring import health

router = APIRouter(include_in_schema=False)


@router.get("/livez")
async def livez() -> dict[str, object]:
    """Report that the process is up; it touches nothing outside the process.

    It depends on nothing, not even the migration history, so a fault there can never turn
    liveness into a restart loop.

    Returns:
        ``{"status": "ok", "version": ...}``.
    """
    return body(get_settings().HDS_VERSION)


@router.get("/health")
async def health_alias() -> dict[str, object]:
    """The same as ``/livez``, kept for the probes and the compose file that use this path."""
    return body(get_settings().HDS_VERSION)


@router.get("/readyz")
async def readyz(svc: HealthService = Depends(health)) -> JSONResponse:
    """Report whether the process can serve: the database answers and is not behind the code.

    Returns:
        200 with ``{"status": "ok", ...}``, or 503 with ``{"status": "not_ready", ...}`` and
        ``failing`` naming ``database`` or ``migrations``, within 2 seconds.
    """
    ok, body = await svc.ready()
    return JSONResponse(body, status_code=200 if ok else 503)
