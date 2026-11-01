from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from hds.core.exceptions import (
    AuthError,
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
)


def _problem(
    status: int,
    title: str,
    detail: str,
    code: str,
    request: Request,
    extra: dict[str, Any] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": f"https://hds.app/errors/{code.replace('.', '/')}",
        "title": title,
        "status": status,
        "detail": detail,
        "code": code,
        "instance": str(request.url),
    }
    if extra:
        body.update(extra)
    return JSONResponse(
        status_code=status,
        content=body,
        headers={"Content-Type": "application/problem+json"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AuthError)
    async def handle_auth_error(request: Request, exc: AuthError) -> JSONResponse:
        status = 423 if exc.code == "auth.account_locked" else 401
        return _problem(status, "Unauthorised", exc.detail, exc.code, request)

    @app.exception_handler(ForbiddenError)
    async def handle_forbidden(request: Request, exc: ForbiddenError) -> JSONResponse:
        return _problem(403, "Forbidden", exc.detail, exc.code, request)

    @app.exception_handler(NotFoundError)
    async def handle_not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        return _problem(
            404,
            "Not Found",
            str(exc),
            "resource.not_found",
            request,
            {"resource": exc.resource, "id": str(exc.id)},
        )

    @app.exception_handler(ConflictError)
    async def handle_conflict(request: Request, exc: ConflictError) -> JSONResponse:
        extra = {"field": exc.field} if exc.field else None
        return _problem(409, "Conflict", exc.detail, exc.code, request, extra)

    @app.exception_handler(IntegrityError)
    async def handle_integrity(request: Request, exc: IntegrityError) -> JSONResponse:
        # Database CHECK / unique violations that slipped past service validation.
        return _problem(
            422,
            "Unprocessable Entity",
            "The request violates a data constraint.",
            "validation.constraint_violation",
            request,
        )

    @app.exception_handler(DomainValidationError)
    async def handle_validation(request: Request, exc: DomainValidationError) -> JSONResponse:
        return _problem(422, "Unprocessable Entity", exc.detail, exc.code, request)
