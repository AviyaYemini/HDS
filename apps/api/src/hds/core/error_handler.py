from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DataError, IntegrityError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.routing import Match

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

    @app.exception_handler(DataError)
    @app.exception_handler(IntegrityError)
    async def handle_integrity(request: Request, exc: IntegrityError | DataError) -> JSONResponse:
        # CHECK / unique violations and over-long / out-of-range values that slipped past
        # validation. Connection and other operational errors are NOT mapped: they stay 5xx.
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

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = [
            {"loc": [str(part) for part in err["loc"]], "msg": err["msg"], "type": err["type"]}
            for err in exc.errors()
        ]
        return _problem(
            422,
            "Unprocessable Entity",
            "The request body or parameters are invalid.",
            "validation.request_invalid",
            request,
            {"errors": errors},
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if exc.status_code == 400:
            return _problem(
                400,
                "Bad Request",
                "The request could not be parsed.",
                "http.bad_request",
                request,
            )
        if exc.status_code != 405:
            return JSONResponse(
                {"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers
            )
        # Each method is its own route, so Starlette's Allow lists only one of them.
        allowed: set[str] = set()
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match != Match.NONE:
                allowed |= set(getattr(route, "methods", None) or ())
        response = _problem(
            405,
            "Method Not Allowed",
            "This method is not allowed for the resource.",
            "http.method_not_allowed",
            request,
        )
        response.headers["Allow"] = ", ".join(sorted(allowed))
        return response
