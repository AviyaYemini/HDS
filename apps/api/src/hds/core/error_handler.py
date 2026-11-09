"""Translate exceptions into RFC 9457 ``application/problem+json`` responses.

Layer: core. Depends on ``hds.core.exceptions`` and ``hds.core.request_context``; it MUST NOT
import services, repositories or routers. Domain code raises typed exceptions and never builds
HTTP responses; this module owns the status code and body shape for every error.
"""

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
    PreconditionFailedError,
    RateLimitedError,
    ServiceBusyError,
)
from hds.core.request_context import get_request_id


def _problem(
    status: int,
    title: str,
    detail: str,
    code: str,
    request: Request,
    extra: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Build a problem-details response carrying the current request id."""
    body: dict[str, Any] = {
        "type": f"https://hds.app/errors/{code.replace('.', '/')}",
        "title": title,
        "status": status,
        "detail": detail,
        "code": code,
        "instance": str(request.url),
        "request_id": get_request_id(),
    }
    if extra:
        body.update(extra)
    return JSONResponse(
        status_code=status,
        content=body,
        headers={"Content-Type": "application/problem+json", **(headers or {})},
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Attach the problem-details handlers to ``app``.

    Status mapping: ``AuthError`` 401 (423 for ``auth.account_locked``), ``ForbiddenError``
    403, ``NotFoundError`` 404, ``ConflictError`` 409, ``PreconditionFailedError`` 412,
    ``DomainValidationError``,
    request-validation errors and database ``IntegrityError``/``DataError`` 422, malformed
    body 400, wrong method 405 with an accurate ``Allow`` header. Other database errors are
    deliberately left unmapped so outages surface as 5xx.

    Args:
        app: The application to register the handlers on.
    """

    @app.exception_handler(AuthError)
    async def handle_auth_error(request: Request, exc: AuthError) -> JSONResponse:
        """Answer 401, or 423 for a locked account so clients can show a lockout message."""
        status = 423 if exc.code == "auth.account_locked" else 401
        return _problem(status, "Unauthorised", exc.detail, exc.code, request)

    @app.exception_handler(RateLimitedError)
    async def handle_rate_limited(request: Request, exc: RateLimitedError) -> JSONResponse:
        """Answer 429 with ``Retry-After`` when a network address is over its limit."""
        return _problem(
            429,
            "Too Many Requests",
            exc.detail,
            exc.code,
            request,
            headers={"Retry-After": str(exc.retry_after)},
        )

    @app.exception_handler(ServiceBusyError)
    async def handle_busy(request: Request, exc: ServiceBusyError) -> JSONResponse:
        """Answer 503 with ``Retry-After`` when the password-hashing queue is full."""
        return _problem(
            503,
            "Service Unavailable",
            exc.detail,
            exc.code,
            request,
            headers={"Retry-After": str(exc.retry_after)},
        )

    @app.exception_handler(ForbiddenError)
    async def handle_forbidden(request: Request, exc: ForbiddenError) -> JSONResponse:
        """Answer 403 with the service-supplied error code."""
        return _problem(403, "Forbidden", exc.detail, exc.code, request)

    @app.exception_handler(NotFoundError)
    async def handle_not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        """Answer 404 with a single code so a missing record and another tenant's look alike."""
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
        """Answer 409, naming the offending request field when the service knows it."""
        extra: dict[str, Any] = {"field": exc.field} if exc.field else {}
        if exc.errors:
            extra["errors"] = exc.errors
        return _problem(409, "Conflict", exc.detail, exc.code, request, extra or None)

    @app.exception_handler(PreconditionFailedError)
    async def handle_precondition(request: Request, exc: PreconditionFailedError) -> JSONResponse:
        """Answer 412: the record changed since the caller read it (007 FR-041)."""
        return _problem(412, "Precondition Failed", exc.detail, exc.code, request)

    @app.exception_handler(DataError)
    @app.exception_handler(IntegrityError)
    async def handle_integrity(request: Request, exc: IntegrityError | DataError) -> JSONResponse:
        """Answer 422 for a database constraint that validation did not catch first."""
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
        """Answer 422 for a business-rule violation raised by a service, with its rule breaks."""
        extra = {"errors": exc.errors} if exc.errors else None
        return _problem(422, "Unprocessable Entity", exc.detail, exc.code, request, extra)

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Answer 422 with the field errors, without echoing the rejected input back."""
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
        """Normalise framework errors: 400 and 405 as problem+json, others passed through."""
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
