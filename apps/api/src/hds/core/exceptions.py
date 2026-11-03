"""Typed domain errors raised by services and dependencies.

Layer: core. Imports nothing from other ``hds`` packages. These classes carry a stable
machine-readable ``code`` (also the key in the locale files) and never an HTTP status; the
status mapping lives in ``hds.core.error_handler``.
"""

import uuid


class AuthError(Exception):
    """Authentication failed or is missing (HTTP 401; 423 when the account is locked)."""

    def __init__(self, code: str, detail: str) -> None:
        """Create the error.

        Args:
            code: Stable error code such as ``auth.invalid_credentials``.
            detail: English explanation for the response body.
        """
        self.code = code
        self.detail = detail
        super().__init__(detail)


class ForbiddenError(Exception):
    """The caller is authenticated but not allowed to do this (HTTP 403)."""

    def __init__(self, code: str, detail: str) -> None:
        """Create the error.

        Args:
            code: Stable error code such as ``users.manager_required``.
            detail: English explanation for the response body.
        """
        self.code = code
        self.detail = detail
        super().__init__(detail)


class NotFoundError(Exception):
    """The addressed record does not exist in the caller's organisation (HTTP 404).

    Records of other organisations are reported the same way, so existence never leaks across
    tenants.
    """

    def __init__(self, resource: str, id: uuid.UUID) -> None:
        """Create the error.

        Args:
            resource: Resource kind such as ``employee``.
            id: Identifier that was looked up.
        """
        self.resource = resource
        self.id = id
        super().__init__(f"{resource} {id} not found")


class ConflictError(Exception):
    """The request clashes with current state, e.g. duplicate email or last admin (HTTP 409)."""

    def __init__(self, code: str, detail: str, field: str | None = None) -> None:
        """Create the error.

        Args:
            code: Stable error code such as ``users.email_taken``.
            detail: English explanation for the response body.
            field: Request field that caused the conflict, if one does.
        """
        self.code = code
        self.detail = detail
        self.field = field
        super().__init__(detail)


class DomainValidationError(Exception):
    """A business rule rejected otherwise well-formed input (HTTP 422)."""

    def __init__(self, code: str, detail: str) -> None:
        """Create the error.

        Args:
            code: Stable error code such as ``validation.rate_not_positive``.
            detail: English explanation for the response body.
        """
        self.code = code
        self.detail = detail
        super().__init__(detail)


class ServiceBusyError(Exception):
    """The password-hashing queue is full; try again shortly (HTTP 503)."""

    def __init__(self, retry_after: int = 1) -> None:
        """Create the error.

        Args:
            retry_after: Seconds the client should wait, sent as ``Retry-After``.
        """
        self.code = "service.busy"
        self.detail = "The service is busy; try again shortly."
        self.retry_after = retry_after
        super().__init__(self.detail)


class RateLimitedError(Exception):
    """Too many failed sign-ins from one network address (HTTP 429)."""

    def __init__(self, retry_after: int) -> None:
        """Create the error.

        Args:
            retry_after: Seconds until the window ends, sent as ``Retry-After``.
        """
        self.code = "auth.rate_limited"
        self.detail = "Too many failed sign-ins from this network; try again later."
        self.retry_after = retry_after
        super().__init__(self.detail)
