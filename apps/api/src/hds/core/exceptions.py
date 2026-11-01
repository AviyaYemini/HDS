import uuid


class AuthError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class ForbiddenError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)


class NotFoundError(Exception):
    def __init__(self, resource: str, id: uuid.UUID) -> None:
        self.resource = resource
        self.id = id
        super().__init__(f"{resource} {id} not found")


class ConflictError(Exception):
    def __init__(self, code: str, detail: str, field: str | None = None) -> None:
        self.code = code
        self.detail = detail
        self.field = field
        super().__init__(detail)


class DomainValidationError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(detail)
