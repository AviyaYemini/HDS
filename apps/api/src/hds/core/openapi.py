"""OpenAPI document: document the problem-details error responses the API really returns.

FastAPI only documents 2xx and its own 422. Everything else this API returns is
``application/problem+json`` (``ProblemDetail``, as in the contract). The rules below add
those responses per operation so the generated document matches behaviour (Schemathesis).
"""

from typing import Any

from fastapi import FastAPI

_PROBLEM = "application/problem+json"
_DESCRIPTIONS = {
    "400": "The request body could not be parsed.",
    "401": "Missing, invalid or expired credentials.",
    "403": "Authenticated but the role or data scope does not allow this.",
    "404": "The referenced resource does not exist in the caller's organisation.",
    "405": "Method not allowed for this resource.",
    "409": "The request conflicts with the current state (duplicate, last admin, in use).",
    "422": "The request is invalid.",
    "423": "The account is temporarily locked after repeated failed sign-ins.",
}
_PROBLEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["type", "title", "status", "code"],
    "properties": {
        "type": {"type": "string"},
        "title": {"type": "string"},
        "status": {"type": "integer"},
        "detail": {"type": "string"},
        "code": {"type": "string", "description": "Stable machine-readable error code."},
        "instance": {"type": "string"},
        "field": {"type": "string"},
        "resource": {"type": "string"},
        "id": {"type": "string"},
        "errors": {"type": "array", "items": {"type": "object"}},
    },
}
_METHODS = {"get", "put", "post", "delete", "patch"}
# Public operations that can answer 401 (bad credentials / tokens).
_PUBLIC_401 = {
    "/auth/login",
    "/auth/refresh",
    "/auth/invite/accept",
    "/auth/password-reset/confirm",
}


def _problem(status: str) -> dict[str, Any]:
    return {
        "description": _DESCRIPTIONS[status],
        "content": {_PROBLEM: {"schema": {"$ref": "#/components/schemas/ProblemDetail"}}},
    }


def _body_references_ids(operation: dict[str, Any], schemas: dict[str, Any]) -> bool:
    content = operation.get("requestBody", {}).get("content", {})
    for media in content.values():
        ref = media.get("schema", {}).get("$ref", "")
        props = schemas.get(ref.rsplit("/", 1)[-1], {}).get("properties", {})
        if any(name.endswith("_id") or name == "ids" for name in props):
            return True
    return False


def _statuses(
    path: str, method: str, operation: dict[str, Any], schemas: dict[str, Any]
) -> set[str]:
    found: set[str] = {"422"}  # request validation (problem+json, see error_handler)
    if operation.get("security"):
        found |= {"401", "403"}
        if method != "get":
            found.add("409")
    if operation.get("requestBody"):
        found.add("400")
    parameters = operation.get("parameters", [])
    if (
        "{" in path
        or _body_references_ids(operation, schemas)
        or any(p.get("in") == "query" and p["name"].endswith("_id") for p in parameters)
    ):
        found.add("404")
    if path in _PUBLIC_401:
        found.add("401")
    if path == "/auth/login":
        found.add("423")
    if method != "get" and "{" not in path and operation.get("security"):
        found.add("409")
    return found


def install_openapi(app: FastAPI) -> None:
    original = app.openapi

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema
        schema = original()
        components = schema.setdefault("components", {}).setdefault("schemas", {})
        components["ProblemDetail"] = _PROBLEM_SCHEMA
        components.pop("HTTPValidationError", None)
        components.pop("ValidationError", None)
        for path, item in schema.get("paths", {}).items():
            for method, operation in item.items():
                if method not in _METHODS:
                    continue
                responses = operation.setdefault("responses", {})
                for status in _statuses(path, method, operation, components):
                    responses[status] = _problem(status)
        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
