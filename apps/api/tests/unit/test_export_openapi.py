"""T124/C2: the drift check compares method, path, 2xx codes and query parameters."""

from typing import Any

from hds.scripts.export_openapi import diff, query_params


def _doc(parameters: list[dict[str, Any]], *, path_level: bool = False, **extra: Any) -> dict:
    op: dict[str, Any] = {"responses": {"200": {"description": "ok"}}}
    item: dict[str, Any] = {"get": op}
    if path_level:
        item["parameters"] = parameters
    else:
        op["parameters"] = parameters
    return {"paths": {"/employees": item}, **extra}


def _q(name: str, schema: dict[str, Any], required: bool = False) -> dict[str, Any]:
    return {"name": name, "in": "query", "required": required, "schema": schema}


def test_identical_parameters_agree() -> None:
    doc = _doc([_q("search", {"type": "string"}), _q("is_active", {"type": "boolean"})])
    assert diff(doc, doc) == []


def test_missing_parameter_in_implementation_is_reported() -> None:
    contract = _doc([_q("search", {"type": "string"}), _q("role", {"type": "string"})])
    generated = _doc([_q("search", {"type": "string"})])
    problems = diff(contract, generated)
    assert any("role" in p and "GET /employees" in p for p in problems), problems


def test_extra_parameter_in_implementation_is_reported() -> None:
    contract = _doc([_q("search", {"type": "string"})])
    generated = _doc([_q("search", {"type": "string"}), _q("limit", {"type": "integer"})])
    assert any("limit" in p for p in diff(contract, generated))


def test_type_difference_is_reported() -> None:
    contract = _doc([_q("is_active", {"type": "boolean"})])
    generated = _doc([_q("is_active", {"type": "string"})])
    assert any(
        "is_active" in p and "boolean" in p and "string" in p for p in diff(contract, generated)
    )


def test_required_difference_is_reported() -> None:
    contract = _doc([_q("search", {"type": "string"}, required=True)])
    generated = _doc([_q("search", {"type": "string"}, required=False)])
    assert any("search" in p and "required" in p for p in diff(contract, generated))


def test_refs_and_nullable_unions_are_resolved() -> None:
    components = {
        "components": {
            "schemas": {"UserRole": {"type": "string", "enum": ["admin"]}},
            "parameters": {"Role": _q("role", {"$ref": "#/components/schemas/UserRole"})},
        }
    }
    contract = _doc([{"$ref": "#/components/parameters/Role"}], **components)
    generated = _doc(
        [_q("role", {"anyOf": [{"$ref": "#/components/schemas/UserRole"}, {"type": "null"}]})],
        **components,
    )
    assert diff(contract, generated) == []


def test_path_level_parameters_are_included_and_non_query_ignored() -> None:
    params = [
        _q("search", {"type": "string"}),
        {"name": "Idempotency-Key", "in": "header", "schema": {"type": "string"}},
    ]
    got = query_params(_doc(params, path_level=True))
    assert got == {("GET", "/employees"): {"search": ("string", False)}}


def test_status_code_and_path_checks_still_work() -> None:
    contract = _doc([])
    generated = {"paths": {"/other": {"get": {"responses": {"200": {}}}}}}
    problems = diff(contract, generated)
    assert any("not implemented" in p for p in problems)
    assert any("not in contract" in p for p in problems)
