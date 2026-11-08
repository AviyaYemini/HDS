"""Request bodies take JSON's own integers and booleans (R165) and text without NUL (R184).

Pydantic's lax types read ``true`` as 1 and ``"3"`` as 3, which the published schema does not
allow, so the contract suite finds them one field at a time (R111, R165). This walks every route's
body model, nested models and lists included, and fails on an ``int`` without ``WHOLE_NUMBER`` (or
``Strict``), a ``bool`` without ``Strict``, and a ``str`` without a pattern: PostgreSQL text cannot
hold NUL, so a free-text field uses ``hds.core.types.text`` (or a type built on it) and answers
422, not 500. Response models and query parameters are not bodies.
"""

import types
import typing
from collections.abc import Iterator
from typing import Any

from fastapi.routing import APIRoute
from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator
from pydantic.types import Strict

from hds.core.types import whole_number
from hds.main import app


def _routes(items: Any) -> Iterator[APIRoute]:
    for route in items:
        if isinstance(route, APIRoute):
            yield route
        elif hasattr(route, "original_router"):
            yield from _routes(route.original_router.routes)
        elif hasattr(route, "routes"):
            yield from _routes(route.routes)


def _safe(metadata: list[Any]) -> bool:
    return any(
        (isinstance(m, Strict) and m.strict)
        or (isinstance(m, BeforeValidator) and m.func is whole_number)
        for m in metadata
    )


def _lax(
    annotation: Any, metadata: list[Any], where: str, seen: set[type], found: list[str]
) -> None:
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        base, *more = typing.get_args(annotation)
        _lax(base, [*metadata, *more], where, seen, found)
    elif origin in (typing.Union, types.UnionType):
        for arm in typing.get_args(annotation):
            _lax(arm, metadata, where, seen, found)
    elif origin in (list, set, tuple, frozenset):
        for item in typing.get_args(annotation):
            _lax(item, [], f"{where}[]", seen, found)
    elif annotation is bool and not any(isinstance(m, Strict) and m.strict for m in metadata):
        found.append(f"{where}: bool")
    elif annotation is int and not _safe(metadata):
        found.append(f"{where}: int")
    elif annotation is str and not any(getattr(m, "pattern", None) for m in metadata):
        found.append(f"{where}: str")
    elif (
        isinstance(annotation, type)
        and issubclass(annotation, BaseModel)
        and annotation not in seen
    ):
        seen.add(annotation)
        for name, field in annotation.model_fields.items():
            _lax(field.annotation, list(field.metadata), f"{where}.{name}", seen, found)


def test_no_request_body_takes_a_lax_integer_boolean_or_text() -> None:
    found: list[str] = []
    for route in _routes(app.routes):
        for param in route.dependant.body_params:
            info = param.field_info
            method = sorted(route.methods or [])[0]
            _lax(info.annotation, list(info.metadata), f"{method} {route.path}", set(), found)
    assert found == [], "lax fields in request bodies:\n" + "\n".join(sorted(set(found)))


def test_the_walk_finds_a_lax_field() -> None:
    """The check itself fails on a plain int, bool or str, so a passing run means something."""

    class Probe(BaseModel):
        count: int
        flag: bool
        note: str

    found: list[str] = []
    _lax(Probe, [], "Probe", set(), found)
    assert found == ["Probe.count: int", "Probe.flag: bool", "Probe.note: str"]


def test_a_nul_in_free_text_is_a_validation_error() -> None:
    """The template name the contract suite sent with a NUL (R184) is refused, not stored."""
    import pytest
    from pydantic import ValidationError

    from hds.routers.roster_extras import TemplateCreate

    with pytest.raises(ValidationError):
        TemplateCreate.model_validate({"name": "a\u0000b", "week": "2026-11-08"})
    assert TemplateCreate.model_validate({"name": "שבוע א", "week": "2026-11-08"}).name == "שבוע א"


def test_an_infinite_number_is_a_validation_error_not_a_crash() -> None:
    """JSON's 1e400 reads as infinity; a whole number refuses it like any other bad value."""
    import math

    import pytest

    from hds.core.types import whole_number

    for value in (math.inf, -math.inf, math.nan):
        with pytest.raises(ValueError, match="whole number"):
            whole_number(value)
    assert whole_number(40.0) == 40


def test_the_text_guard_keeps_a_fields_own_minimum() -> None:
    """``text()`` leaves the minimum alone, so an initial password still publishes 12 (R184)."""
    schemas = app.openapi()["components"]["schemas"]
    for name in ("UserCreateRequest", "ClientUserCreateRequest"):
        assert schemas[name]["properties"]["initial_password"]["minLength"] == 12
