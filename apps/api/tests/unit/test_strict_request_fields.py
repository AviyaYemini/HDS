"""Every request body takes integers and booleans exactly as JSON sends them (R165).

Pydantic's lax types read ``true`` as 1 and ``"3"`` as 3, which the published schema does not
allow, so the contract suite finds them one field at a time (R111, R165). This walks every route's
body model, nested models and lists included, and fails on an ``int`` without ``WHOLE_NUMBER`` (or
``Strict``) and a ``bool`` without ``Strict``. Response models and query parameters are not bodies.
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
    elif (
        isinstance(annotation, type)
        and issubclass(annotation, BaseModel)
        and annotation not in seen
    ):
        seen.add(annotation)
        for name, field in annotation.model_fields.items():
            _lax(field.annotation, list(field.metadata), f"{where}.{name}", seen, found)


def test_no_request_body_takes_a_lax_integer_or_boolean() -> None:
    found: list[str] = []
    for route in _routes(app.routes):
        for param in route.dependant.body_params:
            info = param.field_info
            method = sorted(route.methods or [])[0]
            _lax(info.annotation, list(info.metadata), f"{method} {route.path}", set(), found)
    assert found == [], "lax fields in request bodies:\n" + "\n".join(sorted(set(found)))


def test_the_walk_finds_a_lax_field() -> None:
    """The check itself fails on a plain int or bool, so a passing run means something."""

    class Probe(BaseModel):
        count: int
        flag: bool

    found: list[str] = []
    _lax(Probe, [], "Probe", set(), found)
    assert found == ["Probe.count: int", "Probe.flag: bool"]
