"""T125 (Constitution VII): timestamps are timezone-aware UTC; rates and durations are positive.

Money never touches ``float``. Static checks over models, schemas and source.

Covers: FR-028; Constitution VII.
"""

import ast
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, Column, DateTime, Float, Numeric
from sqlalchemy import types as sqltypes

import hds.models  # noqa: F401  (register every table)
from hds.main import app
from hds.models.base import Base
from tests.settings_support import make_settings

_SRC = Path(__file__).resolve().parents[2] / "src" / "hds"
_MONEY_WORDS = ("rate", "price", "amount", "cost", "fee", "salary")


def _py_files() -> list[Path]:
    return [p for p in _SRC.rglob("*.py") if "__pycache__" not in p.parts]


def _columns() -> Iterator[tuple[str, Column[Any]]]:
    for table in Base.metadata.sorted_tables:
        yield from ((table.name, c) for c in table.columns)


# ── timestamps ───────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("table", "column"),
    [(t, c) for t, c in _columns() if isinstance(c.type, sqltypes.DateTime)],
    ids=lambda v: getattr(v, "name", v),
)
def test_every_datetime_column_is_timezone_aware(table: str, column) -> None:  # type: ignore[no-untyped-def]
    assert isinstance(column.type, DateTime)
    assert column.type.timezone is True, f"{table}.{column.name} is a naive DateTime"


def test_there_are_datetime_columns_to_check() -> None:
    assert sum(isinstance(c.type, DateTime) for _, c in _columns()) >= 10


def test_source_never_creates_naive_datetimes() -> None:
    offenders: list[str] = []
    for path in _py_files():
        for node in ast.walk(ast.parse(path.read_text())):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                continue
            name = node.func.attr
            if name in {"utcnow", "utcfromtimestamp"}:
                offenders.append(f"{path.name}:{node.lineno} {name}()")
            if (
                name in {"now", "today", "fromtimestamp"}
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "datetime"
            ):
                has_tz = bool(node.args) or any(k.arg == "tz" for k in node.keywords)
                if name == "today" or not has_tz:
                    offenders.append(f"{path.name}:{node.lineno} datetime.{name}() without tz")
    assert not offenders, offenders


def test_server_defaults_for_timestamps_use_database_clock() -> None:
    for table in Base.metadata.sorted_tables:
        col = table.columns.get("created_at")
        if col is not None:
            assert col.server_default is not None, f"{table.name}.created_at has no default"


# ── positive rates and durations ─────────────────────────────────────────────────────


def test_settings_durations_are_positive() -> None:
    s = make_settings()
    for name in (
        "ACCESS_TOKEN_TTL_SECONDS",
        "REFRESH_TOKEN_TTL_DAYS",
        "LOGIN_MAX_FAILURES",
        "LOGIN_LOCKOUT_MINUTES",
    ):
        assert getattr(s, name) > 0, name


@pytest.mark.parametrize(
    ("table", "column"),
    [("projects", "billing_rate"), ("users", "default_hourly_rate")],
)
def test_rate_columns_have_a_positive_check_constraint(table: str, column: str) -> None:
    tbl = Base.metadata.tables[table]
    checks = [str(c.sqltext) for c in tbl.constraints if isinstance(c, CheckConstraint)]
    assert any(column in c and "> 0" in c for c in checks), checks


# ── no float in money paths ──────────────────────────────────────────────────────────


def test_money_columns_are_numeric_never_float() -> None:
    money = [(t, c) for t, c in _columns() if any(w in c.name for w in _MONEY_WORDS)]
    assert money, "expected at least one money column"
    for table, col in money:
        assert isinstance(col.type, Numeric) and not isinstance(col.type, Float), (
            f"{table}.{col.name} is {col.type!r}"
        )
        assert col.type.asdecimal is True


def test_no_float_column_types_anywhere() -> None:
    for table, col in _columns():
        assert not isinstance(col.type, Float), f"{table}.{col.name}"


def test_source_has_no_float_annotations_or_calls_on_money() -> None:
    offenders: list[str] = []
    for path in _py_files():
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id == "float":
                offenders.append(f"{path.relative_to(_SRC)}:{node.lineno}")
            if isinstance(node, ast.Attribute) and node.attr in {"Float", "REAL", "DOUBLE"}:
                offenders.append(f"{path.relative_to(_SRC)}:{node.lineno}")
    assert not offenders, offenders


def test_api_response_money_fields_are_strings_in_openapi() -> None:
    spec = app.openapi()
    checked = 0
    for name, schema in spec["components"]["schemas"].items():
        if not name.endswith("Read"):
            continue
        for prop, definition in schema.get("properties", {}).items():
            if any(w in prop for w in _MONEY_WORDS):
                checked += 1
                flat = str(definition)
                assert "'number'" not in flat, f"{name}.{prop} serialises as a JSON float"
    assert checked >= 2


def _project_body(billing_rate: str) -> str:
    return (
        '{"client_id":"00000000-0000-0000-0000-000000000001",'
        '"site_id":"00000000-0000-0000-0000-000000000001","name":"x","type":"security",'
        f'"billing_rate":{billing_rate},"start_date":"2026-01-01"}}'
    )


@pytest.mark.parametrize("literal", ["0.1", "10.10", "75.0000", "0.0001"])
def test_decimal_strings_parse_into_exact_decimals(literal: str) -> None:
    from hds.routers.projects import ProjectCreateRequest

    parsed = ProjectCreateRequest.model_validate_json(_project_body(f'"{literal}"'))
    assert parsed.billing_rate == Decimal(literal)


@pytest.mark.parametrize("literal", ["0.1", "10.10", "75.0000", "0.0001"])
def test_json_numbers_are_refused_for_money(literal: str) -> None:
    """A JSON number passes through a binary float in most clients, so it never reaches money."""
    from hds.routers.projects import ProjectCreateRequest

    with pytest.raises(ValidationError):
        ProjectCreateRequest.model_validate_json(_project_body(literal))


def test_decimal_round_trip_is_exact() -> None:
    assert Decimal("0.1") + Decimal("0.2") == Decimal("0.3")
