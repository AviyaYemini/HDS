"""The pay settings in the models agree with migration 022 and with the pay package (005 T007).

``alembic check`` does not compare check constraints, so this test does: the migration keeps its own
frozen copy of the bounds, and the two must say the same. The organisation's column defaults must be
the pay package's defaults.

Covers: 005 FR-003, FR-006; data-model.md.
"""

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

from sqlalchemy import CheckConstraint, Table

from hds.models.organisation import Organisation
from hds.models.pay_columns import pay_setting_checks
from hds.models.project import Project
from hds.payroll import DEFAULT_SETTINGS

MIGRATION = Path(__file__).resolve().parents[2] / "alembic" / "versions" / "022_pay_reports.py"


def migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("migration_022", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def as_pairs(checks: list[CheckConstraint]) -> list[tuple[str, str]]:
    return [(str(c.name), str(c.sqltext)) for c in checks]


def test_the_migration_states_the_same_bounds_as_the_models() -> None:
    frozen = migration()

    for table, optional in (("organisations", False), ("projects", True)):
        assert frozen._checks(table, optional) == as_pairs(pay_setting_checks(table, optional))


def test_the_models_carry_every_bound() -> None:
    for model, table, optional in (
        (Organisation, "organisations", False),
        (Project, "projects", True),
    ):
        table_ = model.__table__
        assert isinstance(table_, Table)
        names = {c.name for c in table_.constraints if isinstance(c, CheckConstraint)}
        assert {n for n, _ in as_pairs(pay_setting_checks(table, optional))} <= names


def defaults_of(model: type[Any]) -> dict[str, Any]:
    columns = model.__table__.columns
    return {c.name: c.default.arg for c in columns if c.name.startswith("pay_") and c.default}


def test_an_organisations_defaults_are_the_pay_packages() -> None:
    d = DEFAULT_SETTINGS
    assert defaults_of(Organisation) == {
        "pay_day_normal_minutes": d.day_normal_minutes,
        "pay_night_normal_minutes": d.night_normal_minutes,
        "pay_tier1_minutes": d.tier1_minutes,
        "pay_overtime1_pct": d.overtime1_pct,
        "pay_overtime2_pct": d.overtime2_pct,
        "pay_rest_start_weekday": d.rest.start_weekday,
        "pay_rest_start_time": d.rest.start_time,
        "pay_rest_end_weekday": d.rest.end_weekday,
        "pay_rest_end_time": d.rest.end_time,
        "pay_rest_pct": d.rest_pct,
        "pay_increase1_points": d.increase1_points,
        "pay_increase2_points": d.increase2_points,
    }
    frozen = {name: default for name, (_, default) in migration().DEFAULTS.items()}
    assert set(frozen) == set(defaults_of(Organisation))
