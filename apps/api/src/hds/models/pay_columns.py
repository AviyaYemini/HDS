"""The check constraints of the premium settings, shared by organisations and projects (005).

Layer: models. Imports SQLAlchemy only.

An organisation holds the premium defaults; a project holds the same settings as overrides, ``NULL``
when it follows the default (005 FR-006, FR-006a). The bounds are the same in both places (FR-003):
lengths 1 to 1440 minutes, premium rates 100 to 300 percent, overtime increases above 0 and at most
200 points, weekdays 0 (Sunday) to 6 (Saturday), and a rest window whose start and end differ. A
project's rest window is one setting: its four columns are all set or all ``NULL``. The SQL is
portable so the SQLite unit-test schema builds.
"""

from sqlalchemy import CheckConstraint

MINUTES = ("pay_day_normal_minutes", "pay_night_normal_minutes", "pay_tier1_minutes")
PERCENTS = ("pay_overtime1_pct", "pay_overtime2_pct", "pay_rest_pct")
POINTS = ("pay_increase1_points", "pay_increase2_points")
WEEKDAYS = ("pay_rest_start_weekday", "pay_rest_end_weekday")
REST = (
    "pay_rest_start_weekday",
    "pay_rest_start_time",
    "pay_rest_end_weekday",
    "pay_rest_end_time",
)


def pay_setting_checks(table: str, optional: bool) -> list[CheckConstraint]:
    """Return the check constraints of the premium setting columns of ``table``.

    Args:
        table: ``organisations`` or ``projects``; it names the constraints.
        optional: Whether the columns may be ``NULL`` (a project's overrides).

    Returns:
        One constraint per bound, named ``ck_<table>_<column>``, plus the rest-window rules.
    """

    def rule(column: str, condition: str) -> CheckConstraint:
        sql = f"{column} IS NULL OR ({condition})" if optional else condition
        return CheckConstraint(sql, name=f"ck_{table}_{column}")

    checks = [rule(c, f"{c} BETWEEN 1 AND 1440") for c in MINUTES]
    checks += [rule(c, f"{c} BETWEEN 100 AND 300") for c in PERCENTS]
    checks += [rule(c, f"{c} > 0 AND {c} <= 200") for c in POINTS]
    checks += [rule(c, f"{c} BETWEEN 0 AND 6") for c in WEEKDAYS]
    differ = (
        "NOT (pay_rest_start_weekday = pay_rest_end_weekday "
        "AND pay_rest_start_time = pay_rest_end_time)"
    )
    if optional:
        none = " AND ".join(f"{c} IS NULL" for c in REST)
        every = " AND ".join(f"{c} IS NOT NULL" for c in REST)
        checks.append(
            CheckConstraint(f"({none}) OR ({every} AND {differ})", name=f"ck_{table}_pay_rest")
        )
    else:
        checks.append(CheckConstraint(differ, name=f"ck_{table}_pay_rest"))
    return checks
