"""Temporary workers (CEO demo): who they are, how long, and on which projects.

Layer: services. A temporary worker belongs to one or more projects for at most three months, which
may be shorter. A run never places them outside their projects or dates; a hand assignment there is
stored with the warning ``warnings.temporary_outside``.
"""

import calendar
import uuid
from collections.abc import Mapping
from datetime import date
from typing import Any

from hds.core.exceptions import DomainValidationError
from hds.models.user import User
from hds.scheduling import Employee, Engagement

PERMANENT = "permanent"
TEMPORARY = "temporary"
MAX_MONTHS = 3
FIELDS = ("employment_type", "engaged_from", "engaged_until", "project_ids")


def add_months(day: date, months: int) -> date:
    """``day`` plus ``months`` calendar months, on the month's last day when it is shorter."""
    month = day.month - 1 + months
    year, month = day.year + month // 12, month % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def resolve(target: User | None, data: Mapping[str, Any]) -> dict[str, Any] | None:
    """The engagement after a change: ``None`` when ``data`` touches none of its fields.

    Starts from ``target``'s (a new account starts permanent) and applies what ``data`` sets. A
    permanent worker ends with no dates and no projects, whatever else ``data`` says.

    Raises:
        DomainValidationError: ``employees.temporary_period_invalid`` when a temporary worker
            lacks a date, ends before starting or stays longer than three months;
            ``employees.temporary_projects_required`` when they have no project (HTTP 422).
    """
    if target is not None and not any(f in data for f in FIELDS):
        return None
    kind = data.get("employment_type") or (target.employment_type if target else PERMANENT)
    if kind == PERMANENT:
        return {
            "employment_type": PERMANENT,
            "engaged_from": None,
            "engaged_until": None,
            "project_ids": [],
        }
    first = (
        data["engaged_from"] if "engaged_from" in data else getattr(target, "engaged_from", None)
    )
    last = (
        data["engaged_until"] if "engaged_until" in data else getattr(target, "engaged_until", None)
    )
    projects = (
        list(data["project_ids"] or [])
        if "project_ids" in data
        else list(getattr(target, "project_ids", []))
    )
    if first is None or last is None or last < first or last > add_months(first, MAX_MONTHS):
        raise DomainValidationError(
            code="employees.temporary_period_invalid",
            detail="A temporary worker needs a start and an end at most three months later.",
        )
    if not projects:
        raise DomainValidationError(
            code="employees.temporary_projects_required",
            detail="A temporary worker needs at least one project.",
        )
    return {
        "employment_type": TEMPORARY,
        "engaged_from": first,
        "engaged_until": last,
        "project_ids": list(dict.fromkeys(projects)),
    }


def engagement_of(user: User) -> Engagement | None:
    """The rules' view of ``user``'s engagement: ``None`` for permanent staff."""
    if user.employment_type != TEMPORARY or user.engaged_from is None or user.engaged_until is None:
        return None
    return Engagement(frozenset(user.project_ids), user.engaged_from, user.engaged_until)


def as_employee(user: User, *, active: bool | None = None) -> Employee:
    """``user`` as the scheduling rules see them, a temporary worker with their engagement."""
    return Employee(user.id, user.is_active if active is None else active, engagement_of(user))


def project_ids_valid(found: set[uuid.UUID], wanted: list[uuid.UUID]) -> None:
    """Refuse projects that are not the organisation's (HTTP 422)."""
    if set(wanted) - found:
        raise DomainValidationError(
            code="employees.project_invalid",
            detail="A project is not one of the organisation's.",
        )
