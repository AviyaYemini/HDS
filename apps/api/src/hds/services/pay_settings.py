"""Premium settings: the organisation's defaults and each project's switches and overrides (005).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models``, ``hds.payroll`` (the
settings types) and the repositories it is given; it MUST NOT import routers or know about HTTP.

Managers and Admins read the settings; only Admins change them (FR-006, FR-007). A project follows
each organisation default unless it has its own value (FR-006a); ``null`` in a change returns a
setting to the default. When a project ends, ``freeze`` copies the defaults it follows and the
organisation's special days into it, so its pay stops changing (FR-007, FR-008a). Locks are taken
in the order schedule accept uses (004 research G9): the organisation, then the project.
"""

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import time
from decimal import Decimal
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
)
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus
from hds.models.user import User, UserRole
from hds.payroll import PaySettings, RestWindow
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.special_day import SpecialDayRepository

REST_COLUMNS = (
    "pay_rest_start_weekday",
    "pay_rest_start_time",
    "pay_rest_end_weekday",
    "pay_rest_end_time",
)
COLUMNS: dict[str, tuple[str, ...]] = {
    "day_normal_minutes": ("pay_day_normal_minutes",),
    "night_normal_minutes": ("pay_night_normal_minutes",),
    "tier1_minutes": ("pay_tier1_minutes",),
    "overtime1_pct": ("pay_overtime1_pct",),
    "overtime2_pct": ("pay_overtime2_pct",),
    "rest_window": REST_COLUMNS,
    "rest_pct": ("pay_rest_pct",),
    "increase1_points": ("pay_increase1_points",),
    "increase2_points": ("pay_increase2_points",),
}
"""Each setting the API names, and the columns that hold it, in the order of the data model."""

_MINUTES = ("day_normal_minutes", "night_normal_minutes", "tier1_minutes")
_PERCENTS = ("overtime1_pct", "overtime2_pct", "rest_pct")
_POINTS = ("increase1_points", "increase2_points")


@dataclass(frozen=True)
class ProjectPay:
    """A project's pay switches, its effective settings and which of them are its own."""

    pay_enabled: bool
    managers_see_pay: bool
    settings: PaySettings
    own: list[str]


def _invalid(name: str, rule: str) -> DomainValidationError:
    return DomainValidationError(code="pay.settings_invalid", detail=f"{name} must be {rule}.")


def check(name: str, value: Any) -> None:
    """Check one setting against its bounds (FR-003).

    Raises:
        DomainValidationError: ``pay.settings_invalid``, or ``pay.rest_window_invalid`` for a
            window whose ends are the same moment of the week (HTTP 422).
    """
    if name not in COLUMNS:
        raise _invalid(name, "a known setting")
    if value is None:
        raise _invalid(name, "set")
    if name in _MINUTES and not (isinstance(value, int) and 1 <= value <= 1440):
        raise _invalid(name, "1 to 1440 minutes")
    if name in _PERCENTS and not (isinstance(value, Decimal) and 100 <= value <= 300):
        raise _invalid(name, "100 to 300 percent")
    if name in _POINTS and not (isinstance(value, Decimal) and 0 < value <= 200):
        raise _invalid(name, "above 0 and at most 200 points")
    if name == "rest_window":
        window: RestWindow = value
        if not all(0 <= w <= 6 for w in (window.start_weekday, window.end_weekday)):
            raise _invalid(name, "between weekdays 0 and 6")
        if (window.start_weekday, window.start_time) == (window.end_weekday, window.end_time):
            raise DomainValidationError(
                code="pay.rest_window_invalid",
                detail="The weekly rest must end at a different moment than it starts.",
            )


def to_columns(name: str, value: Any) -> dict[str, Any]:
    """The column values of one setting; ``None`` clears every column of it."""
    if name == "rest_window":
        window: RestWindow | None = value
        if window is None:
            return dict.fromkeys(REST_COLUMNS)
        return dict(
            zip(
                REST_COLUMNS,
                (window.start_weekday, window.start_time, window.end_weekday, window.end_time),
                strict=True,
            )
        )
    return {COLUMNS[name][0]: value}


def _value(row: Any, name: str) -> Any:
    """One setting read from an organisation or project row; ``None`` when a project follows."""
    if name == "rest_window":
        parts = [getattr(row, c) for c in REST_COLUMNS]
        if any(p is None for p in parts):
            return None
        start_weekday, start_time, end_weekday, end_time = parts
        return RestWindow(int(start_weekday), _time(start_time), int(end_weekday), _time(end_time))
    return getattr(row, COLUMNS[name][0])


def _time(value: Any) -> time:
    if not isinstance(value, time):
        raise TypeError(f"expected a time, got {value!r}")
    return value


def _settings(values: Mapping[str, Any]) -> PaySettings:
    """Settings from values keyed by API name (the window is ``rest`` in the pay package)."""
    return PaySettings(**{("rest" if k == "rest_window" else k): v for k, v in values.items()})


def organisation_settings(org: Organisation) -> PaySettings:
    """The organisation's premium defaults."""
    return _settings({name: _value(org, name) for name in COLUMNS})


def project_settings(org: Organisation, project: Project) -> tuple[PaySettings, list[str]]:
    """A project's effective settings and the names it sets itself."""
    own = [name for name in COLUMNS if _value(project, name) is not None]
    values = {name: _value(project if name in own else org, name) for name in COLUMNS}
    return _settings(values), own


class PaySettingsService:
    """Read and change premium settings; freeze a project's pay when it ends."""

    def __init__(
        self,
        organisation_repo: OrganisationRepository,
        project_repo: ProjectRepository,
        special_day_repo: SpecialDayRepository,
    ) -> None:
        """Wire the service to the organisations, projects and special days."""
        self._orgs = organisation_repo
        self._projects = project_repo
        self._days = special_day_repo

    async def _organisation(self, org_id: uuid.UUID) -> Organisation:
        org = await self._orgs.get_by_id(org_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=org_id)
        return org

    async def read_organisation(self, actor: User) -> PaySettings:
        """The organisation's premium defaults (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return organisation_settings(await self._organisation(actor.organization_id))

    async def change_organisation(self, actor: User, changes: Mapping[str, Any]) -> PaySettings:
        """Change premium defaults (Admin); projects that follow them follow the change.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            DomainValidationError: ``pay.settings_invalid`` or ``pay.rest_window_invalid`` (422).
        """
        ensure_role(actor, UserRole.admin)
        values: dict[str, Any] = {}
        for name, value in changes.items():
            check(name, value)
            values.update(to_columns(name, value))
        org = await self._orgs.update_pay(actor.organization_id, values)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organization_id)
        return organisation_settings(org)

    async def _project(self, actor: User, project_id: uuid.UUID) -> Project:
        project = await self._projects.get_by_id(project_id, actor.organization_id)
        if project is None:
            raise NotFoundError(resource="Project", id=project_id)
        return project

    async def read_project(self, actor: User, project_id: uuid.UUID) -> ProjectPay:
        """A project's switches and effective settings (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        project = await self._project(actor, project_id)
        settings, own = project_settings(await self._organisation(actor.organization_id), project)
        return ProjectPay(project.pay_enabled, project.managers_see_pay, settings, own)

    def _columns(self, changes: Mapping[str, Any]) -> dict[str, Any]:
        """Column values for a change of switches and overrides; ``None`` returns to the default."""
        values: dict[str, Any] = {}
        for switch in ("pay_enabled", "managers_see_pay"):
            if switch in changes:
                if not isinstance(changes[switch], bool):
                    raise _invalid(switch, "true or false")
                values[switch] = changes[switch]
        for name, value in (changes.get("settings") or {}).items():
            if value is not None:
                check(name, value)
            elif name not in COLUMNS:
                raise _invalid(name, "a known setting")
            values.update(to_columns(name, value))
        return values

    async def change_project(
        self, actor: User, project_id: uuid.UUID, changes: Mapping[str, Any]
    ) -> ProjectPay:
        """Change a project's switches or overrides (Admin) while it has not ended.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            ConflictError: ``pay.project_ended`` (HTTP 409).
            DomainValidationError: ``pay.settings_invalid`` or ``pay.rest_window_invalid`` (422).
        """
        ensure_role(actor, UserRole.admin)
        values = self._columns(changes)
        project = await self._projects.lock_and_read(project_id, actor.organization_id)
        if project is None:
            raise NotFoundError(resource="Project", id=project_id)
        if project.status == ProjectStatus.ended:
            raise ConflictError(
                code="pay.project_ended",
                detail="An ended project's pay settings and special days are fixed.",
            )
        if values:
            await self._projects.update(project_id, actor.organization_id, **values)
        return await self.read_project(actor, project_id)

    def columns_for_new_project(self, actor: User, pay: Mapping[str, Any]) -> dict[str, Any]:
        """Column values for the pay settings sent with a new project (Admin only).

        Raises:
            ForbiddenError: ``projects.pay_settings_admin_only`` (HTTP 403).
            DomainValidationError: ``pay.settings_invalid`` or ``pay.rest_window_invalid`` (422).
        """
        if actor.role != UserRole.admin:
            raise ForbiddenError(
                code="projects.pay_settings_admin_only",
                detail="Only an Admin sets a project's pay settings.",
            )
        return self._columns(pay)

    async def freeze(self, org_id: uuid.UUID, project_id: uuid.UUID) -> None:
        """Fix a project's pay before it ends: copy in the defaults it follows and the days.

        Locks the organisation, then the project, and does nothing if the project has already
        ended. The caller sets the status in the same transaction.
        """
        await self._orgs.lock(org_id)
        project = await self._projects.lock_and_read(project_id, org_id)
        if project is None or project.status == ProjectStatus.ended:
            return
        org = await self._organisation(org_id)
        missing = {
            column: getattr(org, column)
            for columns in COLUMNS.values()
            for column in columns
            if getattr(project, column) is None
        }
        if missing:
            await self._projects.update(project_id, org_id, **missing)
        await self._days.copy_organisation_days(org_id, project_id)
