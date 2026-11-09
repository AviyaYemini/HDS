"""Special days: dates paid at their own rate, for the organisation or one project (005 FR-008).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repositories it is
given; it MUST NOT import routers or know about HTTP.

Admins keep the organisation's list and each project's own days; Managers read them. A project day
on a date the organisation also lists overrides it for that project (FR-008a). An ended project's
days are fixed: ending copied the organisation's days into it, so it reads its own list only
(FR-007). Writes to the organisation's days lock the organisation; writes to a project's days lock
the project, where ending also takes its lock.
"""

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, DomainValidationError, NotFoundError
from hds.models.project import Project, ProjectStatus
from hds.models.special_day import SpecialDay
from hds.models.user import User, UserRole
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.special_day import SpecialDayRepository


@dataclass(frozen=True)
class DayInEffect:
    """A special day as a project sees it, and whether it is the project's or the organisation's."""

    id: uuid.UUID
    project_id: uuid.UUID | None
    day: date
    name: str
    rate_pct: Decimal
    source: str


def _check(values: Mapping[str, Any]) -> None:
    """Check a day's name and rate (FR-003).

    Raises:
        DomainValidationError: ``special_days.name_required`` or ``pay.settings_invalid`` (422).
    """
    if "name" in values and not str(values["name"]).strip():
        raise DomainValidationError(
            code="special_days.name_required", detail="A special day needs a name."
        )
    rate = values.get("rate_pct")
    if rate is not None and not 100 <= rate <= 300:
        raise DomainValidationError(
            code="pay.settings_invalid", detail="rate_pct must be 100 to 300 percent."
        )


def _ended() -> ConflictError:
    return ConflictError(
        code="pay.project_ended",
        detail="An ended project's pay settings and special days are fixed.",
    )


def _duplicate(day: date) -> ConflictError:
    return ConflictError(
        code="special_days.duplicate",
        detail=f"There is already a special day on {day}.",
        field="day",
    )


class SpecialDayService:
    """Keep and read special days."""

    def __init__(
        self,
        special_day_repo: SpecialDayRepository,
        organisation_repo: OrganisationRepository,
        project_repo: ProjectRepository,
    ) -> None:
        """Wire the service to the special days, organisations and projects."""
        self._days = special_day_repo
        self._orgs = organisation_repo
        self._projects = project_repo

    async def list_organisation(self, actor: User, year: int | None) -> list[SpecialDay]:
        """The organisation's days (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._days.list_organisation(actor.organisation_id, year)

    async def create_organisation_day(self, actor: User, values: Mapping[str, Any]) -> SpecialDay:
        """Add an organisation day (Admin); it applies to every project that has not ended.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            DomainValidationError: A blank name or a rate out of bounds (HTTP 422).
            ConflictError: ``special_days.duplicate`` (HTTP 409).
        """
        ensure_role(actor, UserRole.admin)
        _check(values)
        await self._orgs.lock(actor.organisation_id)
        if await self._days.exists(actor.organisation_id, None, values["day"]):
            raise _duplicate(values["day"])
        return await self._days.create(actor.organisation_id, None, values)

    async def _open_project(self, actor: User, project_id: uuid.UUID) -> Project:
        """Lock a project of the organisation and check it has not ended."""
        project = await self._projects.lock_and_read(project_id, actor.organisation_id)
        if project is None:
            raise NotFoundError(resource="Project", id=project_id)
        if project.status == ProjectStatus.ended:
            raise _ended()
        return project

    async def create_project_day(
        self, actor: User, project_id: uuid.UUID, values: Mapping[str, Any]
    ) -> SpecialDay:
        """Add a day to one project, or override an organisation day's rate for it (Admin).

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            ConflictError: ``pay.project_ended`` or ``special_days.duplicate`` (HTTP 409).
            DomainValidationError: A blank name or a rate out of bounds (HTTP 422).
        """
        ensure_role(actor, UserRole.admin)
        _check(values)
        await self._open_project(actor, project_id)
        if await self._days.exists(actor.organisation_id, project_id, values["day"]):
            raise _duplicate(values["day"])
        return await self._days.create(actor.organisation_id, project_id, values)

    async def _writable(self, actor: User, day_id: uuid.UUID) -> SpecialDay:
        """Find a day and take the lock its owner's writes use."""
        row = await self._days.get_by_id(day_id, actor.organisation_id)
        if row is None:
            raise NotFoundError(resource="SpecialDay", id=day_id)
        if row.project_id is None:
            await self._orgs.lock(actor.organisation_id)
        else:
            await self._open_project(actor, row.project_id)
        # Read again under the lock: the copy above may predate a change committed meanwhile.
        fresh = await self._days.reread(day_id, actor.organisation_id)
        if fresh is None:
            raise NotFoundError(resource="SpecialDay", id=day_id)
        return fresh

    async def change_day(
        self, actor: User, day_id: uuid.UUID, values: Mapping[str, Any]
    ) -> SpecialDay:
        """Change a day's date, name or rate (Admin).

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The day is not in the organisation (HTTP 404).
            ConflictError: ``pay.project_ended`` or ``special_days.duplicate`` (HTTP 409).
            DomainValidationError: A blank name or a rate out of bounds (HTTP 422).
        """
        ensure_role(actor, UserRole.admin)
        _check(values)
        row = await self._writable(actor, day_id)
        new_day = values.get("day")
        if new_day is not None and new_day != row.day:
            if await self._days.exists(actor.organisation_id, row.project_id, new_day):
                raise _duplicate(new_day)
        return await self._days.change(row, values)

    async def delete_day(self, actor: User, day_id: uuid.UUID) -> None:
        """Delete a day (Admin); a project's override of the same date stays.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The day is not in the organisation (HTTP 404).
            ConflictError: ``pay.project_ended`` (HTTP 409).
        """
        ensure_role(actor, UserRole.admin)
        await self._days.remove(await self._writable(actor, day_id))

    async def in_effect(
        self, actor: User, project_id: uuid.UUID, year: int | None
    ) -> list[DayInEffect]:
        """The days a project's shifts are paid by, by date (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        project = await self._projects.get_by_id(project_id, actor.organisation_id)
        if project is None:
            raise NotFoundError(resource="Project", id=project_id)
        own = await self._days.list_project(project_id, year)
        merged = {d.day: _seen(d, "project") for d in own}
        if project.status != ProjectStatus.ended:
            for d in await self._days.list_organisation(actor.organisation_id, year):
                merged.setdefault(d.day, _seen(d, "organisation"))
        return [merged[day] for day in sorted(merged)]


def _seen(row: SpecialDay, source: str) -> DayInEffect:
    return DayInEffect(row.id, row.project_id, row.day, row.name, row.rate_pct, source)


def merge_days(rows: list[SpecialDay], project_id: uuid.UUID, ended: bool) -> dict[date, Decimal]:
    """The rate of each special day for one project, from rows of the organisation and projects.

    A project's own day wins over the organisation's; an ended project has only its own (FR-007).
    """
    own = {r.day: r.rate_pct for r in rows if r.project_id == project_id}
    if ended:
        return own
    shared = {r.day: r.rate_pct for r in rows if r.project_id is None}
    return {**shared, **own}
