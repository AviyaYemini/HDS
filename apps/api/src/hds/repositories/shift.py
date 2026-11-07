"""Shifts and the employees assigned to them.

Layer: repositories. Depends on ``hds.models``, ``hds.core.exceptions`` and
``hds.repositories.base`` only; it MUST NOT import services or routers. Every read is scoped to the
caller's organisation.
"""

import uuid
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import and_, delete, func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError

from hds.core.exceptions import ConflictError
from hds.models.project import Project
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType
from hds.models.site import Site
from hds.repositories.base import OrgScopedRepository

_ONE_PER_SHIFT = "uq_assignments_shift_user"


@dataclass(frozen=True)
class ShiftView:
    """A shift with its site's time zone and its assignments, as the roster reads it."""

    shift: Shift
    timezone: str
    assignments: list[Assignment]
    rate_shown: bool = True
    """Whether the reader may see the shift's own ``pay_rate`` (005 FR-022a); services decide."""


@dataclass(frozen=True)
class OwnShift:
    """A published shift of one employee, with only what they may see of it (research R7)."""

    id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    timezone: str
    shift_type: str
    project_id: uuid.UUID
    project_name: str
    site_id: uuid.UUID
    site_name: str


@dataclass(frozen=True)
class OwnShiftDetail(OwnShift):
    """One of an employee's published shifts with its site's address (007 FR-023, E11)."""

    site_address: str | None


class ShiftRepository(OrgScopedRepository[Shift]):
    """An organisation's shifts."""

    model = Shift

    async def site_timezone(self, project_id: uuid.UUID) -> str:
        """The IANA time zone of the site a project is at."""
        result = await self.session.execute(
            select(Site.timezone)
            .join(Project, Project.site_id == Site.id)
            .where(Project.id == project_id)
        )
        return str(result.scalar_one())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Shift:
        """Insert a draft shift and flush."""
        shift = Shift(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(shift)
        await self.session.flush()
        return shift

    async def lock_between(self, org_id: uuid.UUID, start: datetime, end: datetime) -> None:
        """Lock the organisation's shifts that overlap ``start`` to ``end``, in id order.

        ``FOR NO KEY UPDATE``, as ``get_locked`` takes, so publishing, changing, deleting,
        assigning and unassigning any of them waits. Accepting a schedule proposal holds these
        (004 research G9).
        """
        await self.session.execute(
            select(Shift.id)
            .where(Shift.organization_id == org_id, Shift.starts_at < end, Shift.ends_at > start)
            .order_by(Shift.id)
            .with_for_update(key_share=True)
        )

    async def create_many(self, org_id: uuid.UUID, rows: Sequence[dict[str, Any]]) -> None:
        """Insert many shifts in one statement; each row carries its own ``id``."""
        if rows:
            await self.session.execute(
                insert(Shift), [{**row, "organization_id": org_id} for row in rows]
            )

    async def get_locked(self, id: uuid.UUID, org_id: uuid.UUID) -> Shift | None:
        """Fetch a shift of the organisation and lock it until the transaction ends.

        Reads the committed row even if the session holds an older copy. ``FOR NO KEY UPDATE``,
        so assignments being added to it (whose foreign key takes a key-share lock) do not wait.
        """
        result = await self.session.execute(
            select(Shift)
            .where(Shift.id == id, Shift.organization_id == org_id)
            .with_for_update(key_share=True)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Shift:
        """Change a shift of the organisation.

        Raises:
            NotFoundError: The shift is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def touch(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Mark a shift changed (``updated_at`` now) after one of its assignments changed.

        The roster's ETag is the shift's ``updated_at``, so an assignment change makes a reader's
        ``If-Match`` stale too (007 FR-041), and "changed since publish" sees it.
        """
        await self.session.execute(
            update(Shift)
            .where(Shift.id == id, Shift.organization_id == org_id)
            .values(updated_at=func.now())
            .execution_options(synchronize_session=False)
        )

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Remove a shift; its assignments go with it (``ON DELETE CASCADE``)."""
        await self.session.execute(
            delete(Shift).where(Shift.id == id, Shift.organization_id == org_id)
        )
        await self.session.flush()

    async def views(self, shifts: Sequence[Shift]) -> list[ShiftView]:
        """The shifts with their sites' time zones and their assignments, in the given order."""
        if not shifts:
            return []
        ids = [s.id for s in shifts]
        zones = dict(
            (
                await self.session.execute(
                    select(Shift.id, Site.timezone)
                    .join(Project, Project.id == Shift.project_id)
                    .join(Site, Site.id == Project.site_id)
                    .where(Shift.id.in_(ids))
                )
            ).all()
        )
        assigned: dict[uuid.UUID, list[Assignment]] = {i: [] for i in ids}
        for assignment in (
            await self.session.execute(
                select(Assignment)
                .where(Assignment.shift_id.in_(ids))
                .order_by(Assignment.created_at, Assignment.id)
            )
        ).scalars():
            assigned[assignment.shift_id].append(assignment)
        return [ShiftView(s, zones[s.id], assigned[s.id]) for s in shifts]

    async def page(
        self,
        org_id: uuid.UUID,
        *,
        starts_from: datetime | None,
        starts_before: datetime | None,
        project_id: uuid.UUID | None,
        status: str | None,
        after: tuple[datetime, uuid.UUID] | None,
        limit: int,
    ) -> list[Shift]:
        """Shifts of the organisation by start time, then id, at most ``limit`` after ``after``.

        Args:
            org_id: The caller's organisation.
            starts_from: Keep shifts starting at or after this instant.
            starts_before: Keep shifts starting before this instant.
            project_id: Keep one project's shifts.
            status: Keep ``draft`` or ``published`` shifts.
            after: The ``(starts_at, id)`` of the last shift of the previous page.
            limit: The most shifts to return.
        """
        stmt = select(Shift).where(Shift.organization_id == org_id)
        if starts_from is not None:
            stmt = stmt.where(Shift.starts_at >= starts_from)
        if starts_before is not None:
            stmt = stmt.where(Shift.starts_at < starts_before)
        if project_id is not None:
            stmt = stmt.where(Shift.project_id == project_id)
        if status is not None:
            stmt = stmt.where(Shift.status == status)
        if after is not None:
            stmt = stmt.where(
                or_(
                    Shift.starts_at > after[0],
                    and_(Shift.starts_at == after[0], Shift.id > after[1]),
                )
            )
        result = await self.session.execute(stmt.order_by(Shift.starts_at, Shift.id).limit(limit))
        return list(result.scalars().all())

    async def publish(
        self,
        org_id: uuid.UUID,
        project_id: uuid.UUID,
        starts_from: datetime | None,
        starts_before: datetime | None,
        now: datetime,
    ) -> int:
        """Publish a project's drafts that start in a period; published ones are left as they are.

        Returns:
            How many shifts were published.
        """
        stmt = update(Shift).where(
            Shift.organization_id == org_id,
            Shift.project_id == project_id,
            Shift.status == "draft",
        )
        if starts_from is not None:
            stmt = stmt.where(Shift.starts_at >= starts_from)
        if starts_before is not None:
            stmt = stmt.where(Shift.starts_at < starts_before)
        result = await self.session.execute(stmt.values(status="published", published_at=now))
        await self.session.flush()
        return int(result.rowcount)  # type: ignore[attr-defined]

    async def own_published(
        self,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        starts_from: datetime | None,
        starts_before: datetime | None,
    ) -> list[OwnShift]:
        """The published shifts an employee is on, by start time, and nothing about anyone else."""
        stmt = (
            select(
                Shift.id,
                func.coalesce(Assignment.starts_at, Shift.starts_at),
                func.coalesce(Assignment.ends_at, Shift.ends_at),
                Site.timezone,
                ShiftType.name,
                Project.id,
                Project.name,
                Site.id,
                Site.name,
            )
            .join(Assignment, Assignment.shift_id == Shift.id)
            .join(Project, Project.id == Shift.project_id)
            .join(Site, Site.id == Project.site_id)
            .join(ShiftType, ShiftType.id == Shift.shift_type_id)
            .where(
                Shift.organization_id == org_id,
                Shift.status == "published",
                Assignment.user_id == user_id,
                Project.is_deleted.is_(False),
            )
        )
        if starts_from is not None:
            stmt = stmt.where(Shift.starts_at >= starts_from)
        if starts_before is not None:
            stmt = stmt.where(Shift.starts_at < starts_before)
        rows = await self.session.execute(stmt.order_by(Shift.starts_at, Shift.id))
        return [OwnShift(*row) for row in rows.all()]

    async def own_published_one(
        self, org_id: uuid.UUID, user_id: uuid.UUID, shift_id: uuid.UUID
    ) -> OwnShiftDetail | None:
        """One published shift the employee is on, with its site's address, or ``None``."""
        stmt = (
            select(
                Shift.id,
                func.coalesce(Assignment.starts_at, Shift.starts_at),
                func.coalesce(Assignment.ends_at, Shift.ends_at),
                Site.timezone,
                ShiftType.name,
                Project.id,
                Project.name,
                Site.id,
                Site.name,
                Site.address,
            )
            .join(Assignment, Assignment.shift_id == Shift.id)
            .join(Project, Project.id == Shift.project_id)
            .join(Site, Site.id == Project.site_id)
            .join(ShiftType, ShiftType.id == Shift.shift_type_id)
            .where(
                Shift.organization_id == org_id,
                Shift.id == shift_id,
                Shift.status == "published",
                Assignment.user_id == user_id,
                Project.is_deleted.is_(False),
            )
        )
        row = (await self.session.execute(stmt)).one_or_none()
        if row is None:
            return None
        base = OwnShift(*row[:9])
        return OwnShiftDetail(**vars(base), site_address=row[9])

    async def of_employees(
        self,
        org_id: uuid.UUID,
        user_ids: Collection[uuid.UUID],
        starts_before: datetime,
        ends_after: datetime,
    ) -> list[Shift]:
        """Every shift of the organisation with one of the employees on it in a period.

        Across all projects and including drafts (003 FR-117); indexed by employee and start.
        """
        if not user_ids:
            return []
        on_them = select(Assignment.shift_id).where(Assignment.user_id.in_(user_ids))
        result = await self.session.execute(
            select(Shift).where(
                Shift.organization_id == org_id,
                Shift.id.in_(on_them),
                Shift.starts_at < starts_before,
                Shift.ends_at > ends_after,
            )
        )
        return list(result.scalars().all())


class AssignmentRepository(OrgScopedRepository[Assignment]):
    """An organisation's assignments."""

    model = Assignment

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Assignment:
        """Insert an assignment and flush.

        Raises:
            ConflictError: ``assignments.duplicate`` (HTTP 409) when the employee is already on
                the shift, also when another request assigned them at the same moment.
        """
        assignment = Assignment(id=uuid.uuid4(), organization_id=org_id, **data)
        try:
            async with self.session.begin_nested():
                self.session.add(assignment)
                await self.session.flush()
        except IntegrityError as exc:
            if _ONE_PER_SHIFT in str(exc.orig):
                raise ConflictError(
                    code="assignments.duplicate",
                    detail="The employee is already assigned to this shift.",
                    field="user_id",
                ) from None
            raise
        return assignment

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Assignment:
        """Change an assignment of the organisation (its own times, 007 FR-027).

        Raises:
            NotFoundError: The assignment is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def create_many(self, org_id: uuid.UUID, rows: Sequence[dict[str, Any]]) -> None:
        """Insert many assignments in one statement.

        Callers hold the locks that rule out a duplicate, so ``assignments.duplicate`` cannot
        happen here.
        """
        if rows:
            await self.session.execute(
                insert(Assignment),
                [{"id": uuid.uuid4(), **row, "organization_id": org_id} for row in rows],
            )

    async def remove_people(self, shift_id: uuid.UUID, user_ids: Sequence[uuid.UUID]) -> int:
        """Take ``user_ids`` off a shift; the number of assignments removed."""
        if not user_ids:
            return 0
        result = await self.session.execute(
            delete(Assignment)
            .where(Assignment.shift_id == shift_id, Assignment.user_id.in_(list(user_ids)))
            .returning(Assignment.id)
        )
        return len(result.all())

    async def exists(self, id: uuid.UUID, org_id: uuid.UUID) -> bool:
        """Whether the assignment is still there, read from the database, not the session."""
        found = await self.session.execute(
            select(Assignment.id).where(Assignment.id == id, Assignment.organization_id == org_id)
        )
        return found.first() is not None

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Remove an assignment of the organisation; nothing happens when it is not there."""
        await self.session.execute(
            delete(Assignment).where(Assignment.id == id, Assignment.organization_id == org_id)
        )
        await self.session.flush()
