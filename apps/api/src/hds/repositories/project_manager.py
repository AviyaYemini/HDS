"""Persistence of managers on projects (005 FR-021, FR-022).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. Callers lock the project before replacing its set.
"""

import uuid
from collections.abc import Collection, Sequence

from sqlalchemy import delete, insert, select

from hds.models.project_manager import ProjectManager
from hds.models.user import User, UserRole
from hds.repositories.base import BaseRepository

ASSIGNABLE_ROLES = (UserRole.manager, UserRole.admin)
"""Who may be on a project as its manager."""


class ProjectManagerRepository(BaseRepository[ProjectManager]):
    """Which Managers and Admins are on which projects."""

    model = ProjectManager

    async def members(self, org_id: uuid.UUID, project_id: uuid.UUID) -> list[User]:
        """The project's managers, by name then id."""
        query = (
            select(User)
            .join(ProjectManager, ProjectManager.user_id == User.id)
            .where(
                ProjectManager.organisation_id == org_id,
                ProjectManager.project_id == project_id,
            )
            .order_by(User.full_name, User.id)
        )
        return list((await self.session.execute(query)).scalars())

    async def staff_among(
        self, org_id: uuid.UUID, user_ids: Collection[uuid.UUID]
    ) -> set[uuid.UUID]:
        """Those of ``user_ids`` who are active Managers or Admins of the organisation."""
        if not user_ids:
            return set()
        query = select(User.id).where(
            User.organisation_id == org_id,
            User.id.in_(list(user_ids)),
            User.role.in_(ASSIGNABLE_ROLES),
            User.is_active.is_(True),
            User.is_deleted.is_(False),
        )
        return set((await self.session.execute(query)).scalars())

    async def replace(
        self, org_id: uuid.UUID, project_id: uuid.UUID, user_ids: Sequence[uuid.UUID]
    ) -> None:
        """Make ``user_ids`` the project's managers."""
        await self.session.execute(
            delete(ProjectManager).where(ProjectManager.project_id == project_id)
        )
        if user_ids:
            await self.session.execute(
                insert(ProjectManager),
                [
                    {"organisation_id": org_id, "project_id": project_id, "user_id": u}
                    for u in user_ids
                ],
            )
        await self.session.flush()

    async def remove_user(self, org_id: uuid.UUID, user_id: uuid.UUID) -> None:
        """Take ``user_id`` off every project of the organisation they are assigned to."""
        await self.session.execute(
            delete(ProjectManager).where(
                ProjectManager.organisation_id == org_id, ProjectManager.user_id == user_id
            )
        )
        await self.session.flush()

    async def manages(
        self, user_id: uuid.UUID, project_ids: Collection[uuid.UUID]
    ) -> set[uuid.UUID]:
        """Those of ``project_ids`` that ``user_id`` is assigned to."""
        if not project_ids:
            return set()
        query = select(ProjectManager.project_id).where(
            ProjectManager.user_id == user_id, ProjectManager.project_id.in_(list(project_ids))
        )
        return set((await self.session.execute(query)).scalars())

    async def managed_by(self, org_id: uuid.UUID, user_id: uuid.UUID) -> list[uuid.UUID]:
        """Every project of the organisation that ``user_id`` manages, sorted (007 FR-028).

        ``GET /me`` serves this list next to the capabilities, so the client knows where a
        manager's ``:project`` capabilities apply.
        """
        query = (
            select(ProjectManager.project_id)
            .where(ProjectManager.organisation_id == org_id, ProjectManager.user_id == user_id)
            .order_by(ProjectManager.project_id)
        )
        return list((await self.session.execute(query)).scalars())

    async def names_for(
        self, org_id: uuid.UUID, project_ids: Collection[uuid.UUID]
    ) -> dict[uuid.UUID, list[tuple[uuid.UUID, str]]]:
        """Each of ``project_ids``' managers, as (id, name), by name (CEO demo)."""
        if not project_ids:
            return {}
        query = (
            select(ProjectManager.project_id, User.id, User.full_name)
            .join(User, User.id == ProjectManager.user_id)
            .where(
                ProjectManager.organisation_id == org_id,
                ProjectManager.project_id.in_(list(project_ids)),
            )
            .order_by(User.full_name, User.id)
        )
        found: dict[uuid.UUID, list[tuple[uuid.UUID, str]]] = {}
        for project_id, user_id, name in (await self.session.execute(query)).all():
            found.setdefault(project_id, []).append((user_id, name))
        return found
