"""Persistence of project teams: for now, the projects a temporary worker may work on (CEO demo).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from collections.abc import Collection, Sequence

from sqlalchemy import delete, func, insert, select

from hds.models.project import Project
from hds.models.project_member import ProjectMember
from hds.models.user import User
from hds.repositories.base import BaseRepository


class ProjectMemberRepository(BaseRepository[ProjectMember]):
    """Who is on which project's team."""

    model = ProjectMember

    async def existing_projects(
        self, org_id: uuid.UUID, project_ids: Collection[uuid.UUID]
    ) -> set[uuid.UUID]:
        """Those of ``project_ids`` that are the organisation's projects and not deleted."""
        if not project_ids:
            return set()
        query = select(Project.id).where(
            Project.organisation_id == org_id,
            Project.id.in_(list(project_ids)),
            Project.is_deleted.is_(False),
        )
        return set((await self.session.execute(query)).scalars())

    async def replace_for_user(
        self, org_id: uuid.UUID, user_id: uuid.UUID, project_ids: Sequence[uuid.UUID]
    ) -> None:
        """Make ``project_ids`` the projects ``user_id`` is on, then reload their memberships."""
        await self.session.execute(
            delete(ProjectMember).where(
                ProjectMember.organisation_id == org_id, ProjectMember.user_id == user_id
            )
        )
        if project_ids:
            await self.session.execute(
                insert(ProjectMember),
                [
                    {"organisation_id": org_id, "project_id": p, "user_id": user_id}
                    for p in dict.fromkeys(project_ids)
                ],
            )
        await self.session.flush()
        user = await self.session.get(User, user_id)
        if user is not None:
            await self.session.refresh(user, ["memberships"])

    async def temporary_counts(self, org_id: uuid.UUID) -> dict[uuid.UUID, int]:
        """How many temporary workers each project of the organisation has on its team."""
        query = (
            select(ProjectMember.project_id, func.count())
            .join(User, User.id == ProjectMember.user_id)
            .where(
                ProjectMember.organisation_id == org_id,
                User.employment_type == "temporary",
                User.is_deleted.is_(False),
            )
            .group_by(ProjectMember.project_id)
        )
        return {row[0]: row[1] for row in (await self.session.execute(query)).all()}
