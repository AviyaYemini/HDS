"""Who may see a project's pay: the one rule the reports and the roster share (005 FR-022, FR-022a).

Layer: services. Depends on ``hds.models`` and the project-manager repository it is given; it MUST
NOT import routers or know about HTTP. Read-only.

An Admin sees pay for every project with pay on. A Manager sees it only for a project with pay on,
open to Managers, and that they are assigned to, read from the live role on every request. Anyone
else sees none.
"""

import uuid
from collections.abc import Iterable

from hds.models.project import Project
from hds.models.user import User, UserRole
from hds.repositories.project_manager import ProjectManagerRepository


async def pay_visible(
    actor: User, projects: Iterable[Project], managers: ProjectManagerRepository
) -> set[uuid.UUID]:
    """The ids of those of ``projects`` whose pay ``actor`` may see."""
    with_pay = [p for p in projects if p.pay_enabled]
    if actor.role == UserRole.admin:
        return {p.id for p in with_pay}
    if actor.role != UserRole.manager:
        return set()
    open_ = {p.id for p in with_pay if p.managers_see_pay}
    if not open_:
        return set()
    return await managers.manages(actor.id, open_)
