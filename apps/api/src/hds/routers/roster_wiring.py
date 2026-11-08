"""Builds the roster service on a request's session, for the routers that use it.

Layer: routers. Wiring only: it constructs ``RosterService`` and ``RosterToolsService`` with their
repositories and holds no logic, so the routers that use them share one place to build them.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from hds.repositories.availability import AvailabilityRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.roster_extras import (
    PublishEventRepository,
    RuleOverrideRepository,
    WeekTemplateRepository,
)
from hds.repositories.shift import AssignmentRepository, ShiftRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.user import UserRepository
from hds.services.roster import RosterService
from hds.services.roster_tools import RosterToolsService
from hds.wiring import submission_service


def roster_service(db: AsyncSession) -> RosterService:
    """The roster service on the request's session."""
    return RosterService(
        shift_repo=ShiftRepository(db),
        assignment_repo=AssignmentRepository(db),
        shift_type_repo=ShiftTypeRepository(db),
        project_repo=ProjectRepository(db),
        user_repo=UserRepository(db),
        entry_repo=AvailabilityRepository(db),
        organisation_repo=OrganisationRepository(db),
        project_manager_repo=ProjectManagerRepository(db),
        availability=submission_service(db),
    )


def roster_tools(db: AsyncSession) -> RosterToolsService:
    """The roster tools (overrides, publish review, copy week, templates, bulk, tracker)."""
    return RosterToolsService(
        roster=roster_service(db),
        shift_repo=ShiftRepository(db),
        assignment_repo=AssignmentRepository(db),
        override_repo=RuleOverrideRepository(db),
        template_repo=WeekTemplateRepository(db),
        event_repo=PublishEventRepository(db),
        user_repo=UserRepository(db),
        submissions=submission_service(db),
    )
