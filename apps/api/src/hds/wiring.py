"""Builds services on a session: the composition root for routers and the worker (004).

Layer: composition root, beside ``hds.main``. It constructs services with their repositories and
holds no rule. Routers receive ready services through the FastAPI dependencies here, so they import
neither the database nor a repository (Constitution II); the worker and the tests use the plain
builders.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import cache, lru_cache

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hds.core.config import get_settings
from hds.core.database import get_health_session_factory, get_unit_of_work
from hds.core.health import Migrations, load_migrations
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.availability_submission import AvailabilitySubmissionRepository
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.health import HealthRepository
from hds.repositories.idempotency_key import IdempotencyKeyRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.pay_reports import PayReportRepository, ReportShiftRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.roster_extras import (
    PublishEventRepository,
    RosterEditEventRepository,
    RuleOverrideRepository,
    WeekTemplateRepository,
)
from hds.repositories.saved_view import SavedViewRepository
from hds.repositories.schedule_job import ScheduleJobRepository
from hds.repositories.schedule_job_event import ScheduleJobEventRepository
from hds.repositories.scheduling_inputs import SchedulingInputRepository
from hds.repositories.shift import AssignmentRepository, ShiftRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.site import SiteRepository
from hds.repositories.special_day import SpecialDayRepository
from hds.repositories.staffing_need import StaffingNeedRepository
from hds.repositories.submission_window import SubmissionWindowRepository
from hds.repositories.user import UserRepository
from hds.services.auth import AuthService
from hds.services.availability_submission import AvailabilitySubmissionService
from hds.services.health import HealthService
from hds.services.idempotency import IdempotencyService
from hds.services.organisation_contact import OrganisationContactService
from hds.services.pay import PayService
from hds.services.pay_settings import PaySettingsService
from hds.services.project import ProjectService
from hds.services.project_manager import ProjectManagerService
from hds.services.proposal import ProposalService
from hds.services.reports import ReportsService
from hds.services.roster import RosterService
from hds.services.roster_cover import RosterCoverService
from hds.services.roster_tools import RosterToolsService
from hds.services.saved_view import SavedViewService
from hds.services.schedule_inputs import ScheduleInputService
from hds.services.schedule_job import ScheduleJobService
from hds.services.scheduling_settings import SchedulingSettingsService
from hds.services.special_day import SpecialDayService
from hds.services.staffing_need import StaffingNeedService


def submission_service(db: AsyncSession) -> AvailabilitySubmissionService:
    """Availability submissions and the entries scheduling reads, on ``db`` (007 FR-014)."""
    return AvailabilitySubmissionService(
        window_repo=SubmissionWindowRepository(db),
        entry_repo=AvailabilityRepository(db),
        submission_repo=AvailabilitySubmissionRepository(db),
        user_repo=UserRepository(db),
    )


def availability_submissions(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> AvailabilitySubmissionService:
    """FastAPI dependency: availability submissions, in the request's unit of work."""
    return submission_service(db)


def roster_service_for(db: AsyncSession) -> RosterService:
    """The roster service on ``db``: the one place it is wired."""
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
        edit_events=RosterEditEventRepository(db),
        overrides=RuleOverrideRepository(db),
    )


def roster_tools_for(db: AsyncSession) -> RosterToolsService:
    """The roster tools on ``db``: the one place they are wired."""
    return RosterToolsService(
        roster=roster_service_for(db),
        shift_repo=ShiftRepository(db),
        assignment_repo=AssignmentRepository(db),
        override_repo=RuleOverrideRepository(db),
        template_repo=WeekTemplateRepository(db),
        event_repo=PublishEventRepository(db),
        user_repo=UserRepository(db),
        submissions=submission_service(db),
    )


def roster_tools_service(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> RosterToolsService:
    """FastAPI dependency: the roster tools (007 FR-034 to FR-040), in the unit of work."""
    return roster_tools_for(db)


def saved_views(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> SavedViewService:
    """FastAPI dependency: the caller's saved views (007 FR-043), in the unit of work."""
    return SavedViewService(SavedViewRepository(db))


def input_service(db: AsyncSession) -> ScheduleInputService:
    """The roster problem builder on ``db``."""
    return ScheduleInputService(
        inputs=SchedulingInputRepository(db),
        organisations=OrganisationRepository(db),
        shift_types=ShiftTypeRepository(db),
        needs=StaffingNeedRepository(db),
        entries=AvailabilityRepository(db),
        availability=submission_service(db),
    )


def job_service(db: AsyncSession) -> ScheduleJobService:
    """The schedule job service on ``db``."""
    return ScheduleJobService(
        job_repo=ScheduleJobRepository(db),
        organisation_repo=OrganisationRepository(db),
        inputs=input_service(db),
        project_repo=ProjectRepository(db),
        shift_repo=ShiftRepository(db),
        assignment_repo=AssignmentRepository(db),
        user_repo=UserRepository(db),
        shift_type_repo=ShiftTypeRepository(db),
        project_manager_repo=ProjectManagerRepository(db),
        override_repo=RuleOverrideRepository(db),
        event_repo=ScheduleJobEventRepository(db),
    )


def proposals(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ProposalService:
    """FastAPI dependency: proposals under review (008), in the request's unit of work."""
    return ProposalService(
        jobs=job_service(db),
        inputs=input_service(db),
        users=UserRepository(db),
        organisations=OrganisationRepository(db),
        events=ScheduleJobEventRepository(db),
    )


def roster_cover(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> RosterCoverService:
    """FastAPI dependency: "Find someone" on the roster (008 FR-034), per request."""
    return RosterCoverService(
        roster=roster_service_for(db), inputs=input_service(db), users=UserRepository(db)
    )


def schedule_jobs(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ScheduleJobService:
    """FastAPI dependency: the schedule job service in the request's unit of work."""
    return job_service(db)


def idempotency(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> IdempotencyService:
    """FastAPI dependency: retried writes, in the request's unit of work."""
    return IdempotencyService(IdempotencyKeyRepository(db))


def staffing_needs(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> StaffingNeedService:
    """FastAPI dependency: staffing needs, in the request's unit of work."""
    return StaffingNeedService(
        need_repo=StaffingNeedRepository(db),
        shift_type_repo=ShiftTypeRepository(db),
        project_repo=ProjectRepository(db),
    )


def scheduling_settings(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> SchedulingSettingsService:
    """FastAPI dependency: the organisation's scheduling limits, in the request's unit of work."""
    return SchedulingSettingsService(organisation_repo=OrganisationRepository(db))


def auth_service_for(db: AsyncSession) -> AuthService:
    """The sign-in and session service over ``db``: the one place it is wired."""
    return AuthService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        config=get_settings(),
        link_repo=InvitationTokenRepository(db),
        throttle_repo=LoginThrottleRepository(db),
    )


def organisation_contact(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> OrganisationContactService:
    """FastAPI dependency: the organisation's contact card, in the request's unit of work."""
    return OrganisationContactService(organisation_repo=OrganisationRepository(db))


def pay_settings_service(db: AsyncSession) -> PaySettingsService:
    """The premium settings service on ``db``."""
    return PaySettingsService(
        organisation_repo=OrganisationRepository(db),
        project_repo=ProjectRepository(db),
        special_day_repo=SpecialDayRepository(db),
    )


def pay_settings(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> PaySettingsService:
    """FastAPI dependency: premium settings, in the request's unit of work (005)."""
    return pay_settings_service(db)


def projects(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ProjectService:
    """FastAPI dependency: projects, freezing pay when one ends, in the request's unit of work."""
    return ProjectService(
        project_repo=ProjectRepository(db),
        client_repo=ClientRepository(db),
        site_repo=SiteRepository(db),
        pay_settings=pay_settings_service(db),
    )


def special_days(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> SpecialDayService:
    """FastAPI dependency: special days, in the request's unit of work (005)."""
    return SpecialDayService(
        special_day_repo=SpecialDayRepository(db),
        organisation_repo=OrganisationRepository(db),
        project_repo=ProjectRepository(db),
    )


def project_managers(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ProjectManagerService:
    """FastAPI dependency: managers on projects, in the request's unit of work (005)."""
    return ProjectManagerService(
        project_manager_repo=ProjectManagerRepository(db), project_repo=ProjectRepository(db)
    )


def pay(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> PayService:
    """FastAPI dependency: a person's pay to date, in the request's unit of work (005)."""
    return PayService(
        pay_report_repo=PayReportRepository(db),
        organisation_repo=OrganisationRepository(db),
        special_day_repo=SpecialDayRepository(db),
    )


def reports(
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ReportsService:
    """FastAPI dependency: the work-log and the hours report, in the request's unit of work."""
    return ReportsService(
        report_shift_repo=ReportShiftRepository(db),
        organisation_repo=OrganisationRepository(db),
        special_day_repo=SpecialDayRepository(db),
        project_manager_repo=ProjectManagerRepository(db),
        project_repo=ProjectRepository(db),
        site_repo=SiteRepository(db),
        group_repo=GroupRepository(db),
        client_repo=ClientRepository(db),
    )


@cache
def migrations() -> Migrations:
    """The code's migration history, read once per process.

    The API's lifespan and the worker call it at start-up, so an image whose history is missing or
    has two heads fails at boot with ``load_migrations``' error, not on its first ready check.
    """
    return load_migrations()


@lru_cache(maxsize=8)
def health_service(
    factory: async_sessionmaker[AsyncSession], *, check_migrations: bool = True
) -> HealthService:
    """The health service on ``factory``, built once per factory.

    Each ready check opens and closes its own session on ``factory``, which should be the health
    checks' own (``get_health_session_factory``), never the request pool.

    Args:
        factory: Where each check opens its session.
        check_migrations: Whether a database behind the code is not ready (the API's rule; the
            worker's readiness is the database only).
    """

    @asynccontextmanager
    async def open_probe() -> AsyncIterator[HealthRepository]:
        """A health repository on a session of its own, closed when the check ends."""
        async with factory() as session:
            yield HealthRepository(session)

    return HealthService(
        open_probe,
        migrations(),
        version=get_settings().HDS_VERSION,
        check_migrations=check_migrations,
    )


def health(
    factory: async_sessionmaker[AsyncSession] = Depends(get_health_session_factory),
) -> HealthService:
    """FastAPI dependency: the API's health service, on the health checks' own connections."""
    return health_service(factory)
