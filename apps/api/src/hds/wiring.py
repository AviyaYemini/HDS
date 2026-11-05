"""Builds services on a session: the composition root for routers and the worker (004).

Layer: composition root, beside ``hds.main``. It constructs services with their repositories and
holds no rule. Routers receive ready services through the FastAPI dependencies here, so they import
neither the database nor a repository (Constitution II); the worker and the tests use the plain
builders.
"""

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.idempotency_key import IdempotencyKeyRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.schedule_job import ScheduleJobRepository
from hds.repositories.scheduling_inputs import SchedulingInputRepository
from hds.repositories.shift import AssignmentRepository, ShiftRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.staffing_need import StaffingNeedRepository
from hds.repositories.user import UserRepository
from hds.services.idempotency import IdempotencyService
from hds.services.schedule_inputs import ScheduleInputService
from hds.services.schedule_job import ScheduleJobService
from hds.services.scheduling_settings import SchedulingSettingsService
from hds.services.staffing_need import StaffingNeedService


def input_service(db: AsyncSession) -> ScheduleInputService:
    """The roster problem builder on ``db``."""
    return ScheduleInputService(
        inputs=SchedulingInputRepository(db),
        organisations=OrganisationRepository(db),
        shift_types=ShiftTypeRepository(db),
        needs=StaffingNeedRepository(db),
        entries=AvailabilityRepository(db),
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
