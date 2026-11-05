"""Scheduling settings router: the organisation's minimum rest and weekly maximum.

Layer: routers. Delegates to ``SchedulingSettingsService``, which ``hds.wiring`` builds; HTTP only.
Managers and Admins read the limits; Admins change them.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from hds.core.deps import require_admin, require_manager
from hds.core.types import ChangeRequest, omittable
from hds.models.user import User
from hds.services.scheduling_settings import SchedulingSettingsService
from hds.wiring import scheduling_settings

router = APIRouter(prefix="/api/v1", tags=["scheduling settings"])

# Not strict: JSON's 5.0 is the integer 5, as the published schema says; 5.5 and "eleven" are still
# refused.
RestHours = Annotated[int, Field(ge=1, le=24)]
WeeklyHours = Annotated[int, Field(ge=1, le=168)]


class SchedulingSettings(BaseModel):
    """The limits the roster is generated and checked against, in whole hours."""

    min_rest_hours: int
    max_weekly_hours: int

    model_config = {"from_attributes": True}


class SchedulingSettingsUpdate(ChangeRequest):
    """Either limit; any other field is refused with 422."""

    min_rest_hours: RestHours = omittable()
    max_weekly_hours: WeeklyHours = omittable()


@router.get(
    "/organisation/scheduling",
    response_model=SchedulingSettings,
    summary="Read the scheduling limits",
    description=(
        "Manager or Admin. The minimum rest between shifts and the maximum hours per week, used "
        "by generation and by the warnings of manual rostering."
    ),
)
async def read_settings(
    current_user: User = Depends(require_manager),
    svc: SchedulingSettingsService = Depends(scheduling_settings),
) -> object:
    """Read the scheduling limits.

    Manager or Admin. The minimum rest between shifts and the maximum hours per week.
    """
    return await svc.read(current_user)


@router.patch(
    "/organisation/scheduling",
    response_model=SchedulingSettings,
    summary="Change the scheduling limits",
    description=(
        "Admin. Change the minimum rest (1 to 24 hours) or the weekly maximum (1 to 168 hours); "
        "later generation and warnings use the new values."
    ),
)
async def change_settings(
    body: SchedulingSettingsUpdate,
    current_user: User = Depends(require_admin),
    svc: SchedulingSettingsService = Depends(scheduling_settings),
) -> object:
    """Change the scheduling limits.

    Admin. Minimum rest 1 to 24 hours, weekly maximum 1 to 168 hours.
    """
    return await svc.change(current_user, body.model_dump(exclude_unset=True))
