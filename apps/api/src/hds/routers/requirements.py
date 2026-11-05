"""Staffing needs router: how many people each project needs per shift type and weekday.

Layer: routers. Delegates to ``StaffingNeedService``, which ``hds.wiring`` builds; HTTP only.
Managers and Admins only.
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from hds.core.deps import require_manager
from hds.core.types import Weekday
from hds.models.user import User
from hds.services.staffing_need import StaffingNeedService
from hds.wiring import staffing_needs

router = APIRouter(prefix="/api/v1", tags=["staffing needs"])

Headcount = Annotated[int, Field(ge=1, le=999, strict=True)]
# Seven days times the shift types a project could use; far more than any real set, and small
# enough that the request's ids always fit one query.
MAX_NEEDS = 1000


class NeedItem(BaseModel):
    """People needed on one shift type on one weekday; any other field is refused with 422."""

    shift_type_id: uuid.UUID
    weekday: Weekday
    headcount: Headcount

    model_config = ConfigDict(extra="forbid", from_attributes=True)


class NeedsRequest(BaseModel):
    """A project's complete set of needs; an empty list removes them all."""

    items: list[NeedItem] = Field(max_length=MAX_NEEDS)

    model_config = ConfigDict(extra="forbid")


class NeedsResponse(BaseModel):
    """A project's needs, by weekday."""

    items: list[NeedItem]


@router.get(
    "/projects/{id}/requirements",
    response_model=NeedsResponse,
    summary="List a project's staffing needs",
    description="Manager or Admin. How many people the project needs per shift type and weekday.",
)
async def list_needs(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: StaffingNeedService = Depends(staffing_needs),
) -> NeedsResponse:
    """List a project's staffing needs.

    Manager or Admin. How many people the project needs per shift type and weekday.
    """
    needs = await svc.list_needs(current_user, id)
    return NeedsResponse(items=[NeedItem.model_validate(n) for n in needs])


@router.put(
    "/projects/{id}/requirements",
    response_model=NeedsResponse,
    summary="Set a project's staffing needs",
    description=(
        "Manager or Admin. Replace the project's needs as a whole. Each shift type must be one "
        "the project uses (422 `requirements.shift_type_not_in_project`) and not retired (422 "
        "`requirements.shift_type_retired`); a shift type and weekday appear once (422 "
        "`requirements.duplicate`)."
    ),
)
async def replace_needs(
    id: uuid.UUID,
    body: NeedsRequest,
    current_user: User = Depends(require_manager),
    svc: StaffingNeedService = Depends(staffing_needs),
) -> NeedsResponse:
    """Set a project's staffing needs.

    Manager or Admin. Replace them as a whole; a type the project does not use, a retired type or a
    repeated shift type and weekday is refused with 422.
    """
    items = [item.model_dump() for item in body.items]
    needs = await svc.replace_needs(current_user, id, items)
    return NeedsResponse(items=[NeedItem.model_validate(n) for n in needs])
