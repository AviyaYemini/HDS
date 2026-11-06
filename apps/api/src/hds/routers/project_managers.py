"""Project managers router: which Managers may see a project's pay when it is open to them.

Layer: routers. Delegates to ``ProjectManagerService``, which ``hds.wiring`` builds; HTTP only.
Managers and Admins read; Admins replace the set (005 FR-021).
"""

import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from hds.core.deps import require_admin, require_manager
from hds.models.user import User
from hds.services.project_manager import ProjectManagerService
from hds.wiring import project_managers

router = APIRouter(prefix="/api/v1", tags=["project managers"])


class ManagersRequest(BaseModel):
    """The complete set of a project's managers; an empty list clears it."""

    user_ids: list[uuid.UUID] = Field(max_length=1000)

    model_config = ConfigDict(extra="forbid")


class ManagerRead(BaseModel):
    """A Manager or Admin on the project."""

    id: uuid.UUID
    full_name: str

    model_config = ConfigDict(from_attributes=True)


class ManagersResponse(BaseModel):
    """A project's managers, by name."""

    items: list[ManagerRead]


@router.get(
    "/projects/{id}/managers",
    response_model=ManagersResponse,
    summary="List a project's managers",
    description="Manager or Admin. The Managers and Admins assigned to the project, by name.",
)
async def list_managers(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ProjectManagerService = Depends(project_managers),
) -> ManagersResponse:
    """List a project's managers.

    Manager or Admin. By name.
    """
    members = await svc.members(current_user, id)
    return ManagersResponse(items=[ManagerRead.model_validate(m) for m in members])


@router.put(
    "/projects/{id}/managers",
    response_model=ManagersResponse,
    summary="Set a project's managers",
    description=(
        "Admin. Replace the project's managers as a whole. Each must be an active Manager or "
        "Admin of the organisation (422 `project_managers.not_staff`). A Manager sees the "
        "project's pay only while assigned and the project's pay is open to Managers."
    ),
)
async def replace_managers(
    id: uuid.UUID,
    body: ManagersRequest,
    current_user: User = Depends(require_admin),
    svc: ProjectManagerService = Depends(project_managers),
) -> ManagersResponse:
    """Set a project's managers.

    Admin. Replace them as a whole; each must be an active Manager or Admin.
    """
    members = await svc.replace(current_user, id, body.user_ids)
    return ManagersResponse(items=[ManagerRead.model_validate(m) for m in members])
