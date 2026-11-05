"""Projects router: CRUD and status management.

Layer: routers. Delegates to ``ProjectService``, which ``hds.wiring`` builds. Reads are open to
Manager, Admin and Client (a Client is narrowed to their own projects in the service); writes need
Manager or Admin and deletion needs Admin.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from hds.core.deps import require_admin, require_manager, require_manager_or_client
from hds.core.types import ChangeRequest, Money, Name, StrictDate, omittable
from hds.models.project import ProjectStatus, ProjectType
from hds.models.user import User
from hds.routers.pay_settings import ProjectPayChanges, project_pay_changes
from hds.services.project import ProjectService
from hds.wiring import projects

router = APIRouter(prefix="/api/v1/projects", tags=["projects"])


class ProjectRead(BaseModel):
    """A project; ``billing_rate`` is a decimal string, never a float."""

    id: uuid.UUID
    organization_id: uuid.UUID
    client_id: uuid.UUID
    site_id: uuid.UUID
    name: str
    type: ProjectType
    billing_rate: Decimal
    start_date: date
    end_date: date | None = None
    status: ProjectStatus
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProjectCreateRequest(BaseModel):
    """New project.

    ``billing_rate`` must be greater than zero and ``end_date`` not before ``start_date``.
    ``pay_settings`` (Admin only) sets the project's pay switches and its own premium values;
    without it the project has pay, closed to Managers, and follows every default (005 FR-007).
    """

    client_id: uuid.UUID
    site_id: uuid.UUID
    name: Name
    type: ProjectType
    billing_rate: Money
    start_date: StrictDate
    end_date: StrictDate | None = None
    pay_settings: ProjectPayChanges | None = None


class ProjectUpdateRequest(ChangeRequest):
    """Project fields that may change; omitted fields are left as they are, null clears."""

    name: Name = omittable()
    billing_rate: Money = omittable()
    end_date: StrictDate | None = None
    status: ProjectStatus = omittable()


class ProjectListResponse(BaseModel):
    """The projects visible to the caller."""

    items: list[ProjectRead]


@router.get(
    "",
    response_model=ProjectListResponse,
    summary="List projects",
    description=(
        "Manager or Admin see every project of the organisation, optionally filtered by "
        "status or client. A Client sees only their own client's projects whatever "
        "filter is sent, and none if the account has no client. Employees receive 403."
    ),
)
async def list_projects(
    status: ProjectStatus | None = None,
    client_id: uuid.UUID | None = None,
    current_user: User = Depends(require_manager_or_client),
    svc: ProjectService = Depends(projects),
) -> ProjectListResponse:
    """List projects.

    Manager or Admin see every project of the organisation, optionally filtered by status or
    client. A Client sees only their own client's projects whatever filter is sent, and none if
    the account has no client. Employees receive 403.
    """
    projects = await svc.list_projects(
        org_id=current_user.organization_id,
        current_user=current_user,
        status=status,
        client_id=client_id,
    )
    return ProjectListResponse(items=projects)  # type: ignore[arg-type]


@router.post(
    "",
    response_model=ProjectRead,
    status_code=201,
    summary="Create a project",
    description=(
        "Manager or Admin. Create a project for a client and a site of the "
        "organisation. The billing rate must be greater than zero "
        "(`projects.billing_rate_invalid`) and the end date must not be before the "
        "start date (`projects.date_range_invalid`). Only an Admin may send `pay_settings` "
        "(403 `projects.pay_settings_admin_only`)."
    ),
)
async def create_project(
    body: ProjectCreateRequest,
    current_user: User = Depends(require_manager),
    svc: ProjectService = Depends(projects),
) -> object:
    """Create a project.

    Manager or Admin. Create a project for a client and a site of the organisation. The billing
    rate must be greater than zero (`projects.billing_rate_invalid`) and the end date must not
    be before the start date (`projects.date_range_invalid`).
    """
    data = body.model_dump(exclude={"pay_settings"})
    if body.pay_settings is not None:
        data["pay_settings"] = project_pay_changes(body.pay_settings)
    return await svc.create_project(actor=current_user, data=data)


@router.get(
    "/{id}",
    response_model=ProjectRead,
    summary="Get a project",
    description=(
        "Manager or Admin for any project; a Client only for their own client's "
        "projects (otherwise 403 `projects.access_denied`), and 404 if the account has no "
        "client. Employees receive 403."
    ),
)
async def get_project(
    id: uuid.UUID,
    current_user: User = Depends(require_manager_or_client),
    svc: ProjectService = Depends(projects),
) -> object:
    """Get a project.

    Manager or Admin for any project; a Client only for their own client's projects (otherwise
    403 `projects.access_denied`), and 404 if the account has no client. Employees receive 403.
    """
    return await svc.get_project(
        id=id, org_id=current_user.organization_id, current_user=current_user
    )


@router.patch(
    "/{id}",
    response_model=ProjectRead,
    summary="Edit a project",
    description=(
        "Manager or Admin. Update name, billing rate, end date or status. The same rate "
        "and date rules as creation apply. "
        "An explicit null clears a field that allows it and is refused with 422 on a "
        "required one; an omitted field is left alone."
    ),
)
async def edit_project(
    id: uuid.UUID,
    body: ProjectUpdateRequest,
    current_user: User = Depends(require_manager),
    svc: ProjectService = Depends(projects),
) -> object:
    """Edit a project.

    Manager or Admin. Update name, billing rate, end date or status. The same rate and date
    rules as creation apply.
    An explicit null clears a field that allows it and is refused with 422 on a required one; an
    omitted field is left alone.
    """
    data = body.model_dump(exclude_unset=True)
    new_status = data.pop("status", None)
    return await svc.change_project(current_user, id, data, new_status)


@router.delete(
    "/{id}",
    status_code=204,
    summary="Delete a project",
    description=("Admin only. Soft-delete the project; the record is kept and hidden."),
)
async def delete_project(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    svc: ProjectService = Depends(projects),
) -> None:
    """Delete a project.

    Admin only. Soft-delete the project; the record is kept and hidden.
    """
    await svc.soft_delete(actor=current_user, id=id)
