"""Projects router: CRUD and status management."""

import uuid
from datetime import date, datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import get_current_user, require_admin, require_manager
from hds.models.project import ProjectStatus, ProjectType
from hds.models.user import User
from hds.repositories.client import ClientRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.site import SiteRepository
from hds.services.project import ProjectService

router = APIRouter(prefix="/api/v1/projects", tags=["projects"])


class ProjectRead(BaseModel):
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
    client_id: uuid.UUID
    site_id: uuid.UUID
    name: str
    type: ProjectType
    billing_rate: Decimal
    start_date: date
    end_date: date | None = None


class ProjectUpdateRequest(BaseModel):
    name: str | None = None
    billing_rate: Decimal | None = None
    end_date: date | None = None
    status: ProjectStatus | None = None


class ProjectListResponse(BaseModel):
    items: list[ProjectRead]


def _svc(db: AsyncSession) -> ProjectService:
    return ProjectService(
        project_repo=ProjectRepository(db),
        client_repo=ClientRepository(db),
        site_repo=SiteRepository(db),
    )


@router.get("", response_model=ProjectListResponse)
async def list_projects(
    status: ProjectStatus | None = None,
    client_id: uuid.UUID | None = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> ProjectListResponse:
    svc = _svc(db)
    projects = await svc.list_projects(
        org_id=current_user.organization_id,
        current_user=current_user,
        status=status,
        client_id=client_id,
    )
    return ProjectListResponse(items=projects)  # type: ignore[arg-type]


@router.post("", response_model=ProjectRead, status_code=201)
async def create_project(
    body: ProjectCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = _svc(db)
    return await svc.create_project(actor=current_user, data=body.model_dump())


@router.get("/{id}", response_model=ProjectRead)
async def get_project(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = _svc(db)
    return await svc.get_project(
        id=id, org_id=current_user.organization_id, current_user=current_user
    )


@router.patch("/{id}", response_model=ProjectRead)
async def edit_project(
    id: uuid.UUID,
    body: ProjectUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = _svc(db)
    data = body.model_dump(exclude_none=True)
    new_status = data.pop("status", None)
    project = None
    if data or new_status is None:
        project = await svc.edit_project(actor=current_user, id=id, data=data)
    if new_status is not None:
        project = await svc.set_status(actor=current_user, id=id, status=new_status)
    return project


@router.delete("/{id}", status_code=204)
async def delete_project(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = _svc(db)
    await svc.soft_delete(actor=current_user, id=id)
