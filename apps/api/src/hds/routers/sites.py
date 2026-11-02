"""Sites router: CRUD for organisation sites."""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import require_manager
from hds.core.types import Address, Name, Timezone
from hds.models.user import User
from hds.repositories.site import SiteRepository
from hds.services.site import SiteService

router = APIRouter(prefix="/api/v1/sites", tags=["sites"])


class SiteRead(BaseModel):
    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    address: str
    timezone: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SiteCreateRequest(BaseModel):
    name: Name
    address: Address
    timezone: Timezone


class SiteUpdateRequest(BaseModel):
    name: Name | None = None
    address: Address | None = None
    timezone: Timezone | None = None


class SiteListResponse(BaseModel):
    items: list[SiteRead]


@router.get("", response_model=SiteListResponse)
async def list_sites(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> SiteListResponse:
    sites = await SiteRepository(db).list_by_org(current_user.organization_id)
    return SiteListResponse(items=sites)  # type: ignore[arg-type]


@router.post("", response_model=SiteRead, status_code=201)
async def create_site(
    body: SiteCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = SiteService(site_repo=SiteRepository(db))
    return await svc.create_site(actor=current_user, data=body.model_dump())


@router.get("/{id}", response_model=SiteRead)
async def get_site(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    from hds.core.exceptions import NotFoundError

    site = await SiteRepository(db).get_by_id(id, current_user.organization_id)
    if site is None:
        raise NotFoundError(resource="Site", id=id)
    return site


@router.patch("/{id}", response_model=SiteRead)
async def edit_site(
    id: uuid.UUID,
    body: SiteUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = SiteService(site_repo=SiteRepository(db))
    return await svc.edit_site(
        actor=current_user,
        id=id,
        data=body.model_dump(exclude_none=True),
    )
