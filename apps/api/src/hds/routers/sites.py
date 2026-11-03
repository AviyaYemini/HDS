"""Sites router: CRUD for organisation sites.

Layer: routers. Delegates to ``SiteService``; all endpoints require Manager or Admin. There is
no delete endpoint.
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import require_manager
from hds.core.types import Address, Name, Timezone, omittable
from hds.models.user import User
from hds.repositories.site import SiteRepository
from hds.services.site import SiteService

router = APIRouter(prefix="/api/v1/sites", tags=["sites"])


class SiteRead(BaseModel):
    """A site with its IANA time zone."""

    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    address: str
    timezone: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SiteCreateRequest(BaseModel):
    """New site; ``timezone`` is an IANA zone name such as ``Asia/Jerusalem``."""

    name: Name
    address: Address
    timezone: Timezone


class SiteUpdateRequest(BaseModel):
    """Site fields that may change; omitted fields are left as they are."""

    name: Name = omittable()
    address: Address = omittable()
    timezone: Timezone = omittable()


class SiteListResponse(BaseModel):
    """The organisation's sites."""

    items: list[SiteRead]


@router.get(
    "",
    response_model=SiteListResponse,
    summary="List sites",
    description=("Manager or Admin. Return the organisation's sites ordered by name."),
)
async def list_sites(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> SiteListResponse:
    """List sites.

    Manager or Admin. Return the organisation's sites ordered by name.
    """
    sites = await SiteRepository(db).list_by_org(current_user.organization_id)
    return SiteListResponse(items=sites)  # type: ignore[arg-type]


@router.post(
    "",
    response_model=SiteRead,
    status_code=201,
    summary="Create a site",
    description=(
        "Manager or Admin. Create a site with its address and IANA time zone. A time "
        "zone that is not an exact IANA name is refused with 422 (`sites.timezone_invalid`). "
        "Sites cannot be deleted."
    ),
)
async def create_site(
    body: SiteCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Create a site.

    Manager or Admin. Create a site with its address and IANA time zone. A time zone that is not
    an exact IANA name is refused with 422 (`sites.timezone_invalid`). Sites cannot be deleted.
    """
    svc = SiteService(site_repo=SiteRepository(db))
    return await svc.create_site(actor=current_user, data=body.model_dump())


@router.get(
    "/{id}",
    response_model=SiteRead,
    summary="Get a site",
    description=("Manager or Admin. Return one site of the organisation."),
)
async def get_site(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Get a site.

    Manager or Admin. Return one site of the organisation.
    """
    from hds.core.exceptions import NotFoundError

    site = await SiteRepository(db).get_by_id(id, current_user.organization_id)
    if site is None:
        raise NotFoundError(resource="Site", id=id)
    return site


@router.patch(
    "/{id}",
    response_model=SiteRead,
    summary="Edit a site",
    description=(
        "Manager or Admin. Update a site's name, address or time zone. A time zone that "
        "is not an exact IANA name is refused with 422 (`sites.timezone_invalid`). "
        "Every field here is required, so null is refused with 422; an omitted field is "
        "left alone."
    ),
)
async def edit_site(
    id: uuid.UUID,
    body: SiteUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Edit a site.

    Manager or Admin. Update a site's name, address or time zone. A time zone that is not a real
    IANA name is refused with 422 (`sites.timezone_invalid`).
    Every field here is required, so null is refused with 422; an omitted field is left alone.
    """
    svc = SiteService(site_repo=SiteRepository(db))
    return await svc.edit_site(
        actor=current_user,
        id=id,
        data=body.model_dump(exclude_unset=True),
    )
