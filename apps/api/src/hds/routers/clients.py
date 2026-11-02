"""Clients router: CRUD for clients and their users.

Layer: routers. Delegates to ``ClientService``; Manager or Admin, except deletion which is
Admin only.
"""

import uuid
from datetime import date, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr, Field, StrictBool
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import require_admin, require_manager
from hds.core.types import Name, Password, Phone, StrictDate
from hds.models.user import User
from hds.repositories.client import ClientRepository
from hds.repositories.user import UserRepository
from hds.services.client import ClientService

router = APIRouter(prefix="/api/v1/clients", tags=["clients"])


class ClientRead(BaseModel):
    """A client and its contract dates."""

    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    contact_name: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    contract_start: date
    contract_end: date | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ClientCreateRequest(BaseModel):
    """New client; ``contract_start`` is required and dates are ISO-8601 strings."""

    name: Name
    contact_name: Name | None = None
    contact_email: EmailStr | None = None
    contact_phone: Phone | None = None
    contract_start: StrictDate
    contract_end: StrictDate | None = None


class ClientUpdateRequest(BaseModel):
    """Client fields that may change; omitted fields are left as they are."""

    name: Name | None = None
    contact_name: Name | None = None
    contact_email: EmailStr | None = None
    contact_phone: Phone | None = None
    contract_end: StrictDate | None = None
    is_active: StrictBool | None = None


class ClientListResponse(BaseModel):
    """The organisation's clients."""

    items: list[ClientRead]


class ClientUserRead(BaseModel):
    """A client portal account."""

    id: uuid.UUID
    organization_id: uuid.UUID
    client_id: uuid.UUID | None = None
    email: str
    full_name: str
    phone: str | None = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ClientUserCreateRequest(BaseModel):
    """New portal account; the initial password needs at least 12 characters."""

    email: EmailStr
    full_name: Name
    phone: Phone | None = None
    initial_password: Password = Field(min_length=12)


def _svc(db: AsyncSession) -> ClientService:
    return ClientService(client_repo=ClientRepository(db), user_repo=UserRepository(db))


@router.get(
    "",
    response_model=ClientListResponse,
    summary="List clients",
    description=(
        "Manager or Admin. Return the organisation's non-deleted clients, optionally "
        "only active or only inactive ones."
    ),
)
async def list_clients(
    is_active: bool | None = None,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> ClientListResponse:
    """List clients.

    Manager or Admin. Return the organisation's non-deleted clients, optionally only active or
    only inactive ones.
    """
    clients = await ClientRepository(db).list_by_org(current_user.organization_id, is_active)
    return ClientListResponse(items=clients)  # type: ignore[arg-type]


@router.post(
    "",
    response_model=ClientRead,
    status_code=201,
    summary="Create a client",
    description=(
        "Manager or Admin. Create a client with its contract start date and optional "
        "contact details."
    ),
)
async def create_client(
    body: ClientCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    """Create a client.

    Manager or Admin. Create a client with its contract start date and optional contact details.
    """
    svc = _svc(db)
    return await svc.create_client(
        actor=current_user,
        data=body.model_dump(exclude_none=True),
    )


@router.get(
    "/{id}",
    response_model=ClientRead,
    summary="Get a client",
    description=("Manager or Admin. Return one client of the organisation."),
)
async def get_client(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    """Get a client.

    Manager or Admin. Return one client of the organisation.
    """
    from hds.core.exceptions import NotFoundError

    client = await ClientRepository(db).get_by_id(id, current_user.organization_id)
    if client is None:
        raise NotFoundError(resource="Client", id=id)
    return client


@router.patch(
    "/{id}",
    response_model=ClientRead,
    summary="Edit a client",
    description=(
        "Manager or Admin. Update contact details, contract end or active state. "
        "Deactivating an active client also deactivates its portal users; reactivating "
        "the client does not reactivate them."
    ),
)
async def edit_client(
    id: uuid.UUID,
    body: ClientUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    """Edit a client.

    Manager or Admin. Update contact details, contract end or active state. Deactivating an
    active client also deactivates its portal users; reactivating the client does not reactivate
    them.
    """
    svc = _svc(db)
    return await svc.edit_client(
        actor=current_user,
        id=id,
        data=body.model_dump(exclude_none=True),
    )


@router.delete(
    "/{id}",
    status_code=204,
    summary="Delete a client",
    description=(
        "Admin only. Soft-delete a client and deactivate its portal users. Refused with "
        "409 (`clients.has_projects`) while the client still has projects."
    ),
)
async def delete_client(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> None:
    """Delete a client.

    Admin only. Soft-delete a client and deactivate its portal users. Refused with 409
    (`clients.has_projects`) while the client still has projects.
    """
    svc = _svc(db)
    await svc.soft_delete(actor=current_user, id=id)


@router.get(
    "/{id}/users",
    response_model=list[ClientUserRead],
    summary="List a client's users",
    description=("Manager or Admin. Return the portal accounts attached to the client."),
)
async def list_client_users(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> list[User]:
    """List a client's users.

    Manager or Admin. Return the portal accounts attached to the client.
    """
    from hds.core.exceptions import NotFoundError

    repo = ClientRepository(db)
    if await repo.get_by_id(id, current_user.organization_id) is None:
        raise NotFoundError(resource="Client", id=id)
    return await repo.list_users(id, current_user.organization_id)


@router.post(
    "/{id}/users",
    response_model=ClientUserRead,
    status_code=201,
    summary="Create a client user",
    description=(
        "Manager or Admin. Create a portal account with the Client role for this "
        "client, with an initial password of at least 12 characters. A duplicate email "
        "returns 409 (`users.duplicate_email`)."
    ),
)
async def create_client_user(
    id: uuid.UUID,
    body: ClientUserCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    """Create a client user.

    Manager or Admin. Create a portal account with the Client role for this client, with an
    initial password of at least 12 characters. A duplicate email returns 409
    (`users.duplicate_email`).
    """
    svc = _svc(db)
    return await svc.create_client_user(
        actor=current_user,
        client_id=id,
        data=body.model_dump(),
    )


@router.delete(
    "/{id}/users/{user_id}",
    status_code=204,
    summary="Remove a client user",
    description=(
        "Manager or Admin. Soft-delete a portal account of this client. Staff accounts "
        "cannot be removed here and return 404."
    ),
)
async def remove_client_user(
    id: uuid.UUID,
    user_id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> None:
    """Remove a client user.

    Manager or Admin. Soft-delete a portal account of this client. Staff accounts cannot be
    removed here and return 404.
    """
    svc = _svc(db)
    await svc.remove_client_user(actor=current_user, client_id=id, user_id=user_id)
