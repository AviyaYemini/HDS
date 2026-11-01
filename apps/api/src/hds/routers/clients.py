"""Clients router: CRUD for clients and their users."""

import uuid
from datetime import date, datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import require_admin, require_manager
from hds.models.user import User
from hds.repositories.client import ClientRepository
from hds.repositories.user import UserRepository
from hds.services.client import ClientService

router = APIRouter(prefix="/api/v1/clients", tags=["clients"])


class ClientRead(BaseModel):
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
    name: str
    contact_name: str | None = None
    contact_email: EmailStr | None = None
    contact_phone: str | None = None
    contract_start: date
    contract_end: date | None = None


class ClientUpdateRequest(BaseModel):
    name: str | None = None
    contact_name: str | None = None
    contact_email: EmailStr | None = None
    contact_phone: str | None = None
    contract_end: date | None = None
    is_active: bool | None = None


class ClientListResponse(BaseModel):
    items: list[ClientRead]


class ClientUserRead(BaseModel):
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
    email: EmailStr
    full_name: str
    phone: str | None = None
    initial_password: str = Field(min_length=12)


def _svc(db: AsyncSession) -> ClientService:
    return ClientService(client_repo=ClientRepository(db), user_repo=UserRepository(db))


@router.get("", response_model=ClientListResponse)
async def list_clients(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> ClientListResponse:
    clients = await ClientRepository(db).list_by_org(current_user.organization_id)
    return ClientListResponse(items=clients)  # type: ignore[arg-type]


@router.post("", response_model=ClientRead, status_code=201)
async def create_client(
    body: ClientCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = _svc(db)
    return await svc.create_client(
        actor=current_user,
        data=body.model_dump(exclude_none=True),
    )


@router.get("/{id}", response_model=ClientRead)
async def get_client(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    from hds.core.exceptions import NotFoundError

    client = await ClientRepository(db).get_by_id(id, current_user.organization_id)
    if client is None:
        raise NotFoundError(resource="Client", id=id)
    return client


@router.patch("/{id}", response_model=ClientRead)
async def edit_client(
    id: uuid.UUID,
    body: ClientUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = _svc(db)
    return await svc.edit_client(
        actor=current_user,
        id=id,
        data=body.model_dump(exclude_none=True),
    )


@router.delete("/{id}", status_code=204)
async def delete_client(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = _svc(db)
    await svc.soft_delete(actor=current_user, id=id)


@router.get("/{id}/users", response_model=list[ClientUserRead])
async def list_client_users(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> list[User]:
    from hds.core.exceptions import NotFoundError

    repo = ClientRepository(db)
    if await repo.get_by_id(id, current_user.organization_id) is None:
        raise NotFoundError(resource="Client", id=id)
    return await repo.list_users(id, current_user.organization_id)


@router.post("/{id}/users", response_model=ClientUserRead, status_code=201)
async def create_client_user(
    id: uuid.UUID,
    body: ClientUserCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = _svc(db)
    return await svc.create_client_user(
        actor=current_user,
        client_id=id,
        data=body.model_dump(),
    )


@router.delete("/{id}/users/{user_id}", status_code=204)
async def remove_client_user(
    id: uuid.UUID,
    user_id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = _svc(db)
    await svc.remove_client_user(actor=current_user, client_id=id, user_id=user_id)
