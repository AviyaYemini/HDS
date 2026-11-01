"""Client service: manage clients, their users, and enforce deletion guards."""

import uuid
from typing import Any

from hds.core.authz import ADMIN_ROLES, MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.user import UserRepository
from hds.services.employee import validate_initial_password


class ClientService:
    def __init__(self, client_repo: ClientRepository, user_repo: UserRepository) -> None:
        self._client_repo = client_repo
        self._user_repo = user_repo

    async def create_client(self, actor: User, data: dict[str, Any]) -> Client:
        ensure_role(actor, *MANAGER_ROLES)
        return await self._client_repo.create(actor.organization_id, data)

    async def edit_client(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> Client:
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._client_repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Client", id=id)
        if data.get("is_active") is False and existing.is_active:
            await self._client_repo.deactivate_client_users(id)
        return await self._client_repo.update(id, **data)

    async def create_client_user(
        self, actor: User, client_id: uuid.UUID, data: dict[str, Any]
    ) -> User:
        ensure_role(actor, *MANAGER_ROLES)
        if await self._client_repo.get_by_id(client_id, actor.organization_id) is None:
            raise NotFoundError(resource="Client", id=client_id)
        email = data.get("email", "")
        if await self._user_repo.get_by_email(email) is not None:
            raise ConflictError(
                code="users.duplicate_email",
                detail=f"Email '{email}' is already in use.",
                field="email",
            )
        data = dict(data)
        initial_password = data.pop("initial_password")
        validate_initial_password(initial_password)
        data["password_hash"] = hash_password(initial_password)
        data["role"] = UserRole.client
        data["client_id"] = client_id
        return await self._user_repo.create(actor.organization_id, data)

    async def remove_client_user(
        self, actor: User, client_id: uuid.UUID, user_id: uuid.UUID
    ) -> None:
        ensure_role(actor, *MANAGER_ROLES)
        user = await self._user_repo.get_by_id(user_id, actor.organization_id)
        if user is None or user.role != UserRole.client or user.client_id != client_id:
            raise NotFoundError(resource="User", id=user_id)
        await self._user_repo.soft_delete(user_id)

    async def soft_delete(self, actor: User, id: uuid.UUID) -> None:
        ensure_role(actor, *ADMIN_ROLES)
        existing = await self._client_repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Client", id=id)
        count = await self._client_repo.count_non_deleted_projects(id)
        if count > 0:
            raise ConflictError(
                code="clients.has_projects",
                detail="Cannot delete a client that has active projects.",
            )
        await self._client_repo.deactivate_client_users(id)
        await self._client_repo.soft_delete(id)
