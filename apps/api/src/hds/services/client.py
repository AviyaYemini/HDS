"""Client service: manage clients, their users, and enforce deletion guards.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Deactivating or deleting a client also deactivates its portal users, so a client that is no
longer under contract cannot keep signing in.
"""

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
    """Client records and the portal accounts attached to them."""

    def __init__(self, client_repo: ClientRepository, user_repo: UserRepository) -> None:
        """Wire the service.

        Args:
            client_repo: Client storage.
            user_repo: User storage for client portal accounts.
        """
        self._client_repo = client_repo
        self._user_repo = user_repo

    async def create_client(self, actor: User, data: dict[str, Any]) -> Client:
        """Create a client in the actor's organisation (Manager or Admin).

        Args:
            actor: The caller.
            data: Client fields.

        Returns:
            The new client.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._client_repo.create(actor.organization_id, data)

    async def edit_client(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> Client:
        """Update a client (Manager or Admin).

        Setting ``is_active`` to false on an active client also deactivates its portal users.
        Reactivating the client does not reactivate them; that is a deliberate manual step.

        Args:
            actor: The caller.
            id: Client to edit.
            data: Fields to change.

        Returns:
            The updated client.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The client is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._client_repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Client", id=id)
        if data.get("is_active") is False and existing.is_active:
            await self._client_repo.deactivate_client_users(id)
        return await self._client_repo.update(id, actor.organization_id, **data)

    async def create_client_user(
        self, actor: User, client_id: uuid.UUID, data: dict[str, Any]
    ) -> User:
        """Create a portal user for a client (Manager or Admin).

        The role and client link are forced here, ignoring any ``role`` in ``data``.

        Args:
            actor: The caller.
            client_id: Client the user belongs to.
            data: User fields including ``initial_password`` (at least 12 characters).

        Returns:
            The new Client-role user.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The client is not in the organisation (HTTP 404).
            ConflictError: ``users.duplicate_email`` (HTTP 409).
            DomainValidationError: ``users.password_too_short`` (HTTP 422).
        """
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
        """Soft-delete a client's portal user (Manager or Admin).

        Args:
            actor: The caller.
            client_id: Client the user must belong to.
            user_id: User to remove.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: No such Client-role user under that client in the organisation
                (HTTP 404). Staff accounts cannot be removed through this method.
        """
        ensure_role(actor, *MANAGER_ROLES)
        user = await self._user_repo.get_by_id(user_id, actor.organization_id)
        if user is None or user.role != UserRole.client or user.client_id != client_id:
            raise NotFoundError(resource="User", id=user_id)
        await self._user_repo.soft_delete(user_id, actor.organization_id)

    async def soft_delete(self, actor: User, id: uuid.UUID) -> None:
        """Delete a client that has no live projects (Admin only).

        Args:
            actor: The caller.
            id: Client to delete.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The client is not in the organisation (HTTP 404).
            ConflictError: ``clients.has_projects`` while non-deleted projects remain
                (HTTP 409).

        Side effects:
            Deactivates the client's portal users.
        """
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
        await self._client_repo.soft_delete(id, actor.organization_id)
