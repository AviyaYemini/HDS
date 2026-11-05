"""Client service: manage clients, their users, and enforce deletion guards.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Deactivating or deleting a client also deactivates its portal users and ends their sessions, so a
client that is no longer under contract cannot keep signing in.
"""

import uuid
from datetime import date
from typing import Any

from hds.core import passwords
from hds.core.authz import ADMIN_ROLES, MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, DomainValidationError, NotFoundError
from hds.models.client import Client
from hds.models.refresh_token import RevocationReason
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.employee import validate_initial_password


def _validate_contract_dates(start: date | None, end: date | None) -> None:
    """Refuse a contract that ends before it starts (FR-243).

    Args:
        start: The contract's first day.
        end: Its last day, ``None`` when open-ended.

    Raises:
        DomainValidationError: ``clients.date_range_invalid`` (HTTP 422).
    """
    if start is not None and end is not None and end < start:
        raise DomainValidationError(
            code="clients.date_range_invalid",
            detail="contract_end must not be earlier than contract_start.",
        )


class ClientService:
    """Client records and the portal accounts attached to them."""

    def __init__(
        self,
        client_repo: ClientRepository,
        user_repo: UserRepository,
        link_repo: InvitationTokenRepository,
        token_repo: RefreshTokenRepository,
    ) -> None:
        """Wire the service.

        Args:
            client_repo: Client storage.
            user_repo: User storage for client portal accounts.
            link_repo: Used to cancel the outstanding links of portal users when their client is
                deactivated or deleted, or when they are removed.
            token_repo: Used to end the sessions of those same portal users.
        """
        self._client_repo = client_repo
        self._user_repo = user_repo
        self._link_repo = link_repo
        self._token_repo = token_repo

    async def _deactivate_portal_users(self, client_id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Deactivate every portal user of a client, ending their sessions and links (FR-225).

        The users arrive in ascending id order and are handled in that order, so two simultaneous
        changes to one client cannot take the same rows in opposite orders and deadlock. A pending
        portal user is left alone, with its link (FR-226).

        Args:
            client_id: A client already verified to belong to ``org_id``.
            org_id: The caller's organisation.
        """
        users = await self._client_repo.list_users(client_id, org_id)
        portal_users = [user for user in users if user.activated_at is not None]
        for user in portal_users:  # takes each user's row lock, before any of their links
            await self._token_repo.revoke_all_for_user(user.id, RevocationReason.account)
        await self._link_repo.cancel_for_users([u.id for u in portal_users])
        await self._client_repo.deactivate_client_users(client_id)

    async def list_clients(self, actor: User, is_active: bool | None = None) -> list[Client]:
        """List the organisation's clients (Manager or Admin).

        Args:
            actor: The caller.
            is_active: Keep only active or only inactive clients.

        Returns:
            The clients.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._client_repo.list_by_org(actor.organization_id, is_active)

    async def get_client(self, actor: User, id: uuid.UUID) -> Client:
        """Return one client of the organisation (Manager or Admin).

        Args:
            actor: The caller.
            id: Client to load.

        Returns:
            The client.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The client is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        client = await self._client_repo.get_by_id(id, actor.organization_id)
        if client is None:
            raise NotFoundError(resource="Client", id=id)
        return client

    async def list_client_users(self, actor: User, id: uuid.UUID) -> list[User]:
        """List the portal accounts of a client (Manager or Admin).

        Args:
            actor: The caller.
            id: The client.

        Returns:
            Its portal accounts.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The client is not in the organisation (HTTP 404).
        """
        await self.get_client(actor, id)
        return await self._client_repo.list_users(id, actor.organization_id)

    async def create_client(self, actor: User, data: dict[str, Any]) -> Client:
        """Create a client in the actor's organisation (Manager or Admin).

        Args:
            actor: The caller.
            data: Client fields.

        Returns:
            The new client.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``clients.date_range_invalid`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        _validate_contract_dates(data.get("contract_start"), data.get("contract_end"))
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
            DomainValidationError: ``clients.date_range_invalid`` (HTTP 422), judged on the pair
                as it will be after the change, with the stored value of a date that is not sent,
                and only when that pair differs from the stored one. A service rule and not a
                constraint, because rows stored before it may break it.

        Side effects:
            Locks the client row when either date is sent or on deactivation, before any portal
            user. On deactivation, ends the portal users' sessions and cancels their links.
        """
        ensure_role(actor, *MANAGER_ROLES)
        # A date change locks the row: two requests moving the start and the end at once would
        # each pass against the old pair and together store a broken one. Deactivation locks it
        # too, so every path takes the client row before its portal users and none can deadlock.
        lock = "contract_start" in data or "contract_end" in data or data.get("is_active") is False
        existing = await self._client_repo.get_by_id(id, actor.organization_id, for_update=lock)
        if existing is None:
            raise NotFoundError(resource="Client", id=id)
        start = data.get("contract_start", existing.contract_start)
        end = data.get("contract_end", existing.contract_end)
        if (start, end) != (existing.contract_start, existing.contract_end):
            _validate_contract_dates(start, end)
        if data.get("is_active") is False and existing.is_active:
            await self._deactivate_portal_users(id, actor.organization_id)
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
            ConflictError: ``users.client_inactive`` when the client is inactive, or
                ``users.duplicate_email`` (HTTP 409).
            DomainValidationError: ``users.password_too_short`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        client = await self._client_repo.get_by_id(client_id, actor.organization_id)
        if client is None:
            raise NotFoundError(resource="Client", id=client_id)
        if not client.is_active:
            raise ConflictError(
                code="users.client_inactive",
                detail="This client is inactive; reactivate it before adding users.",
            )
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
        await self._user_repo.release_connection()  # none held while hashing (FR-222)
        data["password_hash"] = await passwords.hash_password(initial_password)
        data["role"] = UserRole.client
        data["client_id"] = client_id
        # Hashing can take a while, so another request may have taken the address since the check.
        user, created = await self._user_repo.create_if_absent(actor.organization_id, data)
        if not created:
            raise ConflictError(
                code="users.duplicate_email",
                detail=f"Email '{email}' is already in use.",
                field="email",
            )
        return user

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

        Side effects:
            Ends the user's sessions and cancels their outstanding links (FR-207, FR-225).
        """
        ensure_role(actor, *MANAGER_ROLES)
        user = await self._user_repo.get_by_id(user_id, actor.organization_id)
        if user is None or user.role != UserRole.client or user.client_id != client_id:
            raise NotFoundError(resource="User", id=user_id)
        await self._user_repo.soft_delete(user_id, actor.organization_id)
        await self._token_repo.revoke_all_for_user(user_id, RevocationReason.account)
        await self._link_repo.cancel_for_user(user_id)

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
            Locks the client row, then deactivates the client's portal users, ends their sessions
            and cancels their links.
        """
        ensure_role(actor, *ADMIN_ROLES)
        # The client row before its portal users, as in edit_client, so the two cannot deadlock.
        existing = await self._client_repo.get_by_id(id, actor.organization_id, for_update=True)
        if existing is None:
            raise NotFoundError(resource="Client", id=id)
        count = await self._client_repo.count_non_deleted_projects(id)
        if count > 0:
            raise ConflictError(
                code="clients.has_projects",
                detail="Cannot delete a client that has active projects.",
            )
        await self._deactivate_portal_users(id, actor.organization_id)
        await self._client_repo.soft_delete(id, actor.organization_id)
