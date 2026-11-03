"""Employee service: self-service profile and manager operations.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Holds the rules for who may change whom: only an Admin may change or create an Admin (FR-011a),
the last active Admin can never be removed (FR-011), and any change to a user's access ends their
sessions so it takes effect at once.
"""

import uuid
from decimal import Decimal
from typing import Any

from hds.core import passwords
from hds.core.authz import ADMIN_ROLES, MANAGER_ROLES, ensure_role, ensure_self_or_manager
from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
)
from hds.models.group import Group
from hds.models.qualification import Qualification
from hds.models.refresh_token import RevocationReason
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository

# Fields that an employee is permitted to edit on their own profile
_ALLOWED_SELF_FIELDS = frozenset({"full_name", "phone"})

MIN_PASSWORD_LENGTH = 12


def validate_initial_password(password: str) -> None:
    """Reject a password shorter than ``MIN_PASSWORD_LENGTH`` characters.

    Args:
        password: Clear-text initial password.

    Raises:
        DomainValidationError: ``users.password_too_short`` (HTTP 422).
    """
    if len(password) < MIN_PASSWORD_LENGTH:
        raise DomainValidationError(
            code="users.password_too_short",
            detail=f"Password must be at least {MIN_PASSWORD_LENGTH} characters.",
        )


def validate_hourly_rate(rate: Any) -> None:
    """Reject a zero or negative hourly rate (Constitution VII: rates are positive).

    Args:
        rate: Value convertible to ``Decimal``, or ``None`` when no rate is set.

    Raises:
        DomainValidationError: ``users.hourly_rate_invalid`` (HTTP 422).
    """
    if rate is not None and Decimal(str(rate)) <= Decimal("0"):
        raise DomainValidationError(
            code="users.hourly_rate_invalid",
            detail="default_hourly_rate must be greater than 0.",
        )


def guard_client_role(target: User) -> None:
    """Refuse a role change on a Client account, which is tied to one client's data (FR-242).

    Args:
        target: The account whose role would change.

    Raises:
        ConflictError: ``users.client_role_locked`` (HTTP 409).
    """
    if target.role == UserRole.client:
        raise ConflictError(
            code="users.client_role_locked",
            detail="The role of a Client account cannot be changed.",
        )


class EmployeeService:
    """Profile access for everyone and employee administration for Managers and Admins."""

    def __init__(
        self,
        user_repo: UserRepository,
        token_repo: RefreshTokenRepository,
        link_repo: InvitationTokenRepository,
        group_repo: GroupRepository,
        qualification_repo: QualificationRepository,
        client_repo: ClientRepository,
    ) -> None:
        """Wire the service.

        Args:
            user_repo: User storage.
            token_repo: Used to end sessions when access changes.
            link_repo: Used to cancel outstanding invitation and reset links when an account is
                deactivated or deleted.
            group_repo: Used to check that a group filter belongs to the organisation.
            qualification_repo: Used to check that a qualification filter belongs to the
                organisation.
            client_repo: Used to refuse reactivating a portal user whose client is gone.
        """
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._link_repo = link_repo
        self._group_repo = group_repo
        self._qualification_repo = qualification_repo
        self._client_repo = client_repo

    # ── Self-service ─────────────────────────────────────────────────────────

    async def get_profile(self, user_id: uuid.UUID, org_id: uuid.UUID) -> User:
        """Load a user of the organisation.

        No role check: the router decides who may read which profile.

        Args:
            user_id: User to load.
            org_id: The caller's organisation.

        Raises:
            NotFoundError: Missing, deleted or in another organisation (HTTP 404).
        """
        user = await self._user_repo.get_by_id(user_id, org_id)
        if user is None:
            raise NotFoundError(resource="User", id=user_id)
        return user

    async def list_employees(
        self,
        actor: User,
        *,
        search: str | None = None,
        role: UserRole | None = None,
        is_active: bool | None = None,
        group_id: uuid.UUID | None = None,
        qualification_id: uuid.UUID | None = None,
    ) -> list[User]:
        """Search and filter the organisation's employees (FR-013; Manager or Admin).

        Args:
            actor: The caller.
            search: Substring of name or email.
            role: Keep only this role.
            is_active: Keep only active or inactive users.
            group_id: Keep only members of this group.
            qualification_id: Keep only holders of this qualification.

        Returns:
            Matching users, ordered by name.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: ``group_id`` or ``qualification_id`` is not in the organisation
                (HTTP 404), the same answer as for an id that does not exist.
        """
        ensure_role(actor, *MANAGER_ROLES)
        if group_id is not None and not await self._group_repo.get_by_id(
            group_id, actor.organization_id
        ):
            raise NotFoundError(resource="Group", id=group_id)
        if qualification_id is not None and not await self._qualification_repo.get_by_id(
            qualification_id, actor.organization_id
        ):
            raise NotFoundError(resource="Qualification", id=qualification_id)
        return await self._user_repo.search(
            actor.organization_id,
            search=search,
            role=role,
            is_active=is_active,
            group_id=group_id,
            qualification_id=qualification_id,
        )

    async def update_own_profile(self, current_user: User, data: dict[str, Any]) -> User:
        """Let any user edit only their own name and phone.

        Role, rate, email and activity are excluded so self-service cannot escalate privileges.

        Args:
            current_user: The caller, whose own profile is edited.
            data: Requested changes.

        Returns:
            The updated user.

        Raises:
            ForbiddenError: ``profile.field_not_allowed`` for any other field (HTTP 403).
        """
        disallowed = set(data.keys()) - _ALLOWED_SELF_FIELDS
        if disallowed:
            field = next(iter(disallowed))
            raise ForbiddenError(
                code="profile.field_not_allowed",
                detail=f"Field '{field}' cannot be updated by the employee.",
            )
        # Explicit whitelist: only these keys are ever passed on to the repository.
        changes = {key: data[key] for key in _ALLOWED_SELF_FIELDS if key in data}
        return await self._user_repo.update(
            current_user.id, current_user.organization_id, **changes
        )

    # ── Reads ────────────────────────────────────────────────────────────────

    async def get_employee(self, actor: User, id: uuid.UUID) -> User:
        """Return a user of the organisation; anyone but a manager may read only themselves.

        Args:
            actor: The caller.
            id: User to load.

        Returns:
            The user.

        Raises:
            ForbiddenError: ``users.forbidden`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
        """
        ensure_self_or_manager(actor, id)
        return await self._get_target(actor, id)

    async def ensure_manageable(self, actor: User, id: uuid.UUID) -> None:
        """Refuse a change to a user the actor may not change, the role before the id.

        The role comes first, so a caller without it cannot learn which ids exist.

        Args:
            actor: The caller.
            id: The user.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        await self._get_target(actor, id)

    async def list_groups_of(self, actor: User, id: uuid.UUID) -> list[Group]:
        """List a user's groups, under the same rule as `get_employee`.

        Args:
            actor: The caller.
            id: The user.

        Returns:
            The groups.

        Raises:
            ForbiddenError: ``users.forbidden`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
        """
        await self.get_employee(actor, id)
        return await self._group_repo.get_groups_for_user(id, actor.organization_id)

    async def list_qualifications_of(self, actor: User, id: uuid.UUID) -> list[Qualification]:
        """List a user's qualifications, under the same rule as `get_employee`.

        Args:
            actor: The caller.
            id: The user.

        Returns:
            The qualifications.

        Raises:
            ForbiddenError: ``users.forbidden`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
        """
        await self.get_employee(actor, id)
        return await self._qualification_repo.get_qualifications_for_user(id, actor.organization_id)

    # ── Manager operations ───────────────────────────────────────────────────

    async def create_employee(self, actor: User, data: dict[str, Any]) -> User:
        """Create an employee, Manager or Admin account with an initial password.

        Args:
            actor: The Manager or Admin creating the account. Only an Admin may create an Admin.
            data: User fields including ``initial_password`` and optionally ``role`` (default
                employee) and ``default_hourly_rate``.

        Returns:
            The new user.

        Raises:
            ForbiddenError: ``users.manager_required``, or ``users.admin_required`` when a
                Manager asks for the Admin role (HTTP 403).
            DomainValidationError: ``users.role_invalid`` (Client users have their own
                endpoints), ``users.hourly_rate_invalid`` or ``users.password_too_short``
                (HTTP 422).
            ConflictError: ``users.duplicate_email`` (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        data = dict(data)
        role = UserRole(data.get("role", UserRole.employee))
        self._check_assignable_role(actor, role)
        validate_hourly_rate(data.get("default_hourly_rate"))

        email = data.get("email", "")
        if await self._user_repo.get_by_email(email) is not None:
            raise ConflictError(
                code="users.duplicate_email",
                detail=f"Email '{email}' is already in use.",
                field="email",
            )
        initial_password = data.pop("initial_password")
        validate_initial_password(initial_password)
        await self._user_repo.release_connection()  # none held while hashing (FR-222)
        data["password_hash"] = await passwords.hash_password(initial_password)
        data["role"] = role
        # Hashing can take a while, so another request may have taken the address since the check.
        user, created = await self._user_repo.create_if_absent(actor.organization_id, data)
        if not created:
            raise ConflictError(
                code="users.duplicate_email",
                detail=f"Email '{email}' is already in use.",
                field="email",
            )
        return user

    async def edit_employee(self, actor: User, target_id: uuid.UUID, data: dict[str, Any]) -> User:
        """Update an employee's details, and optionally their role (Manager or Admin).

        Args:
            actor: The caller. Only an Admin may edit an Admin or assign the Admin role.
            target_id: User to change.
            data: Fields to change.

        Returns:
            The updated user.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``users.admin_required`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
            DomainValidationError: ``users.role_invalid`` or ``users.hourly_rate_invalid``
                (HTTP 422).
            ConflictError: ``users.last_admin`` when demoting the last active Admin, or
                ``users.client_role_locked`` for any role change on a Client account (HTTP 409).

        Side effects:
            Revokes the user's refresh tokens when the role changes.
        """
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        data = dict(data)
        validate_hourly_rate(data.get("default_hourly_rate"))

        role_changed = False
        if data.get("role") is not None:
            guard_client_role(target)
            new_role = UserRole(data["role"])
            data["role"] = new_role
            self._check_assignable_role(actor, new_role)
            if new_role != target.role:
                role_changed = True
                if target.role == UserRole.admin:
                    await self._guard_last_admin(target)
        updated = await self._user_repo.update(target_id, actor.organization_id, **data)
        if role_changed:
            await self._token_repo.revoke_all_for_user(target_id, RevocationReason.account)
        return updated

    async def deactivate(self, actor: User, target_id: uuid.UUID) -> User:
        """Block a user from signing in without deleting them (Manager or Admin).

        A *pending* account (invited, never activated) is refused: it cannot sign in, so there is
        nothing to block, and stamping it would turn "never activated" into "was activated".
        ``activated_at`` is written only by a real activation (FR-226). Only an Admin's delete
        removes a pending account.

        Args:
            actor: The caller. Only an Admin may deactivate an Admin.
            target_id: User to deactivate.

        Returns:
            The updated user.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``users.admin_required`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
            ConflictError: ``users.pending_cannot_deactivate`` when the account never accepted
                its invitation, or ``users.last_admin`` (HTTP 409). Nothing is changed.

        Side effects:
            Revokes the user's refresh tokens, ending their sessions, and cancels their
            outstanding invitation and reset links so none can bring the account back.
        """
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        if target.activated_at is None:
            raise ConflictError(
                code="users.pending_cannot_deactivate",
                detail="This account has not accepted its invitation yet, so it cannot be "
                "deactivated.",
            )
        if target.role == UserRole.admin:
            await self._guard_last_admin(target)
        updated = await self._user_repo.update(target_id, actor.organization_id, is_active=False)
        await self._token_repo.revoke_all_for_user(target_id, RevocationReason.account)
        await self._link_repo.cancel_for_user(target_id)
        return updated

    async def reactivate(self, actor: User, target_id: uuid.UUID) -> User:
        """Allow a deactivated user to sign in again (Manager or Admin).

        Only an account that was active once can be reactivated. A *pending* account (invited,
        never activated) is refused: only accepting its invitation activates it, and the database
        would reject an active account without an activation time anyway. A portal user is refused
        while their client is inactive or deleted (FR-225).

        Args:
            actor: The caller. Only an Admin may reactivate an Admin.
            target_id: User to reactivate.

        Returns:
            The updated user.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``users.admin_required`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
            ConflictError: ``users.pending_cannot_reactivate`` when the account never accepted
                its invitation, or ``users.client_inactive`` for a portal user whose client is
                inactive or deleted (HTTP 409). Nothing is changed.
        """
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        if target.activated_at is None:
            raise ConflictError(
                code="users.pending_cannot_reactivate",
                detail="This account has not accepted its invitation yet; only accepting the "
                "invitation activates it.",
            )
        if target.role == UserRole.client:
            client = (
                await self._client_repo.get_by_id(target.client_id, actor.organization_id)
                if target.client_id is not None
                else None
            )
            if client is None or not client.is_active:
                raise ConflictError(
                    code="users.client_inactive",
                    detail="This user's client is inactive or deleted; reactivate it first.",
                )
        return await self._user_repo.update(target_id, actor.organization_id, is_active=True)

    async def soft_delete(self, actor: User, target_id: uuid.UUID) -> None:
        """Delete a user, keeping their history and freeing their email (Admin only).

        Args:
            actor: The caller.
            target_id: User to delete.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The user is not in the organisation (HTTP 404).
            ConflictError: ``users.last_admin`` (HTTP 409).

        Side effects:
            Revokes the user's refresh tokens and cancels their outstanding links.
        """
        ensure_role(actor, *ADMIN_ROLES)
        target = await self._get_target(actor, target_id)
        if target.role == UserRole.admin:
            await self._guard_last_admin(target)
        await self._user_repo.soft_delete(target_id, actor.organization_id)
        await self._token_repo.revoke_all_for_user(target_id, RevocationReason.account)
        await self._link_repo.cancel_for_user(target_id)

    # ── Guards ───────────────────────────────────────────────────────────────

    async def _get_target(self, actor: User, target_id: uuid.UUID) -> User:
        target = await self._user_repo.get_by_id(target_id, actor.organization_id)
        if target is None:
            raise NotFoundError(resource="User", id=target_id)
        return target

    @staticmethod
    def _check_assignable_role(actor: User, role: UserRole) -> None:
        if role == UserRole.client:
            raise DomainValidationError(
                code="users.role_invalid",
                detail="Client users are created through the client endpoints.",
            )
        if role == UserRole.admin:
            ensure_role(actor, *ADMIN_ROLES)

    @staticmethod
    def _guard_admin_target(actor: User, target: User) -> None:
        """FR-011a: only an Admin may change an Admin."""
        if target.role == UserRole.admin:
            ensure_role(actor, *ADMIN_ROLES)

    async def _guard_last_admin(self, target: User) -> None:
        """FR-011: the last active Admin cannot be removed, deactivated or demoted.

        Safe against two Admins acting on each other at once: the Admins are locked first (see
        ``UserRepository.lock_active_admins``), so the second request counts after the first.
        """
        if not target.is_active:
            return
        # Locks the Admins until the request ends, so a simultaneous removal waits and then sees
        # the result of this one (FR-211).
        admins = await self._user_repo.lock_active_admins(target.organization_id)
        if len(admins) <= 1:
            raise ConflictError(
                code="users.last_admin",
                detail="Cannot remove, deactivate or demote the last active Admin.",
            )
