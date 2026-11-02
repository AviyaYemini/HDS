"""Employee service: self-service profile and manager operations."""

import uuid
from decimal import Decimal
from typing import Any

from hds.core.authz import ADMIN_ROLES, MANAGER_ROLES, ensure_role
from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
)
from hds.core.security import hash_password
from hds.models.user import User, UserRole
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository

# Fields that an employee is permitted to edit on their own profile
_ALLOWED_SELF_FIELDS = frozenset({"full_name", "phone"})

MIN_PASSWORD_LENGTH = 12


def validate_initial_password(password: str) -> None:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise DomainValidationError(
            code="users.password_too_short",
            detail=f"Password must be at least {MIN_PASSWORD_LENGTH} characters.",
        )


def validate_hourly_rate(rate: Any) -> None:
    if rate is not None and Decimal(str(rate)) <= Decimal("0"):
        raise DomainValidationError(
            code="users.hourly_rate_invalid",
            detail="default_hourly_rate must be greater than 0.",
        )


class EmployeeService:
    def __init__(self, user_repo: UserRepository, token_repo: RefreshTokenRepository) -> None:
        self._user_repo = user_repo
        self._token_repo = token_repo

    # ── Self-service ─────────────────────────────────────────────────────────

    async def get_profile(self, user_id: uuid.UUID, org_id: uuid.UUID) -> User:
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
        """FR-013: Managers search and filter their own organisation's employees."""
        ensure_role(actor, *MANAGER_ROLES)
        return await self._user_repo.search(
            actor.organization_id,
            search=search,
            role=role,
            is_active=is_active,
            group_id=group_id,
            qualification_id=qualification_id,
        )

    async def update_own_profile(self, current_user: User, data: dict[str, Any]) -> User:
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

    # ── Manager operations ───────────────────────────────────────────────────

    async def create_employee(self, actor: User, data: dict[str, Any]) -> User:
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
        data["password_hash"] = hash_password(initial_password)
        data["role"] = role
        return await self._user_repo.create(actor.organization_id, data)

    async def promote_to_manager(self, actor: User, target_id: uuid.UUID) -> User:
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        if target.role == UserRole.admin:
            await self._guard_last_admin(target)
        updated = await self._user_repo.update(
            target_id, actor.organization_id, role=UserRole.manager
        )
        await self._token_repo.revoke_all_for_user(target_id)
        return updated

    async def promote_to_admin(self, actor: User, target_id: uuid.UUID) -> User:
        ensure_role(actor, *ADMIN_ROLES)
        await self._get_target(actor, target_id)
        updated = await self._user_repo.update(
            target_id, actor.organization_id, role=UserRole.admin
        )
        await self._token_repo.revoke_all_for_user(target_id)
        return updated

    async def edit_employee(self, actor: User, target_id: uuid.UUID, data: dict[str, Any]) -> User:
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        data = dict(data)
        validate_hourly_rate(data.get("default_hourly_rate"))

        role_changed = False
        if data.get("role") is not None:
            new_role = UserRole(data["role"])
            data["role"] = new_role
            self._check_assignable_role(actor, new_role)
            if new_role != target.role:
                role_changed = True
                if target.role == UserRole.admin:
                    await self._guard_last_admin(target)
        updated = await self._user_repo.update(target_id, actor.organization_id, **data)
        if role_changed:
            await self._token_repo.revoke_all_for_user(target_id)
        return updated

    async def deactivate(self, actor: User, target_id: uuid.UUID) -> User:
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        if target.role == UserRole.admin:
            await self._guard_last_admin(target)
        updated = await self._user_repo.update(target_id, actor.organization_id, is_active=False)
        await self._token_repo.revoke_all_for_user(target_id)
        return updated

    async def reactivate(self, actor: User, target_id: uuid.UUID) -> User:
        ensure_role(actor, *MANAGER_ROLES)
        target = await self._get_target(actor, target_id)
        self._guard_admin_target(actor, target)
        return await self._user_repo.update(target_id, actor.organization_id, is_active=True)

    async def soft_delete(self, actor: User, target_id: uuid.UUID) -> None:
        ensure_role(actor, *ADMIN_ROLES)
        target = await self._get_target(actor, target_id)
        if target.role == UserRole.admin:
            await self._guard_last_admin(target)
        await self._user_repo.soft_delete(target_id, actor.organization_id)
        await self._token_repo.revoke_all_for_user(target_id)

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
        """FR-011: the last active Admin cannot be removed, deactivated or demoted."""
        if not target.is_active:
            return
        count = await self._user_repo.count_active_admins(target.organization_id)
        if count <= 1:
            raise ConflictError(
                code="users.last_admin",
                detail="Cannot remove, deactivate or demote the last active Admin.",
            )
