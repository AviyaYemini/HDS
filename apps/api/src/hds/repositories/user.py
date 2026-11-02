"""Persistence of users, including the lockout counters used at sign-in.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. Most queries are tenant-scoped; the two unscoped lookups are
named and documented below because they exist for flows where the organisation is not yet
known.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select, update

from hds.models.group import user_groups
from hds.models.qualification import user_qualifications
from hds.models.user import User, UserRole
from hds.repositories.base import OrgScopedRepository


def _escape_like(text: str) -> str:
    """Make ``%``, ``_`` and the escape character literal inside a LIKE pattern."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class UserRepository(OrgScopedRepository[User]):
    """Queries and writes for ``User``. Soft-deleted users are invisible to every lookup."""

    model = User

    async def get_by_email(self, email: str) -> User | None:
        """Find a non-deleted user by exact email, across all organisations.

        Email is unique per deployment (FR-012), so this is safe for sign-in, where the
        organisation is learned from the account rather than supplied by the caller.

        Args:
            email: Address exactly as stored; the match is case-sensitive.

        Returns:
            The user, or ``None``.
        """
        result = await self.session.execute(
            select(User).where(User.email == email, User.is_deleted.is_(False)).limit(1)
        )
        return result.scalar_one_or_none()

    async def search(
        self,
        org_id: uuid.UUID,
        *,
        search: str | None = None,
        role: UserRole | None = None,
        is_active: bool | None = None,
        group_id: uuid.UUID | None = None,
        qualification_id: uuid.UUID | None = None,
    ) -> list[User]:
        """List non-deleted users of one organisation, ordered by name then id.

        Args:
            org_id: The caller's organisation.
            search: Case-insensitive substring of full name or email; ``%`` and ``_`` are
                treated literally.
            role: Keep only this role.
            is_active: Keep only active or only inactive users.
            group_id: Keep only members of this group.
            qualification_id: Keep only holders of this qualification.

        Returns:
            Users matching all given filters (AND).
        """
        stmt = select(User).where(User.organization_id == org_id, User.is_deleted.is_(False))
        if search:
            needle = "%" + _escape_like(search) + "%"
            stmt = stmt.where(
                or_(
                    User.full_name.ilike(needle, escape="\\"),
                    User.email.ilike(needle, escape="\\"),
                )
            )
        if role is not None:
            stmt = stmt.where(User.role == role)
        if is_active is not None:
            stmt = stmt.where(User.is_active.is_(is_active))
        if group_id is not None:
            stmt = stmt.where(
                User.id.in_(select(user_groups.c.user_id).where(user_groups.c.group_id == group_id))
            )
        if qualification_id is not None:
            stmt = stmt.where(
                User.id.in_(
                    select(user_qualifications.c.user_id).where(
                        user_qualifications.c.qualification_id == qualification_id
                    )
                )
            )
        result = await self.session.execute(stmt.order_by(User.full_name, User.id))
        return list(result.scalars().all())

    async def get_by_id_for_token(self, user_id: uuid.UUID) -> User | None:
        """Load the user an invite or reset token belongs to.

        Unscoped by organisation because the redeemer is not signed in; the token itself is the
        proof of ownership. Never call it with an id taken from a request path or body.

        Args:
            user_id: The ``user_id`` stored on the validated token.

        Returns:
            The non-deleted user, or ``None``.
        """
        result = await self.session.execute(
            select(User).where(User.id == user_id, User.is_deleted.is_(False))
        )
        return result.scalar_one_or_none()

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> User | None:
        """Fetch a non-deleted user of the organisation, or ``None`` (also for other tenants)."""
        result = await self.session.execute(
            select(User).where(
                User.id == id,
                User.organization_id == org_id,
                User.is_deleted.is_(False),
            )
        )
        return result.scalar_one_or_none()

    async def get_for_auth(self, id: uuid.UUID) -> User | None:
        """Resolve the owner of a verified credential (access/refresh token).

        Unscoped by organisation because the organisation is not known until the credential's
        owner is loaded. Never call this with an id taken from a request path or body.

        Args:
            id: The id taken from a verified token's subject.

        Returns:
            The non-deleted user, or ``None``. Inactive users are returned; the caller decides.
        """
        result = await self.session.execute(
            select(User).where(User.id == id, User.is_deleted.is_(False))
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> User:
        """Insert a user in the organisation and commit.

        Args:
            org_id: Organisation that will own the user.
            data: Column values; the password must already be hashed.

        Returns:
            The persisted user.

        Raises:
            sqlalchemy.exc.IntegrityError: On a duplicate email or a violated check
                constraint. The error handler maps it to HTTP 422.
        """
        user = User(
            id=uuid.uuid4(),
            organization_id=org_id,
            **data,
        )
        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def increment_failed_attempts(self, user_id: uuid.UUID) -> None:
        """Add one to the failed-sign-in counter in a single atomic UPDATE, then commit.

        Done in SQL rather than read-modify-write so concurrent failures are not lost.

        Args:
            user_id: The account that failed to sign in.
        """
        await self.session.execute(
            update(User)
            .where(User.id == user_id)
            .values(failed_login_attempts=User.failed_login_attempts + 1)
        )
        await self.session.commit()

    async def reset_failed_attempts(self, user_id: uuid.UUID) -> None:
        """Zero the failure counter and clear any lock, then commit (after a good sign-in).

        Args:
            user_id: The account that signed in.
        """
        await self.session.execute(
            update(User)
            .where(User.id == user_id)
            .values(failed_login_attempts=0, locked_until=None)
        )
        await self.session.commit()

    async def set_locked_until(self, user_id: uuid.UUID, until: datetime) -> None:
        """Lock the account until a UTC instant, then commit.

        Args:
            user_id: The account to lock.
            until: Timezone-aware UTC time at which sign-in is allowed again.
        """
        await self.session.execute(
            update(User).where(User.id == user_id).values(locked_until=until)
        )
        await self.session.commit()

    async def update(self, user_id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> User:
        """Update columns of a user of the organisation and commit.

        Args:
            user_id: User to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed user.

        Raises:
            NotFoundError: When the user is not in the organisation.
        """
        return await self._update_scoped(user_id, org_id, **fields)

    async def soft_delete(self, user_id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Mark a user deleted so the email can be reused; history is kept. Commits.

        Args:
            user_id: User to delete.
            org_id: The caller's organisation.
        """
        await self._soft_delete_scoped(user_id, org_id)

    async def count_active_admins(self, org_id: uuid.UUID) -> int:
        """Count active, non-deleted admins; used to refuse removing the last one.

        Args:
            org_id: The organisation to count in.

        Returns:
            The number of admins who can still sign in.
        """
        result = await self.session.execute(
            select(func.count()).where(
                User.organization_id == org_id,
                User.role == UserRole.admin,
                User.is_active.is_(True),
                User.is_deleted.is_(False),
            )
        )
        return result.scalar_one()
