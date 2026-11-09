"""Persistence of users, including the lockout counters used at sign-in.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. Most queries are tenant-scoped; the unscoped lookups are
named and documented below because they exist for flows where the organisation is not yet
known (sign-in, and the first Admin of an empty database).
"""

import uuid
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import exists, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.group import user_groups
from hds.models.qualification import user_qualifications
from hds.models.user import User, UserRole
from hds.repositories.base import OrgScopedRepository


def _escape_like(text: str) -> str:
    """Make ``%``, ``_`` and the escape character literal inside a LIKE pattern."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def lock_user_row(session: AsyncSession, user_id: uuid.UUID) -> None:
    """Take ``SELECT ... FOR UPDATE`` on a user's row, held until the unit of work ends.

    Every path that creates a session or ends all of them takes this lock, so they run one after
    the other. Without it, a new refresh token is invisible to a concurrent "end every session"
    until it commits, and it would survive (FR-230).

    The user's row is always the first lock of a path that also touches that user's links or
    tokens. Otherwise one path holding a link and waiting for the user meets another holding the
    user and waiting for the link, and PostgreSQL aborts one with a deadlock (FR-246). Inserting a
    link or token waits here too, through its foreign key. Several users are locked one at a time
    in ascending id order.

    Args:
        session: The request's session.
        user_id: The user whose sessions are being created or ended.
    """
    await session.execute(select(User.id).where(User.id == user_id).with_for_update())


class UserRepository(OrgScopedRepository[User]):
    """Queries and writes for ``User``. Soft-deleted users are invisible to every lookup."""

    model = User

    async def get_by_email(self, email: str) -> User | None:
        """Find a non-deleted user by exact email, across all organisations.

        Email is unique per deployment (FR-012), so this is safe for sign-in, where the
        organisation is learned from the account rather than supplied by the caller.

        Args:
            email: The address in any letter case; it is trimmed and compared in lowercase
                (FR-215), through the same expression as the unique index.

        Returns:
            The user, or ``None``.
        """
        result = await self.session.execute(
            select(User)
            .where(func.lower(User.email) == email.strip().lower(), User.is_deleted.is_(False))
            .limit(1)
            .execution_options(populate_existing=True)
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
        stmt = select(User).where(User.organisation_id == org_id, User.is_deleted.is_(False))
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

    async def any_exists(self) -> bool:
        """Whether the database holds any user at all, in any organisation, deleted or not.

        Unscoped on purpose: the first Admin is created only in an empty database (specs/006 R13).
        """
        result = await self.session.execute(select(exists().select_from(User)))
        return bool(result.scalar_one())

    async def lock_first_admin(self) -> None:
        """Hold, until the transaction ends, the lock two first-Admin runs would both take.

        Without it, two runs at once could each find no user and each create an Admin.
        """
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext('hds.first_admin'))")
        )

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> User | None:
        """Fetch a non-deleted user of the organisation, or ``None`` (also for other tenants)."""
        result = await self.session.execute(
            select(User).where(
                User.id == id,
                User.organisation_id == org_id,
                User.is_deleted.is_(False),
            )
        )
        return result.scalar_one_or_none()

    async def get_many(self, ids: Collection[uuid.UUID], org_id: uuid.UUID) -> list[User]:
        """The organisation's users among ``ids``, deleted and inactive ones included.

        For judging records that still point at them, such as a deleted employee's assignments.

        Args:
            ids: User ids.
            org_id: The caller's organisation.
        """
        if not ids:
            return []
        result = await self.session.execute(
            select(User).where(User.id.in_(ids), User.organisation_id == org_id)
        )
        return list(result.scalars().all())

    async def staff_of(self, org_id: uuid.UUID) -> list[User]:
        """The organisation's active, undeleted Employees, Managers and Admins, by name.

        The people who submit availability, for the submission tracker (007 FR-040).
        """
        result = await self.session.execute(
            select(User)
            .where(
                User.organisation_id == org_id,
                User.is_active.is_(True),
                User.is_deleted.is_(False),
                User.role.in_([UserRole.employee, UserRole.manager, UserRole.admin]),
            )
            .order_by(User.full_name, User.id)
        )
        return list(result.scalars())

    async def lock_user(self, user_id: uuid.UUID) -> None:
        """Lock the user's row until the unit of work ends (see `lock_user_row`).

        Args:
            user_id: The user whose sessions are being created or ended.
        """
        await lock_user_row(self.session, user_id)

    async def lock_staff(self, org_id: uuid.UUID) -> None:
        """Lock every active, not deleted, non-Client account of the organisation, in id order.

        Accepting a schedule proposal holds these while it checks the proposal is current and
        writes it (004 research G9): availability changes, assignments and deactivations each lock
        their user first, so none can slip in between. Ascending id order, as ``lock_user_row``
        asks of anything that locks several users.

        ``FOR NO KEY UPDATE``, not ``FOR UPDATE``: it still waits for and blocks those paths'
        ``FOR UPDATE``, but not the key-share lock a foreign key takes. An assignment with an
        ``Idempotency-Key`` inserts its key (key-sharing the Admin's row) before it locks its
        shift, while accepting holds the shifts first; a full lock here made the two deadlock.
        """
        await self.session.execute(
            select(User.id)
            .where(
                User.organisation_id == org_id,
                User.is_active.is_(True),
                User.is_deleted.is_(False),
                User.role != UserRole.client,
            )
            .order_by(User.id)
            .with_for_update(key_share=True)
        )

    async def get_for_auth(self, id: uuid.UUID) -> User | None:
        """Resolve the owner of a verified credential: a token, or a one-time link.

        Unscoped by organisation because the organisation is not known until the credential's
        owner is loaded. Never call this with an id taken from a request path or body. The row is
        always read fresh (``populate_existing``): the session keeps the values it loaded earlier,
        so a re-read after taking the lock would otherwise return the state from before the lock.

        Args:
            id: The id taken from a verified token's subject, or from a stored one-time link.

        Returns:
            The non-deleted user, or ``None``. Inactive users are returned; the caller decides.
        """
        result = await self.session.execute(
            select(User)
            .where(User.id == id, User.is_deleted.is_(False))
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> User:
        """Insert a user in the organisation.

        An account created active (the default) is stamped with ``activated_at`` = now, because
        the database refuses an active account without one. An account created inactive, as an
        invitation does, keeps ``activated_at`` empty: it is pending until it is activated.

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
            organisation_id=org_id,
            **self._with_activation(data),
        )
        self.session.add(user)
        await self.session.flush()
        await self.session.refresh(user)
        return user

    async def create_if_absent(self, org_id: uuid.UUID, data: dict[str, Any]) -> tuple[User, bool]:
        """Insert a user, or return the account that already holds this email address.

        The insert runs inside a savepoint. If another request created the same address first,
        the unique email index rejects this insert, only the savepoint is rolled back (the rest of
        the request's transaction is untouched), and the winner's account is returned instead.
        This is what lets two simultaneous invitations for a new address both succeed.

        Args:
            org_id: Organisation that will own a new user.
            data: Column values for the new user, including ``email``.

        Returns:
            ``(user, True)`` for a newly created account, ``(user, False)`` for the account that
            already existed. The caller must still check which organisation it belongs to.

        Raises:
            sqlalchemy.exc.IntegrityError: If the insert failed for a reason other than a
                duplicate email address (for example a check constraint).
        """
        user = User(
            id=uuid.uuid4(),
            organisation_id=org_id,
            **self._with_activation(data),
        )
        try:
            async with self.session.begin_nested():
                self.session.add(user)
                await self.session.flush()
        except IntegrityError:
            existing = await self.get_by_email(data["email"])
            if existing is None:
                raise
            return existing, False
        await self.session.refresh(user)
        return user, True

    @staticmethod
    def _with_activation(data: dict[str, Any]) -> dict[str, Any]:
        """Prepare the column values of a new account.

        Stamps ``activated_at`` on an account that is created active, as the database requires,
        and stores the address trimmed and lowercased (FR-215), so a caller that bypasses the
        request types still cannot create a second spelling of an address.
        """
        fields = dict(data)
        if isinstance(fields.get("email"), str):
            fields["email"] = fields["email"].strip().lower()
        if fields.get("is_active", True) and fields.get("activated_at") is None:
            fields["activated_at"] = datetime.now(UTC)
        return fields

    async def update(self, user_id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> User:
        """Update columns of a user of the organisation.

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
        """Mark a user deleted so the email can be reused; history is kept.

        Args:
            user_id: User to delete.
            org_id: The caller's organisation.
        """
        await self._soft_delete_scoped(user_id, org_id)

    async def lock_active_admins(self, org_id: uuid.UUID) -> list[uuid.UUID]:
        """Lock the organisation's active Admins and return their ids, to guard the last one.

        ``SELECT ... FOR UPDATE`` makes a second request that wants the same rows wait until the
        first one commits or rolls back, and then look again. Two Admins removing each other at
        the same moment therefore run one after the other, and the second sees only one Admin
        left. The rows are ordered by id so two requests always lock them in the same order and
        cannot wait on each other.

        The lock lasts until the request's unit of work ends (commit or rollback), not until this
        method returns, so call it just before the change it protects.

        Args:
            org_id: The organisation to look in.

        Returns:
            The ids of the Admins who can still sign in, as they are after any concurrent
            change has committed.
        """
        result = await self.session.execute(
            select(User.id)
            .where(
                User.organisation_id == org_id,
                User.role == UserRole.admin,
                User.is_active.is_(True),
                User.is_deleted.is_(False),
            )
            .order_by(User.id)
            .with_for_update()
        )
        return list(result.scalars().all())
