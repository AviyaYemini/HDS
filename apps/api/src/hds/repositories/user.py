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
    model = User

    async def get_by_email(self, email: str) -> User | None:
        """Email is unique per deployment (FR-012); the organisation comes from the account."""
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
        """Non-deleted users of one organisation; every given filter must match (AND)."""
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
        """The only lookup not scoped by organisation: redeeming an invite/reset token."""
        result = await self.session.execute(
            select(User).where(User.id == user_id, User.is_deleted.is_(False))
        )
        return result.scalar_one_or_none()

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> User | None:
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

        The ONLY unscoped user lookup: the organisation is not known until the credential's
        owner is loaded. Never call this with an id taken from a request path or body.
        """
        result = await self.session.execute(
            select(User).where(User.id == id, User.is_deleted.is_(False))
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> User:
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
        await self.session.execute(
            update(User)
            .where(User.id == user_id)
            .values(failed_login_attempts=User.failed_login_attempts + 1)
        )
        await self.session.commit()

    async def reset_failed_attempts(self, user_id: uuid.UUID) -> None:
        await self.session.execute(
            update(User)
            .where(User.id == user_id)
            .values(failed_login_attempts=0, locked_until=None)
        )
        await self.session.commit()

    async def set_locked_until(self, user_id: uuid.UUID, until: datetime) -> None:
        await self.session.execute(
            update(User).where(User.id == user_id).values(locked_until=until)
        )
        await self.session.commit()

    async def update(self, user_id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> User:
        return await self._update_scoped(user_id, org_id, **fields)

    async def soft_delete(self, user_id: uuid.UUID, org_id: uuid.UUID) -> None:
        await self._soft_delete_scoped(user_id, org_id)

    async def count_active_admins(self, org_id: uuid.UUID) -> int:
        result = await self.session.execute(
            select(func.count()).where(
                User.organization_id == org_id,
                User.role == UserRole.admin,
                User.is_active.is_(True),
                User.is_deleted.is_(False),
            )
        )
        return result.scalar_one()
