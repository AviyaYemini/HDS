import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update

from hds.models.user import User, UserRole
from hds.repositories.base import OrgScopedRepository


class UserRepository(OrgScopedRepository[User]):
    model = User

    async def get_by_email(self, email: str) -> User | None:
        """Email is unique per deployment (FR-012); the organisation comes from the account."""
        result = await self.session.execute(
            select(User).where(User.email == email, User.is_deleted.is_(False)).limit(1)
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

    async def update(self, user_id: uuid.UUID, **fields: Any) -> User:
        await self.session.execute(update(User).where(User.id == user_id).values(**fields))
        await self.session.commit()
        user = await self._get_unscoped(user_id)
        assert user is not None
        return user

    async def soft_delete(self, user_id: uuid.UUID) -> None:
        await self.session.execute(
            update(User)
            .where(User.id == user_id)
            .values(is_deleted=True, deleted_at=datetime.now(UTC))
        )
        await self.session.commit()

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
