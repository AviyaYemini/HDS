"""Persistence of invitation and password-reset tokens (hashes only).

Layer: repositories. Depends on ``hds.models`` only; it MUST NOT import services or routers.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.invitation_token import InvitationToken


class InvitationTokenRepository:
    """Stores and spends single-use tokens. It does not validate expiry or purpose."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind the repository to a request-scoped session.

        Args:
            session: The request's session from ``hds.core.database.get_unit_of_work``.
        """
        self.session = session

    async def create(
        self,
        user_id: uuid.UUID,
        token_hash: str,
        purpose: str,
        expires_at: datetime,
    ) -> InvitationToken:
        """Store a new token.

        Args:
            user_id: User the token is for.
            token_hash: SHA-256 hex digest of the raw token.
            purpose: ``invite`` or ``reset``; any other value fails the database check.
            expires_at: Timezone-aware UTC expiry.

        Returns:
            The persisted token.

        Raises:
            sqlalchemy.exc.IntegrityError: When ``purpose`` is not allowed.
        """
        token = InvitationToken(
            id=uuid.uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            purpose=purpose,
            expires_at=expires_at,
            created_at=datetime.now(UTC),
        )
        self.session.add(token)
        await self.session.flush()
        await self.session.refresh(token)
        return token

    async def get_by_hash(self, token_hash: str) -> InvitationToken | None:
        """Find a token by hash, whether or not it is used or expired.

        Args:
            token_hash: SHA-256 hex digest of the presented token.

        Returns:
            The token row, or ``None`` if unknown.
        """
        result = await self.session.execute(
            select(InvitationToken).where(InvitationToken.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    async def mark_used(self, id: uuid.UUID) -> None:
        """Spend a token by stamping ``used_at`` with the current UTC time.

        Args:
            id: Primary key of the token.
        """
        await self.session.execute(
            update(InvitationToken)
            .where(InvitationToken.id == id)
            .values(used_at=datetime.now(UTC))
        )
        await self.session.flush()
