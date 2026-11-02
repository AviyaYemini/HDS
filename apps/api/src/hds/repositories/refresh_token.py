"""Persistence of refresh tokens (hashes only).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only. Tokens are
not tenant-scoped rows; they belong to a user, and the user lookup does the scoping.
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update

from hds.models.refresh_token import RefreshToken
from hds.repositories.base import BaseRepository


class RefreshTokenRepository(BaseRepository[RefreshToken]):
    """Stores, finds and revokes refresh tokens. Raw token values never reach this class."""

    model = RefreshToken

    async def create(
        self, user_id: uuid.UUID, token_hash: str, expires_at: datetime
    ) -> RefreshToken:
        """Store a new token and commit.

        Args:
            user_id: Owner of the token.
            token_hash: SHA-256 hex digest of the raw token (the raw value is never stored).
            expires_at: Timezone-aware UTC expiry.

        Returns:
            The persisted token.
        """
        token = RefreshToken(
            id=uuid.uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at,
        )
        self.session.add(token)
        await self.session.commit()
        await self.session.refresh(token)
        return token

    async def get_by_hash(self, token_hash: str) -> RefreshToken | None:
        """Find a token by its hash, including revoked and expired ones.

        The caller checks ``revoked_at`` and ``expires_at`` so it can report which failed.

        Args:
            token_hash: SHA-256 hex digest of the presented token.

        Returns:
            The token row, or ``None`` if the hash is unknown.
        """
        result = await self.session.execute(
            select(RefreshToken).where(RefreshToken.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    async def revoke(self, token_id: uuid.UUID) -> None:
        """Stamp one token as revoked at the current UTC time and commit.

        Re-revoking overwrites the timestamp; callers check ``revoked_at`` first.

        Args:
            token_id: Primary key of the token.
        """
        await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.id == token_id)
            .values(revoked_at=datetime.now(UTC))
        )
        await self.session.commit()

    async def revoke_all_for_user(self, user_id: uuid.UUID) -> None:
        """Revoke every still-valid token of a user, ending all their sessions. Commits.

        Used by sign-out-everywhere and by password reset (NFR-005). Already-revoked tokens keep
        their original timestamp.

        Args:
            user_id: The user whose sessions end.
        """
        await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC))
        )
        await self.session.commit()
