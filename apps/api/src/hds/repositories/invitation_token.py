"""Persistence of invitation and password-reset tokens (hashes only).

Layer: repositories. Depends on ``hds.models`` only; it MUST NOT import services or routers.
"""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import delete, select, update
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

    async def cancel_for_user(self, user_id: uuid.UUID) -> None:
        """Cancel the outstanding links of one account.

        Cancelling marks the link used, so it can never be redeemed. It is done when an account is
        deactivated, deleted, activated, re-invited or has completed a password reset, so a link
        issued for an earlier state of the account cannot outlive it (spec FR-207, FR-224).

        Args:
            user_id: The account whose unused links are cancelled.
        """
        await self.cancel_for_users([user_id])

    async def cancel_for_users(self, user_ids: Sequence[uuid.UUID]) -> None:
        """Cancel the outstanding links of several accounts in one statement.

        Args:
            user_ids: The accounts whose unused links are cancelled; an empty sequence is a no-op.
        """
        if not user_ids:
            return
        await self.session.execute(
            update(InvitationToken)
            .where(InvitationToken.user_id.in_(user_ids), InvitationToken.used_at.is_(None))
            .values(used_at=datetime.now(UTC))
        )
        await self.session.flush()

    async def spend(self, id: uuid.UUID) -> bool:
        """Spend a link only if it is still unused, and say whether this call did.

        This is the single-use guarantee for invitation and reset links. It is one statement,
        ``UPDATE ... SET used_at = now WHERE id = :id AND used_at IS NULL``, so when two requests
        redeem the same link at the same moment exactly one changes the row; the other waits,
        finds the condition false and changes nothing. Cancelling a link (see `cancel_for_users`)
        uses the same column, so a cancelled link cannot be spent.

        Args:
            id: Primary key of the link.

        Returns:
            ``True`` if this call spent the link, ``False`` if it was already used or cancelled.
        """
        result = await self.session.execute(
            update(InvitationToken)
            .where(InvitationToken.id == id, InvitationToken.used_at.is_(None))
            .values(used_at=datetime.now(UTC))
            .returning(InvitationToken.id)
        )
        await self.session.flush()
        return result.scalar_one_or_none() is not None

    async def delete_expired(self, now: datetime, limit: int) -> int:
        """Delete up to ``limit`` links past their expiry, of any user.

        Only the expiry decides, as in ``delete_expired_for_user``. Rows another request is using
        are skipped, so a bulk clean never waits for one.

        Args:
            now: Timezone-aware UTC current time.
            limit: The most rows to delete in one call.

        Returns:
            How many rows were deleted.
        """
        expired = (
            select(InvitationToken.id)
            .where(InvitationToken.expires_at <= now)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self.session.execute(
            delete(InvitationToken).where(InvitationToken.id.in_(expired))
        )
        return int(result.rowcount)  # type: ignore[attr-defined]

    async def delete_expired_for_user(self, user_id: uuid.UUID, now: datetime) -> None:
        """Delete this user's links past their expiry.

        Only the expiry decides. A spent or cancelled link inside its lifetime is kept, because
        cancelling marks a link used and its answer must stay ``auth.token_used``.

        Args:
            user_id: The user whose expired records are removed.
            now: Timezone-aware UTC current time.
        """
        await self.session.execute(
            delete(InvitationToken).where(
                InvitationToken.user_id == user_id, InvitationToken.expires_at <= now
            )
        )
        await self.session.flush()
