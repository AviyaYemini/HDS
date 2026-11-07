"""Persistence of refresh tokens (hashes only).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only. Tokens are
not tenant-scoped rows; they belong to a user, and the user lookup does the scoping.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Select, any_, delete, func, literal_column, select, update

from hds.models.refresh_token import RefreshToken, RevocationReason
from hds.repositories.base import BaseRepository
from hds.repositories.user import lock_user_row


class RefreshTokenRepository(BaseRepository[RefreshToken]):
    """Stores, finds and revokes refresh tokens. Raw token values never reach this class."""

    model = RefreshToken

    async def create(
        self,
        user_id: uuid.UUID,
        token_hash: str,
        expires_at: datetime,
        client_kind: str = "web",
    ) -> RefreshToken:
        """Store a new token.

        Args:
            user_id: Owner of the token.
            token_hash: SHA-256 hex digest of the raw token (the raw value is never stored).
            expires_at: Timezone-aware UTC expiry.
            client_kind: ``web`` or ``native``, the lifetime class (007 FR-010).

        Returns:
            The persisted token.
        """
        token = RefreshToken(
            id=uuid.uuid4(),
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at,
            client_kind=client_kind,
        )
        self.session.add(token)
        await self.session.flush()
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

    async def revoke_if_live(self, token_id: uuid.UUID, reason: RevocationReason) -> bool:
        """Revoke one token only if nobody has revoked it yet, and say whether this call did.

        This is the single-use guarantee. It is one statement, ``UPDATE ... WHERE id = :id AND
        revoked_at IS NULL``, so when two requests present the same token at the same moment the
        database lets exactly one of them change the row; the other waits for the first to
        commit, then finds the condition false and changes nothing. Checking first and writing
        afterwards cannot do this, because both requests pass the check.

        Args:
            token_id: Primary key of the token.
            reason: Why it is being revoked (rotation, sign-out, ...).

        Returns:
            ``True`` if this call revoked the token, ``False`` if it was already revoked.
        """
        result = await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.id == token_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC), revoked_reason=RevocationReason(reason).value)
            .returning(RefreshToken.id)
        )
        await self.session.flush()
        return result.scalar_one_or_none() is not None

    async def live_for_user(self, user_id: uuid.UUID, now: datetime) -> list[RefreshToken]:
        """The user's tokens that still work, newest first: one per signed-in device.

        Rotation revokes a token when it issues the next, so each device's chain has one live
        token, whose ``created_at`` is that device's last refresh (007 N12).

        Args:
            user_id: The user whose sessions are listed.
            now: The moment to judge expiry against.
        """
        result = await self.session.execute(
            select(RefreshToken)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.revoked_at.is_(None),
                RefreshToken.expires_at > now,
            )
            .order_by(RefreshToken.created_at.desc(), RefreshToken.id)
        )
        return list(result.scalars())

    async def revoke_for_user(
        self, token_id: uuid.UUID, user_id: uuid.UUID, reason: RevocationReason
    ) -> bool:
        """Revoke one live token, only if it belongs to ``user_id``; say whether it did.

        One conditional statement, like :meth:`revoke_if_live`, so a token of another user, an
        unknown id and an already revoked token all leave nothing changed and answer ``False``.
        """
        result = await self.session.execute(
            update(RefreshToken)
            .where(
                RefreshToken.id == token_id,
                RefreshToken.user_id == user_id,
                RefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=datetime.now(UTC), revoked_reason=RevocationReason(reason).value)
            .returning(RefreshToken.id)
        )
        await self.session.flush()
        return result.scalar_one_or_none() is not None

    async def revoke_all_for_user(self, user_id: uuid.UUID, reason: RevocationReason) -> None:
        """Revoke every still-valid token of a user, ending all their sessions.

        Used by sign-out-everywhere, by password reset (NFR-005), by account changes and by the
        response to a replayed token. Already-revoked tokens keep their original time and reason.

        Args:
            user_id: The user whose sessions end.
            reason: Why: ``all`` for a deliberate or defensive end of every session,
                ``account`` when the account itself changed.
        """
        await lock_user_row(self.session, user_id)
        await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=datetime.now(UTC), revoked_reason=RevocationReason(reason).value)
        )
        await self.session.flush()

    async def delete_expired(self, now: datetime, limit: int) -> int:
        """Delete up to ``limit`` refresh tokens past their expiry, of any user.

        Rows another request is using are skipped, so a bulk clean never waits for one.

        Args:
            now: Timezone-aware UTC current time.
            limit: The most rows to delete in one call.

        Returns:
            How many rows were deleted.
        """
        # The batch's physical row addresses, found through the expiry index and locked, then
        # deleted by address (a TID scan). Matching ids, or repeating the expiry on the delete,
        # let the planner read the whole table or the whole expired backlog for each batch
        # (003 FR-008). A locked row keeps its address until this transaction ends; a row an
        # update moved while the batch ran is skipped, and the next batch takes it.
        expired: Select[tuple[Any]] = (
            select(literal_column("ctid"))
            .select_from(RefreshToken)
            .where(RefreshToken.expires_at <= now)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        batch = func.array(expired.scalar_subquery())
        result = await self.session.execute(
            delete(RefreshToken).where(literal_column("ctid") == any_(batch))
        )
        return int(result.rowcount)  # type: ignore[attr-defined]

    async def delete_expired_for_user(self, user_id: uuid.UUID, now: datetime) -> None:
        """Delete this user's refresh tokens past their expiry.

        A token past its expiry can never be used again, whatever its state, so its row has no
        further purpose. Once deleted it is answered as unknown (``auth.invalid_token``), not as
        expired.

        Args:
            user_id: The user whose expired records are removed.
            now: Timezone-aware UTC current time.
        """
        await self.session.execute(
            delete(RefreshToken).where(
                RefreshToken.user_id == user_id, RefreshToken.expires_at <= now
            )
        )
        await self.session.flush()
