"""Stored answers of retried writes (003 FR-115a).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Select, any_, delete, func, literal_column, select, update
from sqlalchemy.dialects.postgresql import insert

from hds.models.idempotency_key import IdempotencyKey
from hds.repositories.base import BaseRepository


class IdempotencyKeyRepository(BaseRepository[IdempotencyKey]):
    """Per-caller keys and the answers their first requests got."""

    model = IdempotencyKey

    async def reserve(
        self, user_id: uuid.UUID, key: str, request_hash: str, expired_before: datetime
    ) -> IdempotencyKey | None:
        """Claim ``key`` for a request, or return the claim another request already holds.

        A key older than ``expired_before`` is removed first, so it counts as new. The claim is an
        insert on the primary key: a second request with the same key waits until the first ends,
        then finds its row, so exactly one of them does the work.

        Args:
            user_id: The caller.
            key: The ``Idempotency-Key`` header.
            request_hash: SHA-256 hex digest of the method, path and body.
            expired_before: Keys created before this instant no longer count.

        Returns:
            ``None`` when this request now holds the key; otherwise the stored claim.
        """
        await self.session.execute(
            delete(IdempotencyKey).where(
                IdempotencyKey.user_id == user_id,
                IdempotencyKey.key == key,
                IdempotencyKey.created_at < expired_before,
            )
        )
        claimed = await self.session.execute(
            insert(IdempotencyKey)
            .values(
                user_id=user_id,
                key=key,
                request_hash=request_hash,
                status_code=0,
                response_body={},
            )
            .on_conflict_do_nothing(index_elements=["user_id", "key"])
            .returning(IdempotencyKey.key)
        )
        if claimed.first() is not None:
            return None
        result = await self.session.execute(
            select(IdempotencyKey)
            .where(IdempotencyKey.user_id == user_id, IdempotencyKey.key == key)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one()

    async def store(self, user_id: uuid.UUID, key: str, status_code: int, body: Any) -> None:
        """Record the answer of the request that holds ``key``."""
        await self.session.execute(
            update(IdempotencyKey)
            .where(IdempotencyKey.user_id == user_id, IdempotencyKey.key == key)
            .values(status_code=status_code, response_body=body)
        )
        await self.session.flush()

    async def delete_expired(self, before: datetime, limit: int) -> int:
        """Delete up to ``limit`` keys created before ``before``, skipping rows in use.

        As for the token clean-ups, the batch's row addresses are locked and then deleted by
        address, so a batch reads about its own rows.
        """
        expired: Select[tuple[Any]] = (
            select(literal_column("ctid"))
            .select_from(IdempotencyKey)
            .where(IdempotencyKey.created_at < before)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self.session.execute(
            delete(IdempotencyKey).where(
                literal_column("ctid") == any_(func.array(expired.scalar_subquery()))
            )
        )
        return int(result.rowcount)  # type: ignore[attr-defined]
