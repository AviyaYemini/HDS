"""Availability submissions: one row per employee and window (007 FR-014, data-model 026).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from collections.abc import Collection
from datetime import datetime
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from hds.models.availability_submission import AvailabilitySubmission
from hds.repositories.base import BaseRepository


class AvailabilitySubmissionRepository(BaseRepository[AvailabilitySubmission]):
    """Reads and writes submissions; every query is limited to one organisation."""

    model = AvailabilitySubmission

    async def get(
        self, org_id: uuid.UUID, window_id: uuid.UUID, user_id: uuid.UUID
    ) -> AvailabilitySubmission | None:
        """The submission of ``user_id`` for ``window_id``, if they have submitted."""
        result = await self.session.execute(
            select(AvailabilitySubmission).where(
                AvailabilitySubmission.organisation_id == org_id,
                AvailabilitySubmission.window_id == window_id,
                AvailabilitySubmission.user_id == user_id,
            )
        )
        return result.scalar_one_or_none()

    async def for_windows(
        self,
        org_id: uuid.UUID,
        window_ids: Collection[uuid.UUID],
        user_ids: Collection[uuid.UUID] | None = None,
    ) -> list[AvailabilitySubmission]:
        """The submissions for ``window_ids``, only of ``user_ids`` when given."""
        if not window_ids or (user_ids is not None and not user_ids):
            return []
        query = select(AvailabilitySubmission).where(
            AvailabilitySubmission.organisation_id == org_id,
            AvailabilitySubmission.window_id.in_(list(window_ids)),
        )
        if user_ids is not None:
            query = query.where(AvailabilitySubmission.user_id.in_(list(user_ids)))
        return list((await self.session.execute(query)).scalars())

    async def put(
        self,
        org_id: uuid.UUID,
        window_id: uuid.UUID,
        user_id: uuid.UUID,
        *,
        submitted_at: datetime,
        submitted_by: uuid.UUID,
        snapshot: list[dict[str, Any]],
        snapshot_hash: str,
    ) -> AvailabilitySubmission:
        """Store the employee's submission for the window, replacing an earlier one.

        One ``INSERT … ON CONFLICT (window_id, user_id) DO UPDATE``, so two submits at once leave
        exactly one row, the later one's (the employee's row lock already orders them).
        """
        values = {
            "id": uuid.uuid4(),
            "organisation_id": org_id,
            "window_id": window_id,
            "user_id": user_id,
            "submitted_at": submitted_at,
            "submitted_by": submitted_by,
            "snapshot": snapshot,
            "snapshot_hash": snapshot_hash,
        }
        insert = pg_insert(AvailabilitySubmission).values(**values)
        replaced = ("submitted_at", "submitted_by", "snapshot", "snapshot_hash")
        upsert = insert.on_conflict_do_update(
            constraint="uq_availability_submissions_window_user",
            # A fresh submit is the employee's own again: an earlier hand change no longer applies.
            set_={
                **{key: insert.excluded[key] for key in replaced},
                "changed_by": None,
                "changed_at": None,
                "updated_at": func.now(),
            },
        ).returning(AvailabilitySubmission)
        result = await self.session.execute(upsert, execution_options={"populate_existing": True})
        stored: AvailabilitySubmission = result.scalar_one()
        await self.session.flush()
        return stored

    async def change(
        self,
        org_id: uuid.UUID,
        window_id: uuid.UUID,
        user_id: uuid.UUID,
        *,
        snapshot: list[dict[str, Any]],
        snapshot_hash: str,
        changed_by: uuid.UUID,
        changed_at: datetime,
    ) -> None:
        """Replace an existing submission's snapshot after an Admin's hand edit.

        ``submitted_at`` and ``submitted_by`` stay as they were; the change is recorded beside them.
        """
        await self.session.execute(
            update(AvailabilitySubmission)
            .where(
                AvailabilitySubmission.organisation_id == org_id,
                AvailabilitySubmission.window_id == window_id,
                AvailabilitySubmission.user_id == user_id,
            )
            .values(
                snapshot=snapshot,
                snapshot_hash=snapshot_hash,
                changed_by=changed_by,
                changed_at=changed_at,
                updated_at=func.now(),
            )
        )
        await self.session.flush()
