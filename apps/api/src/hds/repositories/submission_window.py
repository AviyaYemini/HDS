"""Submission windows: the periods employees may submit availability for.

Layer: repositories. Depends on ``hds.models``, ``hds.core.exceptions`` and
``hds.repositories.base`` only; it MUST NOT import services or routers.
"""

import uuid
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from hds.core.exceptions import ConflictError
from hds.models.organisation import Organisation
from hds.models.submission_window import SubmissionWindow
from hds.repositories.base import OrgScopedRepository

_OVERLAP = "ex_submission_windows_overlap"


def _overlap() -> ConflictError:
    return ConflictError(
        code="windows.overlap", detail="The window shares dates with another window."
    )


class SubmissionWindowRepository(OrgScopedRepository[SubmissionWindow]):
    """An organisation's windows; two never share a date (research R5)."""

    model = SubmissionWindow

    async def covering(
        self, org_id: uuid.UUID, first: date | None, last: date | None
    ) -> list[SubmissionWindow]:
        """The windows that share at least one date with ``first`` to ``last``, by start date.

        Args:
            org_id: The caller's organisation.
            first: First date, inclusive; no lower bound when ``None``.
            last: Last date, inclusive; no upper bound when ``None``.

        Returns:
            The windows in date order.
        """
        stmt = select(SubmissionWindow).where(SubmissionWindow.organisation_id == org_id)
        if last is not None:
            stmt = stmt.where(SubmissionWindow.starts_on <= last)
        if first is not None:
            stmt = stmt.where(SubmissionWindow.ends_on >= first)
        result = await self.session.execute(stmt.order_by(SubmissionWindow.starts_on))
        return list(result.scalars().all())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> SubmissionWindow:
        """Insert a window and flush.

        Args:
            org_id: Organisation that owns it.
            data: ``starts_on``, ``ends_on``, ``deadline`` and ``created_by``.

        Returns:
            The stored window.

        Raises:
            ConflictError: ``windows.overlap`` (HTTP 409) when it shares a date with another
                window, including one created at the same moment by another request.

        Side effects:
            Locks the organisation's row until the transaction ends.
        """
        # Two overlapping inserts at once can deadlock inside the exclusion constraint, so window
        # creation is serialised per organisation; the second then meets the constraint and fails.
        # FOR NO KEY UPDATE, not FOR UPDATE, so the foreign-key checks of other inserts in the
        # organisation (FOR KEY SHARE) neither wait for it nor deadlock with it.
        await self.session.execute(
            select(Organisation.id).where(Organisation.id == org_id).with_for_update(key_share=True)
        )
        window = SubmissionWindow(id=uuid.uuid4(), organisation_id=org_id, **data)
        try:
            async with self.session.begin_nested():
                self.session.add(window)
                await self.session.flush()
        except IntegrityError as exc:
            if _OVERLAP in str(exc.orig):
                raise _overlap() from None
            raise
        return window

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> SubmissionWindow:
        """Change a window of the organisation.

        Raises:
            NotFoundError: The window is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Remove a window of the organisation; nothing happens when it is not there."""
        await self._delete_scoped(id, org_id)
