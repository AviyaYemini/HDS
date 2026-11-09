"""A user's saved view of a table: a name and the web address's query (007 FR-043, A2).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid

from sqlalchemy import CheckConstraint, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class SavedView(TimestampedBase):
    """One user's named filters and sorting on one screen's table.

    ``screen`` names the table (for example ``staff`` or ``work_log``); ``query`` is the web
    address's query string the view restores (A19). Names are unique per user and screen. Only its
    owner reads, adds or removes it.
    """

    __tablename__ = "saved_views"

    organisation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    screen: Mapped[str] = mapped_column(String(48), nullable=False)
    name: Mapped[str] = mapped_column(String(60), nullable=False)
    query: Mapped[str] = mapped_column(String(2000), nullable=False)

    __table_args__ = (
        UniqueConstraint("user_id", "screen", "name", name="uq_saved_views_user_screen_name"),
        CheckConstraint("length(name) BETWEEN 1 AND 60", name="ck_saved_views_name"),
        Index("idx_saved_views_user_screen", "user_id", "screen"),
    )
