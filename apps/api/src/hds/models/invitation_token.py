"""One-time tokens for accepting an invitation or resetting a password.

Layer: models. Depends on ``hds.models.base`` and ``hds.models.user`` only.
"""

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import Base

if TYPE_CHECKING:
    from hds.models.user import User


class InvitationToken(Base):
    """A single-use token, stored only as a SHA-256 hash.

    ``purpose`` is ``invite`` (set the first password) or ``reset`` (replace a forgotten one);
    the database rejects any other value. A token is spent by setting ``used_at`` and is
    invalid after ``expires_at``.
    """

    __tablename__ = "invitation_tokens"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    purpose: Mapped[str] = mapped_column(String(16), nullable=False, comment="invite | reset")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    user: Mapped["User"] = relationship("User")

    __table_args__ = (
        CheckConstraint("purpose IN ('invite','reset')", name="ck_invitation_tokens_purpose"),
        Index("idx_invitation_tokens_user", "user_id"),
    )
