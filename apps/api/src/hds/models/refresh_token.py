"""Server-side record of issued refresh tokens.

Layer: models. Depends on ``hds.models.base`` and ``hds.models.user`` only.
"""

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import Base

if TYPE_CHECKING:
    from hds.models.user import User


class RefreshToken(Base):
    """One issued refresh token, identified only by its SHA-256 hash.

    The raw token is never stored. A token is usable while ``revoked_at`` is ``None`` and
    ``expires_at`` is in the future; rotation, sign-out and password reset set
    ``revoked_at`` instead of deleting the row, so a replayed old token is rejected as revoked
    rather than as unknown.
    """

    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    user: Mapped["User"] = relationship("User", back_populates="refresh_tokens")

    __table_args__ = (Index("idx_refresh_tokens_user", "user_id"),)
