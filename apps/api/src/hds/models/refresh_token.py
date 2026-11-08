"""Server-side record of issued refresh tokens.

Layer: models. Depends on ``hds.models.base`` and ``hds.models.user`` only.
"""

import enum
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import Base

if TYPE_CHECKING:
    from hds.models.user import User


class RevocationReason(str, enum.Enum):
    """Why a refresh token was revoked.

    Stored as short text in ``refresh_tokens.revoked_reason`` and restricted by a check
    constraint, so adding a value needs a migration. Only ``rotated`` can mean "this token was
    replayed after it was replaced", which is what the theft response looks for.
    """

    rotated = "rotated"  # replaced by a new token when it was used to refresh
    logout = "logout"  # the user signed out of this session
    all = "all"  # every session of the user ended (sign-out everywhere, password reset, reuse)
    account = "account"  # the account was deactivated, deleted or changed role


class RefreshToken(Base):
    """One issued refresh token, identified only by its SHA-256 hash.

    The raw token is never stored. A token is usable while ``revoked_at`` is ``None`` and
    ``expires_at`` is in the future; rotation, sign-out and password reset set
    ``revoked_at`` instead of deleting the row, so a replayed old token is rejected as revoked
    rather than as unknown. ``revoked_reason`` says why (see `RevocationReason`); it is empty
    for tokens revoked before the column existed, which the service treats as unknown.

    ``client_kind`` is ``web`` or ``native``, as the client declared at sign-in, and decides the
    token's lifetime class; a rotation copies it to the new token (007 FR-010, ADR 016). It is
    self-declared, which ADR 016 records as an accepted limit.
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
    revoked_reason: Mapped[str | None] = mapped_column(String(16))
    client_kind: Mapped[str] = mapped_column(
        String(8), nullable=False, default="web", server_default="web"
    )

    user: Mapped["User"] = relationship("User", back_populates="refresh_tokens")

    __table_args__ = (
        CheckConstraint(
            "revoked_reason IS NULL OR revoked_reason IN "
            + "("
            + ", ".join(f"'{reason.value}'" for reason in RevocationReason)
            + ")",
            name="ck_refresh_tokens_revoked_reason",
        ),
        CheckConstraint("client_kind IN ('web', 'native')", name="ck_refresh_tokens_client_kind"),
        Index("idx_refresh_tokens_user", "user_id"),
        Index("idx_refresh_tokens_expires_at", "expires_at"),  # the bulk clean-up
    )
