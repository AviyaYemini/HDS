"""Failure counts and locks for keys that have no account row to hold them.

Layer: models. Depends on ``hds.models.base`` only.
"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import Base


class LoginThrottle(Base):
    """One count per submitted address, network address or reset key.

    It mirrors ``failed_login_attempts`` and ``locked_until`` on ``users``, so an address with no
    account is treated by the same rule as one with an account (FR-221). It has no organisation
    and no foreign key, because an unregistered address belongs to nothing. ``key_hash`` is the
    HMAC-SHA-256 hex digest of the key, never the address itself. ``kind`` is ``address``,
    ``network`` or ``reset``; the last two count in a fixed window that starts at
    ``window_started_at``. A row is unlocked when ``locked_until`` is empty or in the past.
    """

    __tablename__ = "login_throttle"

    kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    key_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    failures: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    window_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint("kind IN ('address', 'network', 'reset')", name="ck_login_throttle_kind"),
        CheckConstraint("failures >= 0", name="ck_login_throttle_failures"),
        Index("ix_login_throttle_updated_at", "updated_at"),
    )
