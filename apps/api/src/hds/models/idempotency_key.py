"""Stored answers of retried writes, so a repeat returns the first answer (003 FR-115a).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CHAR, JSON, DateTime, ForeignKey, Index, SmallInteger, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import Base


class IdempotencyKey(Base):
    """One caller's ``Idempotency-Key`` and the answer its first request got.

    The primary key is (``user_id``, ``key``), so keys are per caller and one user can never be
    given another's answer; a concurrent repeat waits on the key's row. ``request_hash`` is the
    SHA-256 hex digest of the method, path and body: the same key with another request is refused.
    Keys older than 24 hours are removed by the purge script (research R8).
    """

    __tablename__ = "idempotency_keys"

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), primary_key=True)
    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    status_code: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    # JSONB in PostgreSQL; plain JSON where the SQLite unit-test schema is built.
    response_body: Mapped[Any] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("idx_idempotency_keys_created_at", "created_at"),)
