"""Declarative bases shared by all models.

Layer: models. Imports SQLAlchemy only.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Root of every mapped class; its ``metadata`` is what Alembic compares to the database."""


class TimestampedBase(Base):
    """Abstract base adding a UUID primary key and database-maintained timestamps.

    Timestamps are timezone-aware and set by the database clock (``now()``), so they are UTC
    regardless of the application host (Constitution VII). ``updated_at`` is refreshed on every
    ORM update.
    """

    __abstract__ = True

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
