"""Managers on projects: who may see a project's pay when it is open to Managers (005 FR-021).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, func
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import Base


class ProjectManager(Base):
    """A Manager or Admin assigned to a project, at most once (the primary key).

    The service checks the user is an active Manager or Admin of the organisation when the set is
    replaced; whether they may see pay is decided on every read from the live role (005 FR-022).
    Removed with its project.
    """

    __tablename__ = "project_managers"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("idx_project_managers_user", "user_id"),)
