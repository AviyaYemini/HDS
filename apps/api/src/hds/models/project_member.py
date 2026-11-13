"""The people on a project's team: for now, the projects a temporary worker may work on.

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, func
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import Base


class ProjectMember(Base):
    """A person on a project's team, at most once (the primary key). Removed with its project.

    A temporary worker works only on the projects they are a member of, and only between their
    engagement's dates (CEO demo; project teams, O15, will use it for everyone).
    """

    __tablename__ = "project_members"

    organisation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("idx_project_members_user", "user_id"),)
