import enum
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import TimestampedBase

if TYPE_CHECKING:
    from hds.models.client import Client
    from hds.models.site import Site


class ProjectType(str, enum.Enum):
    security = "security"
    training = "training"


class ProjectStatus(str, enum.Enum):
    active = "active"
    ended = "ended"


class Project(TimestampedBase):
    __tablename__ = "projects"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    client_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("clients.id"), nullable=False)
    site_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sites.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[ProjectType] = mapped_column(
        Enum(ProjectType, name="project_type"), nullable=False
    )
    # Billing rate stored as NUMERIC(12,4) — NEVER Float (Constitution Principle VII)
    billing_rate: Mapped[Decimal] = mapped_column(
        Numeric(precision=12, scale=4, asdecimal=True),
        nullable=False,
        comment="Must be > 0. Stored as NUMERIC(12,4), never Float.",
    )
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[ProjectStatus] = mapped_column(
        Enum(ProjectStatus, name="project_status"), nullable=False, default=ProjectStatus.active
    )
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    client: Mapped["Client"] = relationship("Client", back_populates="projects")
    site: Mapped["Site"] = relationship("Site")

    __table_args__ = (
        CheckConstraint("billing_rate > 0", name="ck_projects_billing_rate_positive"),
        Index("idx_projects_org", "organization_id"),
        Index("idx_projects_client", "client_id"),
    )
