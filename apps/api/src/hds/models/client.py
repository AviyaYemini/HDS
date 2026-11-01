import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import TimestampedBase

if TYPE_CHECKING:
    from hds.models.project import Project
    from hds.models.user import User


class Client(TimestampedBase):
    __tablename__ = "clients"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_name: Mapped[str | None] = mapped_column(String(255))
    contact_email: Mapped[str | None] = mapped_column(String(320))
    contact_phone: Mapped[str | None] = mapped_column(String(32))
    contract_start: Mapped[date] = mapped_column(Date, nullable=False)
    contract_end: Mapped[date | None] = mapped_column(Date)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    users: Mapped[list["User"]] = relationship("User", back_populates="client")
    projects: Mapped[list["Project"]] = relationship("Project", back_populates="client")

    __table_args__ = (Index("idx_clients_org", "organization_id"),)
