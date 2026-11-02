"""Qualifications an employee can hold, and the membership table.

Layer: models. Depends on ``hds.models.base`` and sibling models only.
"""

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Index, String, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import Base, TimestampedBase

if TYPE_CHECKING:
    from hds.models.user import User

# Association table for the many-to-many User <-> Qualification relationship
user_qualifications = Table(
    "user_qualifications",
    Base.metadata,
    Column("user_id", ForeignKey("users.id"), primary_key=True),
    Column(
        "qualification_id",
        ForeignKey("qualifications.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class Qualification(TimestampedBase):
    """A named skill or certificate, unique by name within an organisation.

    Deleting one cascades to its membership rows (``ON DELETE CASCADE`` on
    ``user_qualifications.qualification_id``) but never to the users.
    """

    __tablename__ = "qualifications"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    users: Mapped[list["User"]] = relationship(
        "User", secondary=user_qualifications, back_populates="qualifications"
    )

    __table_args__ = (Index("uq_qualifications_org_name", "organization_id", "name", unique=True),)
