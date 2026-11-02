"""Employee groups and their membership table.

Layer: models. Depends on ``hds.models.base`` and sibling models only.
"""

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Index, String, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import Base, TimestampedBase

if TYPE_CHECKING:
    from hds.models.user import User

# Association table for the many-to-many User <-> Group relationship
user_groups = Table(
    "user_groups",
    Base.metadata,
    Column("user_id", ForeignKey("users.id"), primary_key=True),
    Column("group_id", ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True),
)


class Group(TimestampedBase):
    """A named set of employees, unique by name within an organisation.

    Deleting a group cascades to its membership rows (``ON DELETE CASCADE`` on
    ``user_groups.group_id``) but never to the users themselves.
    """

    __tablename__ = "groups"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    users: Mapped[list["User"]] = relationship(
        "User", secondary=user_groups, back_populates="groups"
    )

    __table_args__ = (Index("uq_groups_org_name", "organization_id", "name", unique=True),)
