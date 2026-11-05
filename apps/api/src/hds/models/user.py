"""Accounts for staff and client contacts.

Layer: models. Depends on ``hds.models.base`` and sibling models only.
"""

import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from hds.models.base import TimestampedBase

if TYPE_CHECKING:
    from hds.models.client import Client
    from hds.models.group import Group
    from hds.models.qualification import Qualification
    from hds.models.refresh_token import RefreshToken


class UserRole(str, enum.Enum):
    """Role in the permission matrix (Employee, Manager, Admin, Client).

    Stored as the PostgreSQL enum ``user_role``; adding a value needs a migration.
    """

    employee = "employee"
    manager = "manager"
    admin = "admin"
    client = "client"


class User(TimestampedBase):
    """A person who can sign in.

    Rules the schema enforces: ``email`` is unique across all organisations among
    non-deleted users, whatever the letter case (partial index ``uq_users_email_ci`` on
    ``lower(email)``); ``default_hourly_rate`` is positive when set; users are soft-deleted
    (``is_deleted``) so history stays intact and the email can be reused.
    Sign-in failures and locks are not kept here: every address's are counted in
    ``login_throttle`` (FR-221), and migration 019 dropped the old columns.

    ``activated_at`` is when the account was first active; it is null only for a *pending* account
    (invited, never activated). The check constraint ``ck_users_active_has_activated_at`` makes
    "active without an activation time" impossible, so a deactivated account can never be mistaken
    for a pending one. An account is pending when it is not deleted, not active and has no
    activation time; deactivated when it is not active but has one.

    ``client_id`` ties a Client-role account to its client's projects. The schema does not
    force it to be set only for that role; the client services guarantee it.
    """

    __tablename__ = "users"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    phone: Mapped[str | None] = mapped_column(String(32))
    role: Mapped[UserRole] = mapped_column(Enum(UserRole, name="user_role"), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    default_hourly_rate: Mapped[Decimal | None] = mapped_column(
        Numeric(precision=12, scale=4, asdecimal=True)
    )
    client_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("clients.id"))

    client: Mapped["Client | None"] = relationship("Client", back_populates="users")
    groups: Mapped[list["Group"]] = relationship(
        "Group", secondary="user_groups", back_populates="users"
    )
    qualifications: Mapped[list["Qualification"]] = relationship(
        "Qualification", secondary="user_qualifications", back_populates="users"
    )
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(
        "RefreshToken", back_populates="user"
    )

    __table_args__ = (
        CheckConstraint("default_hourly_rate > 0", name="ck_users_hourly_rate_positive"),
        CheckConstraint(
            "is_active = false OR activated_at IS NOT NULL",
            name="ck_users_active_has_activated_at",
        ),
        Index(
            "uq_users_email_ci",
            func.lower(email),
            unique=True,
            postgresql_where=text("is_deleted = false"),
        ),
        Index("idx_users_org", "organization_id"),
        Index(
            "idx_users_client",
            "client_id",
            postgresql_where=text("client_id IS NOT NULL"),
        ),
    )
