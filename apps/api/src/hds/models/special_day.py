"""Special days: dates paid at their own rate, for the organisation or one project (005 FR-008).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import date
from decimal import Decimal

from sqlalchemy import CheckConstraint, Date, ForeignKey, Index, Numeric, String, text
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class SpecialDay(TimestampedBase):
    """A local date paid at ``rate_pct`` percent, such as Yom Kippur or an election day.

    An organisation day has no ``project_id`` and applies to every project; a project day applies
    to that project only and, on a date the organisation also lists, overrides the organisation's
    rate for it (005 FR-008a). One organisation day per date (``uq_special_days_org_day``) and one
    day per project and date (``uq_special_days_project_day``). The rate is 100 to 300 percent
    (``ck_special_days_rate``); the name is not blank. Removed with its project.
    """

    __tablename__ = "special_days"

    organisation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE")
    )
    day: Mapped[date] = mapped_column(Date, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    rate_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("150.00"), server_default=text("150.00")
    )

    __table_args__ = (
        CheckConstraint("rate_pct BETWEEN 100 AND 300", name="ck_special_days_rate"),
        CheckConstraint("trim(name) <> ''", name="ck_special_days_name"),
        Index(
            "uq_special_days_org_day",
            "organisation_id",
            "day",
            unique=True,
            postgresql_where=text("project_id IS NULL"),
            sqlite_where=text("project_id IS NULL"),
        ),
        Index(
            "uq_special_days_project_day",
            "project_id",
            "day",
            unique=True,
            postgresql_where=text("project_id IS NOT NULL"),
            sqlite_where=text("project_id IS NOT NULL"),
        ),
    )
