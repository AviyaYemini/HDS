"""The tenant that owns all other records.

Layer: models. Depends on ``hds.models.base`` only.
"""

from sqlalchemy import CheckConstraint, SmallInteger, text
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class Organisation(TimestampedBase):
    """A tenant. Every other table carries ``organization_id`` pointing here.

    ``timezone`` is the IANA zone used to display this organisation's UTC timestamps; times
    are never stored in local time (Constitution VII). Its weeks for the weekly-hours rule run
    Sunday to Saturday in this zone.

    ``min_rest_hours`` (default 11) and ``max_weekly_hours`` (default 48) are the limits the roster
    rules warn against, both positive (docs/hds-spec.md section 5, H2 and H3). A new organisation
    gets the shift types morning, afternoon and night from a database trigger (migration 020).
    """

    __tablename__ = "organisations"

    name: Mapped[str] = mapped_column(nullable=False)
    timezone: Mapped[str] = mapped_column(nullable=False)
    min_rest_hours: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=11, server_default=text("11")
    )
    max_weekly_hours: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=48, server_default=text("48")
    )

    __table_args__ = (
        CheckConstraint("min_rest_hours > 0", name="ck_organisations_min_rest_positive"),
        CheckConstraint("max_weekly_hours > 0", name="ck_organisations_max_weekly_positive"),
    )
