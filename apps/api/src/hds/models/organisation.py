"""The tenant that owns all other records.

Layer: models. Depends on ``hds.models.base`` and ``hds.models.pay_columns`` only.
"""

from datetime import time
from decimal import Decimal
from typing import Any

from sqlalchemy import JSON, CheckConstraint, Numeric, SmallInteger, String, Time, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase
from hds.models.pay_columns import pay_setting_checks


class Organisation(TimestampedBase):
    """A tenant. Every other table carries ``organization_id`` pointing here.

    ``timezone`` is the IANA zone used to display this organisation's UTC timestamps; times
    are never stored in local time (Constitution VII). Its weeks for the weekly-hours rule run
    Sunday to Saturday in this zone.

    ``min_rest_hours`` (default 11) and ``max_weekly_hours`` (default 48) are the limits the roster
    rules warn against, both positive (docs/hds-spec.md section 5, H2 and H3). A new organisation
    gets the shift types morning, afternoon and night from a database trigger (migration 020).

    The ``pay_*`` columns are the premium defaults every project follows unless it has its own
    value (005 FR-006): normal lengths and the first overtime tier in minutes, rates in percent
    (``NUMERIC(5,2)``), increases in points, and the weekly rest window as weekday (0 Sunday) and
    local time at both ends. Their bounds are in ``hds.models.pay_columns``.

    ``contact_phone``, ``contact_email`` and ``opening_hours`` are the contact card every role
    sees and only an Admin changes (007 FR-012). ``opening_hours`` is a list of up to seven
    ``{"weekday": 0-6, "opens": "HH:MM", "closes": "HH:MM"}`` items in the organisation's local
    time; the router and service validate its shape. All three are optional.
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

    pay_day_normal_minutes: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=480, server_default=text("480")
    )
    pay_night_normal_minutes: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=420, server_default=text("420")
    )
    pay_tier1_minutes: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=120, server_default=text("120")
    )
    pay_overtime1_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("125.00"), server_default=text("125.00")
    )
    pay_overtime2_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("150.00"), server_default=text("150.00")
    )
    pay_rest_start_weekday: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=5, server_default=text("5")
    )
    pay_rest_start_time: Mapped[time] = mapped_column(
        Time, nullable=False, default=time(18, 0), server_default=text("'18:00'")
    )
    pay_rest_end_weekday: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=6, server_default=text("6")
    )
    pay_rest_end_time: Mapped[time] = mapped_column(
        Time, nullable=False, default=time(22, 0), server_default=text("'22:00'")
    )
    pay_rest_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("150.00"), server_default=text("150.00")
    )
    pay_increase1_points: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("25.00"), server_default=text("25.00")
    )
    pay_increase2_points: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), nullable=False, default=Decimal("50.00"), server_default=text("50.00")
    )

    contact_phone: Mapped[str | None] = mapped_column(String(32))
    contact_email: Mapped[str | None] = mapped_column(String(254))
    # JSONB on PostgreSQL; the SQLite unit-test schema stores it as JSON text.
    opening_hours: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql")
    )

    __table_args__ = (
        CheckConstraint("min_rest_hours BETWEEN 1 AND 24", name="ck_organisations_min_rest_range"),
        CheckConstraint(
            "max_weekly_hours BETWEEN 1 AND 168", name="ck_organisations_max_weekly_range"
        ),
        *pay_setting_checks("organisations", optional=False),
    )
