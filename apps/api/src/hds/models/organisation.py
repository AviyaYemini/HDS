"""The tenant that owns all other records.

Layer: models. Depends on ``hds.models.base`` only.
"""

from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class Organisation(TimestampedBase):
    """A tenant. Every other table carries ``organization_id`` pointing here.

    ``timezone`` is the IANA zone used to display this organisation's UTC timestamps; times
    are never stored in local time (Constitution VII).
    """

    __tablename__ = "organisations"

    name: Mapped[str] = mapped_column(nullable=False)
    timezone: Mapped[str] = mapped_column(nullable=False)
