from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class Organisation(TimestampedBase):
    __tablename__ = "organisations"

    name: Mapped[str] = mapped_column(nullable=False)
    timezone: Mapped[str] = mapped_column(nullable=False)
