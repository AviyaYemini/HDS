"""The one definition of a user account in API responses.

Layer: routers. Imports models only for their enums; MUST NOT import services or repositories.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, EmailStr, Field

from hds.models.user import UserRole


class UserRead(BaseModel):
    """A user account as returned by the API.

    ``default_hourly_rate`` is a decimal string and is view-only for the user it belongs to
    (FR-017).
    """

    id: uuid.UUID
    organisation_id: uuid.UUID
    email: EmailStr
    full_name: str
    phone: str | None = None
    role: UserRole
    is_active: bool
    default_hourly_rate: Decimal | None = None  # FR-017: viewable by employee, not editable
    employment_type: Literal["permanent", "temporary"] = "permanent"
    engaged_from: date | None = Field(
        default=None, description="A temporary worker's first day; null for permanent staff."
    )
    engaged_until: date | None = Field(
        default=None,
        description="A temporary worker's last day, at most three months after the first.",
    )
    project_ids: list[uuid.UUID] = Field(
        default_factory=list,
        description="The projects a temporary worker works on; empty for permanent staff.",
    )
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
