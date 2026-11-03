"""The one definition of a user account in API responses.

Layer: routers. Imports models only for their enums; MUST NOT import services or repositories.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, EmailStr

from hds.models.user import UserRole


class UserRead(BaseModel):
    """A user account as returned by the API.

    ``default_hourly_rate`` is a decimal string and is view-only for the user it belongs to
    (FR-017).
    """

    id: uuid.UUID
    organization_id: uuid.UUID
    email: EmailStr
    full_name: str
    phone: str | None = None
    role: UserRole
    is_active: bool
    default_hourly_rate: Decimal | None = None  # FR-017: viewable by employee, not editable
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
