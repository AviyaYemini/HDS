"""Shift types and the shift types a project uses.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. Every read is scoped to the caller's organisation.
"""

from hds.models.shift_type import ShiftType
from hds.repositories.base import OrgScopedRepository


class ShiftTypeRepository(OrgScopedRepository[ShiftType]):
    """An organisation's shift types; ``get_by_id`` of another organisation's type is ``None``."""

    model = ShiftType
