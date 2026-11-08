"""The organisation's contact card: phone, e-mail and opening hours (007 FR-012, C6).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the organisations
repository it is given; it MUST NOT import routers or know about HTTP.

Every role reads the card, a client included, because it is the way out of every dead end
(FR-007). Only an Admin changes it (``organisation.contact.edit``).
"""

from collections.abc import Mapping
from typing import Any

from hds.core.capabilities import Capability, has
from hds.core.exceptions import DomainValidationError, ForbiddenError, NotFoundError
from hds.models.organisation import Organisation
from hds.models.user import User
from hds.repositories.organisation import OrganisationRepository


def check_opening_hours(hours: list[dict[str, Any]] | None) -> None:
    """Refuse opening hours with a repeated weekday or a closing time not after the opening.

    The schema already bounds each item (weekday 0 to 6, ``HH:MM`` times); this is the rule
    across items and within one.

    Raises:
        ValidationError: ``organisation.opening_hours_invalid`` (HTTP 422).
    """
    if not hours:
        return
    weekdays = [item["weekday"] for item in hours]
    if len(set(weekdays)) != len(weekdays):
        raise DomainValidationError(
            code="organisation.opening_hours_invalid", detail="A weekday appears twice"
        )
    for item in hours:
        if item["closes"] <= item["opens"]:
            raise DomainValidationError(
                code="organisation.opening_hours_invalid",
                detail="Closing time must be after the opening time",
            )


class OrganisationContactService:
    """Read and change the caller's organisation's contact card."""

    def __init__(self, organisation_repo: OrganisationRepository) -> None:
        """Wire the service to the organisations repository."""
        self._orgs = organisation_repo

    async def read(self, actor: User) -> Organisation:
        """The caller's organisation, for its contact columns (every role)."""
        org = await self._orgs.get_by_id(actor.organization_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organization_id)
        return org

    async def change(self, actor: User, data: Mapping[str, Any]) -> Organisation:
        """Change any of the contact columns (Admin); ``None`` clears one.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            ValidationError: ``organisation.opening_hours_invalid`` (HTTP 422).
        """
        if not has(actor.role, Capability.ORGANISATION_CONTACT_EDIT):
            raise ForbiddenError(code="users.admin_required", detail="Admin role required")
        if "opening_hours" in data:
            check_opening_hours(data["opening_hours"])
        org = await self._orgs.update_contact(actor.organization_id, data)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organization_id)
        return org
