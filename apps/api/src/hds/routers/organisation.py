"""Organisation router: the contact card every role sees (007 FR-012, C6).

Layer: routers. Delegates to ``OrganisationContactService``, which ``hds.wiring`` builds; HTTP only.
Every signed-in role reads the card; Admins change it.
"""

from typing import Annotated, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from hds.core.capabilities import Capability
from hds.core.deps import get_current_user, require_capability
from hds.core.types import WHOLE_NUMBER, ChangeRequest, Email, Phone, omittable
from hds.models.user import User
from hds.services.organisation_contact import OrganisationContactService
from hds.wiring import organisation_contact

router = APIRouter(prefix="/api/v1", tags=["organisation"])

LocalTime = Annotated[str, Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]


class OpeningHours(BaseModel):
    """One day's opening hours in the organisation's local time (0 is Sunday)."""

    # A JSON whole number only: never true or "3" (R165).
    weekday: Annotated[int, Field(ge=0, le=6), WHOLE_NUMBER]
    opens: LocalTime
    closes: LocalTime


class OrganisationContact(BaseModel):
    """The contact card: phone, e-mail and opening hours, each optional."""

    phone: str | None = None
    email: str | None = None
    opening_hours: list[OpeningHours] = []


class OrganisationContactUpdate(ChangeRequest):
    """Any of the card's fields; ``null`` clears one, an omitted one stays."""

    phone: Phone | None = omittable()
    email: Email | None = omittable()
    opening_hours: Annotated[list[OpeningHours], Field(max_length=7)] | None = omittable()


def _card(org: Any) -> OrganisationContact:
    return OrganisationContact(
        phone=org.contact_phone,
        email=org.contact_email,
        opening_hours=[OpeningHours(**item) for item in org.opening_hours or []],
    )


@router.get(
    "/organisation/contact",
    response_model=OrganisationContact,
    summary="Read the contact card",
    description=(
        "Every role, a client included. The organisation's phone, e-mail and opening hours, "
        "shown on the contact card and on every no-access page."
    ),
)
async def read_contact(
    current_user: User = Depends(get_current_user),
    svc: OrganisationContactService = Depends(organisation_contact),
) -> OrganisationContact:
    """Read the contact card.

    Every role. The organisation's phone, e-mail and opening hours.
    """
    return _card(await svc.read(current_user))


@router.patch(
    "/organisation/contact",
    response_model=OrganisationContact,
    summary="Change the contact card",
    description=(
        "Admin. Change the phone, e-mail or opening hours; `null` clears a field. Opening hours "
        "are up to seven days, each weekday once, closing after opening "
        "(`organisation.opening_hours_invalid`, 422)."
    ),
)
async def change_contact(
    body: OrganisationContactUpdate,
    current_user: User = Depends(
        require_capability(Capability.ORGANISATION_CONTACT_EDIT, code="users.admin_required")
    ),
    svc: OrganisationContactService = Depends(organisation_contact),
) -> OrganisationContact:
    """Change the contact card.

    Admin. Phone, e-mail or opening hours; null clears a field.
    """
    data = body.model_dump(exclude_unset=True)
    values: dict[str, Any] = {}
    if "phone" in data:
        values["contact_phone"] = data["phone"]
    if "email" in data:
        values["contact_email"] = data["email"]
    if "opening_hours" in data:
        values["opening_hours"] = data["opening_hours"]
    return _card(await svc.change(current_user, values))
