"""The contact card: every role reads it, only an Admin changes it, and opening hours are sane.

Covers: 007 FR-012 (C6, task T027).
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import DomainValidationError, ForbiddenError
from hds.models.user import User, UserRole
from hds.services.organisation_contact import OrganisationContactService, check_opening_hours


def _user(role: UserRole) -> User:
    user = MagicMock(spec=User)
    user.role, user.organization_id = role, uuid.uuid4()
    return user


@pytest.mark.asyncio
@pytest.mark.parametrize("role", list(UserRole))
async def test_every_role_reads_the_card(role: UserRole) -> None:
    orgs = AsyncMock()
    orgs.get_by_id.return_value = MagicMock()
    assert await OrganisationContactService(orgs).read(_user(role)) is orgs.get_by_id.return_value


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.manager, UserRole.client])
async def test_only_an_admin_changes_it(role: UserRole) -> None:
    orgs = AsyncMock()
    with pytest.raises(ForbiddenError):
        await OrganisationContactService(orgs).change(_user(role), {"contact_phone": "03-1234567"})
    orgs.update_contact.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_admin_changes_and_clears_fields() -> None:
    orgs = AsyncMock()
    admin = _user(UserRole.admin)
    await OrganisationContactService(orgs).change(admin, {"contact_phone": None})
    orgs.update_contact.assert_awaited_once_with(admin.organization_id, {"contact_phone": None})


def test_opening_hours_refuse_a_repeated_weekday() -> None:
    day = {"weekday": 0, "opens": "08:00", "closes": "17:00"}
    with pytest.raises(DomainValidationError) as refused:
        check_opening_hours([day, dict(day)])
    assert refused.value.code == "organisation.opening_hours_invalid"


def test_opening_hours_refuse_closing_before_opening() -> None:
    with pytest.raises(DomainValidationError):
        check_opening_hours([{"weekday": 1, "opens": "17:00", "closes": "08:00"}])


def test_opening_hours_accept_a_normal_week_and_none() -> None:
    check_opening_hours([{"weekday": d, "opens": "08:00", "closes": "17:00"} for d in range(5)])
    check_opening_hours(None)
    check_opening_hours([])
