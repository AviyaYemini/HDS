"""Unit tests for SiteService (T075).

Covers: FR-023.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import DomainValidationError, ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.site import SiteService


def _actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.role = role
    return user


def _site(**kw: object) -> MagicMock:
    s = MagicMock()
    s.id = kw.get("id", uuid.uuid4())
    s.name = kw.get("name", "Main Gate")
    return s


@pytest.mark.asyncio
async def test_create_site_success() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=_site())
    data = {"name": "Main Gate", "address": "1 Example St", "timezone": "UTC"}

    svc = SiteService(site_repo=repo)
    await svc.create_site(actor, data)

    repo.create.assert_awaited_once_with(actor.organization_id, data)


@pytest.mark.asyncio
async def test_edit_site_success() -> None:
    actor = _actor()
    repo = AsyncMock()
    existing = _site()
    repo.get_by_id = AsyncMock(return_value=existing)
    repo.update = AsyncMock(return_value=_site(name="Side Gate"))

    svc = SiteService(site_repo=repo)
    await svc.edit_site(actor, existing.id, {"name": "Side Gate"})

    repo.update.assert_awaited_once_with(existing.id, actor.organization_id, name="Side Gate")


@pytest.mark.asyncio
async def test_edit_site_not_found() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=None)

    svc = SiteService(site_repo=repo)
    with pytest.raises(NotFoundError):
        await svc.edit_site(_actor(), uuid.uuid4(), {"name": "X"})
    repo.update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_non_manager_is_forbidden(role: UserRole) -> None:
    repo = AsyncMock()
    svc = SiteService(site_repo=repo)
    actor = _actor(role)

    with pytest.raises(ForbiddenError):
        await svc.create_site(actor, {"name": "X"})
    with pytest.raises(ForbiddenError):
        await svc.edit_site(actor, uuid.uuid4(), {"name": "X"})

    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()


# ── Time zones must be real IANA names (T293, FR-217) ────────────────────────────────────

GOOD_ZONES = ["Asia/Jerusalem", "UTC", "Asia/Kolkata", "America/Argentina/Buenos_Aires"]
BAD_ZONES = [
    "Mars/Base",
    "",
    " ",
    "../etc/passwd",
    "Asia/../Asia/Jerusalem",
    "/etc/localtime",
    "Asia\\Jerusalem",
    "UTC\x00",
    "israel time",
    "A" * 300,
]


@pytest.mark.asyncio
@pytest.mark.parametrize("zone", GOOD_ZONES)
async def test_a_real_time_zone_is_accepted_on_create_and_edit(zone: str) -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=_site())
    repo.get_by_id = AsyncMock(return_value=_site())
    repo.update = AsyncMock(return_value=_site())
    svc = SiteService(site_repo=repo)

    await svc.create_site(actor, {"name": "S", "address": "x", "timezone": zone})
    await svc.edit_site(actor, uuid.uuid4(), {"timezone": zone})

    repo.create.assert_awaited_once()
    repo.update.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("zone", BAD_ZONES)
async def test_an_invalid_time_zone_is_refused_on_create_and_edit_and_nothing_is_stored(
    zone: str,
) -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_site())
    svc = SiteService(site_repo=repo)

    with pytest.raises(DomainValidationError) as on_create:
        await svc.create_site(actor, {"name": "S", "address": "x", "timezone": zone})
    with pytest.raises(DomainValidationError) as on_edit:
        await svc.edit_site(actor, uuid.uuid4(), {"timezone": zone})

    assert on_create.value.code == on_edit.value.code == "sites.timezone_invalid"
    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_edit_that_does_not_mention_the_time_zone_is_not_checked() -> None:
    """A site that already holds a bad zone can still be renamed; the report script lists it."""
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_site())
    repo.update = AsyncMock(return_value=_site())

    await SiteService(site_repo=repo).edit_site(_actor(), uuid.uuid4(), {"name": "Renamed"})

    repo.update.assert_awaited_once()
