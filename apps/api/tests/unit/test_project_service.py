"""Unit tests for ProjectService manager scope."""

import uuid
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import DomainValidationError, ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.project import ProjectService


def _actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.role = role
    return user


def _project(**kw: object) -> MagicMock:
    p = MagicMock()
    p.id = kw.get("id", uuid.uuid4())
    p.billing_rate = kw.get("billing_rate", Decimal("75.0000"))
    p.status = kw.get("status", "active")
    p.start_date = kw.get("start_date", date(2026, 1, 1))
    p.end_date = kw.get("end_date", None)
    p.is_deleted = kw.get("is_deleted", False)
    return p


def _svc(repo: AsyncMock) -> ProjectService:
    return ProjectService(project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock())


@pytest.mark.asyncio
async def test_create_project_with_valid_billing_rate() -> None:
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=_project())
    actor = _actor()
    data = {
        "name": "Night Watch",
        "billing_rate": Decimal("75.00"),
        "type": "security",
        "start_date": date(2026, 1, 1),
        "client_id": uuid.uuid4(),
        "site_id": uuid.uuid4(),
    }

    await ProjectService(
        project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock()
    ).create_project(actor, data)

    repo.create.assert_awaited_once_with(actor.organization_id, data)


@pytest.mark.asyncio
@pytest.mark.parametrize("rate", [Decimal("0"), Decimal("-1")])
async def test_create_project_non_positive_billing_rate_raises(rate: Decimal) -> None:
    repo = AsyncMock()
    svc = _svc(repo)

    with pytest.raises(DomainValidationError) as exc_info:
        await svc.create_project(_actor(), {"billing_rate": rate, "name": "Bad"})

    assert exc_info.value.code == "projects.billing_rate_invalid"
    repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_project_end_before_start_raises() -> None:
    repo = AsyncMock()
    svc = _svc(repo)

    with pytest.raises(DomainValidationError) as exc_info:
        await svc.create_project(
            _actor(),
            {"start_date": date(2026, 2, 1), "end_date": date(2026, 1, 1), "name": "Bad"},
        )

    assert exc_info.value.code == "projects.date_range_invalid"
    repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_project_end_equal_to_start_allowed() -> None:
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=_project())
    day = date(2026, 1, 1)

    await ProjectService(
        project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock()
    ).create_project(
        _actor(),
        {
            "start_date": day,
            "end_date": day,
            "name": "One day",
            "client_id": uuid.uuid4(),
            "site_id": uuid.uuid4(),
        },
    )

    repo.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_edit_project_end_before_existing_start_raises() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_project(start_date=date(2026, 3, 1)))
    svc = _svc(repo)

    with pytest.raises(DomainValidationError) as exc_info:
        await svc.edit_project(_actor(), uuid.uuid4(), {"end_date": date(2026, 2, 1)})

    assert exc_info.value.code == "projects.date_range_invalid"
    repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_edit_project_invalid_billing_rate_raises() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_project())
    svc = _svc(repo)

    with pytest.raises(DomainValidationError) as exc_info:
        await svc.edit_project(_actor(), uuid.uuid4(), {"billing_rate": Decimal("0")})

    assert exc_info.value.code == "projects.billing_rate_invalid"


@pytest.mark.asyncio
async def test_edit_project_not_found() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await ProjectService(
            project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock()
        ).edit_project(_actor(), uuid.uuid4(), {"name": "X"})


@pytest.mark.asyncio
@pytest.mark.parametrize(("current", "target"), [("active", "ended"), ("ended", "active")])
async def test_set_status_transitions(current: str, target: str) -> None:
    proj = _project(status=current)
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=proj)
    repo.update = AsyncMock(return_value=_project(status=target))

    await ProjectService(
        project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock()
    ).set_status(_actor(), proj.id, target)

    repo.update.assert_awaited_once_with(proj.id, status=target)


@pytest.mark.asyncio
async def test_soft_delete_project_by_admin() -> None:
    proj = _project()
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=proj)

    await ProjectService(
        project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock()
    ).soft_delete(_actor(UserRole.admin), proj.id)

    repo.soft_delete.assert_awaited_once_with(proj.id)


@pytest.mark.asyncio
async def test_soft_delete_project_by_manager_is_forbidden() -> None:
    repo = AsyncMock()

    with pytest.raises(ForbiddenError):
        await ProjectService(
            project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock()
        ).soft_delete(_actor(UserRole.manager), uuid.uuid4())

    repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_non_manager_cannot_mutate(role: UserRole) -> None:
    repo = AsyncMock()
    svc = _svc(repo)
    actor = _actor(role)

    with pytest.raises(ForbiddenError):
        await svc.create_project(actor, {"name": "X"})
    with pytest.raises(ForbiddenError):
        await svc.edit_project(actor, uuid.uuid4(), {"name": "X"})
    with pytest.raises(ForbiddenError):
        await svc.set_status(actor, uuid.uuid4(), "ended")

    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()
