"""Unit tests for QualificationService."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ConflictError, ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.qualification import QualificationService


def _actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.role = role
    return user


def _qual(**kw: object) -> MagicMock:
    q = MagicMock()
    q.id = kw.get("id", uuid.uuid4())
    q.name = kw.get("name", "First Aid")
    return q


@pytest.mark.asyncio
async def test_create_qualification_success() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_name_and_org = AsyncMock(return_value=None)
    repo.create = AsyncMock(return_value=_qual(name="First Aid"))

    svc = QualificationService(qual_repo=repo)
    await svc.create_qualification(actor, "First Aid")

    repo.create.assert_awaited_once_with(actor.organization_id, "First Aid")


@pytest.mark.asyncio
async def test_create_qualification_duplicate_raises_conflict() -> None:
    repo = AsyncMock()
    repo.get_by_name_and_org = AsyncMock(return_value=_qual())

    svc = QualificationService(qual_repo=repo)
    with pytest.raises(ConflictError) as exc_info:
        await svc.create_qualification(_actor(), "First Aid")
    assert exc_info.value.code == "qualifications.duplicate_name"
    repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_rename_qualification() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_qual())
    repo.get_by_name_and_org = AsyncMock(return_value=None)
    repo.update = AsyncMock(return_value=_qual(name="Advanced First Aid"))

    svc = QualificationService(qual_repo=repo)
    qual_id = uuid.uuid4()
    await svc.rename_qualification(actor, qual_id, "Advanced First Aid")

    repo.update.assert_awaited_once_with(qual_id, actor.organization_id, name="Advanced First Aid")


@pytest.mark.asyncio
async def test_rename_qualification_not_found() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=None)

    svc = QualificationService(qual_repo=repo)
    with pytest.raises(NotFoundError):
        await svc.rename_qualification(_actor(), uuid.uuid4(), "X")


@pytest.mark.asyncio
async def test_delete_qualification() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_qual())
    repo.delete = AsyncMock()

    svc = QualificationService(qual_repo=repo)
    qual_id = uuid.uuid4()
    await svc.delete_qualification(actor, qual_id)

    repo.delete.assert_awaited_once_with(qual_id, actor.organization_id)


@pytest.mark.asyncio
async def test_replace_employee_qualifications() -> None:
    repo = AsyncMock()
    repo.replace_user_qualifications = AsyncMock()

    svc = QualificationService(qual_repo=repo)
    user_id = uuid.uuid4()
    qual_ids = [uuid.uuid4()]
    await svc.replace_employee_qualifications(_actor(), user_id, qual_ids)

    repo.replace_user_qualifications.assert_awaited_once_with(user_id, qual_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_non_manager_is_forbidden_everywhere(role: UserRole) -> None:
    repo = AsyncMock()
    svc = QualificationService(qual_repo=repo)
    actor = _actor(role)
    qid = uuid.uuid4()

    with pytest.raises(ForbiddenError):
        await svc.create_qualification(actor, "X")
    with pytest.raises(ForbiddenError):
        await svc.rename_qualification(actor, qid, "X")
    with pytest.raises(ForbiddenError):
        await svc.delete_qualification(actor, qid)
    with pytest.raises(ForbiddenError):
        await svc.replace_employee_qualifications(actor, uuid.uuid4(), [])

    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()
    repo.delete.assert_not_awaited()
    repo.replace_user_qualifications.assert_not_awaited()
