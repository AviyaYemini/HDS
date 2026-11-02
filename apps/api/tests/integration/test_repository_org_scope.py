"""T140: repository update/soft_delete/delete take org_id and are safe on their own."""

import uuid
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.exceptions import NotFoundError
from hds.models.organisation import Organisation
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.site import SiteRepository
from hds.repositories.user import UserRepository
from tests.integration.conftest import OtherOrg


@pytest.mark.asyncio
async def test_update_with_wrong_org_changes_nothing(
    test_db: AsyncSession, seed_org: Organisation, other_org: OtherOrg
) -> None:
    cases: list[tuple[Any, uuid.UUID, str, Any]] = [
        (UserRepository(test_db), other_org.user.id, "full_name", "Hacked"),
        (ClientRepository(test_db), other_org.client.id, "name", "Hacked"),
        (SiteRepository(test_db), other_org.site.id, "name", "Hacked"),
        (ProjectRepository(test_db), other_org.project.id, "name", "Hacked"),
        (GroupRepository(test_db), other_org.group.id, "name", "Hacked"),
        (QualificationRepository(test_db), other_org.qualification.id, "name", "Hacked"),
    ]
    for repo, row_id, field, value in cases:
        with pytest.raises(NotFoundError):
            await repo.update(row_id, seed_org.id, **{field: value})
        row = await repo.get_by_id(row_id, other_org.org.id)
        assert row is not None and getattr(row, field) != value, type(repo).__name__


@pytest.mark.asyncio
async def test_update_with_right_org_works(
    test_db: AsyncSession, seed_org: Organisation, other_org: OtherOrg
) -> None:
    updated = await GroupRepository(test_db).update(other_org.group.id, other_org.org.id, name="N")
    assert updated.name == "N"


@pytest.mark.asyncio
async def test_soft_delete_and_delete_with_wrong_org_change_nothing(
    test_db: AsyncSession, seed_org: Organisation, other_org: OtherOrg
) -> None:
    mine = seed_org.id
    await UserRepository(test_db).soft_delete(other_org.user.id, mine)
    await ClientRepository(test_db).soft_delete(other_org.client.id, mine)
    await ProjectRepository(test_db).soft_delete(other_org.project.id, mine)
    await GroupRepository(test_db).delete(other_org.group.id, mine)
    await QualificationRepository(test_db).delete(other_org.qualification.id, mine)
    theirs = other_org.org.id
    for repo, row_id in [
        (UserRepository(test_db), other_org.user.id),
        (ClientRepository(test_db), other_org.client.id),
        (ProjectRepository(test_db), other_org.project.id),
        (GroupRepository(test_db), other_org.group.id),
        (QualificationRepository(test_db), other_org.qualification.id),
    ]:
        assert await repo.get_by_id(row_id, theirs) is not None, type(repo).__name__


@pytest.mark.asyncio
async def test_soft_delete_with_right_org_works(
    test_db: AsyncSession, seed_org: Organisation, other_org: OtherOrg
) -> None:
    await ProjectRepository(test_db).soft_delete(other_org.project.id, other_org.org.id)
    assert (
        await ProjectRepository(test_db).get_by_id(other_org.project.id, other_org.org.id) is None
    )
