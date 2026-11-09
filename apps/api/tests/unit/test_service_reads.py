"""Reads go through services, with the role check in the service (T401, F20).

Constitution VI puts the role matrix in the service layer. These reads used to live in the routers,
so only the route's dependency protected them.

Covers: FR-239, Constitution II, Constitution VI.
"""

import uuid
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.client import ClientService
from hds.services.employee import EmployeeService
from hds.services.group import GroupService
from hds.services.qualification import QualificationService
from hds.services.site import SiteService

ID = uuid.uuid4()
Read = Callable[[User], Awaitable[Any]]


def actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organisation_id = uuid.uuid4()
    user.role = role
    return user


def employees(
    user_repo: AsyncMock, group: Any = None, qualification: Any = None
) -> EmployeeService:
    return EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=group or AsyncMock(),
        qualification_repo=qualification or AsyncMock(),
        client_repo=AsyncMock(),
        project_manager_repo=AsyncMock(),
    )


def clients(repo: AsyncMock) -> ClientService:
    return ClientService(
        client_repo=repo, user_repo=AsyncMock(), link_repo=AsyncMock(), token_repo=AsyncMock()
    )


def missing() -> AsyncMock:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=None)
    return repo


MANAGER_READS: dict[str, Callable[[AsyncMock], Read]] = {
    "list sites": lambda r: lambda a: SiteService(r).list_sites(a),
    "get site": lambda r: lambda a: SiteService(r).get_site(a, ID),
    "list clients": lambda r: lambda a: clients(r).list_clients(a),
    "get client": lambda r: lambda a: clients(r).get_client(a, ID),
    "list client users": lambda r: lambda a: clients(r).list_client_users(a, ID),
    "list groups": lambda r: lambda a: GroupService(r).list_groups(a),
    "list qualifications": lambda r: lambda a: QualificationService(r).list_qualifications(a),
}
SCOPED_READS = ["get site", "get client", "list client users"]


@pytest.mark.parametrize("name", MANAGER_READS)
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_a_read_requires_a_manager(name: str, role: UserRole) -> None:
    repo = AsyncMock()

    with pytest.raises(ForbiddenError) as refused:
        await MANAGER_READS[name](repo)(actor(role))

    assert refused.value.code == "users.manager_required"
    assert repo.mock_calls == []


@pytest.mark.parametrize("name", MANAGER_READS)
@pytest.mark.parametrize("role", [UserRole.manager, UserRole.admin])
async def test_staff_may_read(name: str, role: UserRole) -> None:
    await MANAGER_READS[name](AsyncMock())(actor(role))


@pytest.mark.parametrize(
    "read",
    [
        lambda r, a: SiteService(r).list_sites(a),
        lambda r, a: GroupService(r).list_groups(a),
        lambda r, a: QualificationService(r).list_qualifications(a),
    ],
    ids=["sites", "groups", "qualifications"],
)
async def test_a_list_is_scoped_to_the_organisation(read: Any) -> None:
    repo = AsyncMock()
    caller = actor()

    await read(repo, caller)

    repo.list_by_org.assert_awaited_once_with(caller.organisation_id)


@pytest.mark.parametrize("is_active", [True, False, None])
async def test_the_client_list_keeps_its_filter(is_active: bool | None) -> None:
    repo = AsyncMock()
    caller = actor()

    await clients(repo).list_clients(caller, is_active)

    repo.list_by_org.assert_awaited_once_with(caller.organisation_id, is_active)


async def test_a_clients_users_are_scoped_to_the_organisation() -> None:
    repo = AsyncMock()
    caller = actor()

    await clients(repo).list_client_users(caller, ID)

    repo.list_users.assert_awaited_once_with(ID, caller.organisation_id)


@pytest.mark.parametrize("name", SCOPED_READS)
async def test_an_id_of_another_organisation_answers_not_found(name: str) -> None:
    caller = actor()
    repo = missing()

    with pytest.raises(NotFoundError):
        await MANAGER_READS[name](repo)(caller)

    repo.get_by_id.assert_awaited_once_with(ID, caller.organisation_id)


async def test_a_clients_users_are_not_listed_when_it_is_missing() -> None:
    repo = missing()

    with pytest.raises(NotFoundError):
        await clients(repo).list_client_users(actor(), ID)

    repo.list_users.assert_not_awaited()


@pytest.mark.parametrize("read", ["get_employee", "list_groups_of", "list_qualifications_of"])
async def test_an_employee_may_read_their_own_record(read: str) -> None:
    caller = actor(UserRole.employee)

    await getattr(employees(AsyncMock()), read)(caller, caller.id)


@pytest.mark.parametrize("read", ["get_employee", "list_groups_of", "list_qualifications_of"])
async def test_an_employee_may_not_read_someone_elses_record(read: str) -> None:
    repo = AsyncMock()

    with pytest.raises(ForbiddenError) as refused:
        await getattr(employees(repo), read)(actor(UserRole.employee), ID)

    assert refused.value.code == "users.forbidden"
    assert repo.mock_calls == []


@pytest.mark.parametrize("read", ["get_employee", "list_groups_of", "list_qualifications_of"])
async def test_a_user_of_another_organisation_answers_not_found(read: str) -> None:
    group = AsyncMock()
    qualification = AsyncMock()

    with pytest.raises(NotFoundError):
        await getattr(employees(missing(), group, qualification), read)(actor(), ID)

    group.get_groups_for_user.assert_not_awaited()
    qualification.get_qualifications_for_user.assert_not_awaited()


async def test_a_change_needs_the_user_to_be_in_the_organisation() -> None:
    with pytest.raises(NotFoundError):
        await employees(missing()).ensure_manageable(actor(), ID)


async def test_a_change_checks_the_role_first_so_no_id_is_confirmed() -> None:
    repo = missing()

    with pytest.raises(ForbiddenError) as refused:
        await employees(repo).ensure_manageable(actor(UserRole.employee), ID)

    assert refused.value.code == "users.manager_required"
    repo.get_by_id.assert_not_awaited()
