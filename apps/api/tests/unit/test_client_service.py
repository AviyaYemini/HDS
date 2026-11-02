"""Unit tests for ClientService (T074).

Covers: FR-009a, FR-021, FR-022, FR-024.
"""

import uuid
from datetime import date
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ConflictError, DomainValidationError, ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.client import ClientService


def _actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.role = role
    return user


def _client(**kw: object) -> MagicMock:
    c = MagicMock()
    c.id = kw.get("id", uuid.uuid4())
    c.name = kw.get("name", "Acme Security")
    c.is_active = kw.get("is_active", True)
    c.is_deleted = kw.get("is_deleted", False)
    return c


def _svc() -> tuple[ClientService, AsyncMock, AsyncMock]:
    repo = AsyncMock()
    user_repo = AsyncMock()
    repo.list_users = AsyncMock(return_value=[])  # portal users of the client; none by default
    svc = ClientService(
        client_repo=repo, user_repo=user_repo, link_repo=AsyncMock(), token_repo=AsyncMock()
    )
    return svc, repo, user_repo


@pytest.mark.asyncio
async def test_create_client_success() -> None:
    svc, repo, _ = _svc()
    actor = _actor()
    repo.create = AsyncMock(return_value=_client())
    data = {"name": "Acme Security", "contract_start": date.today()}

    await svc.create_client(actor, data)

    repo.create.assert_awaited_once_with(actor.organization_id, data)


@pytest.mark.asyncio
async def test_edit_client_deactivate_calls_deactivate_users() -> None:
    svc, repo, _ = _svc()
    client_id = uuid.uuid4()
    repo.get_by_id = AsyncMock(return_value=_client(id=client_id, is_active=True))
    repo.update = AsyncMock(return_value=_client(is_active=False))
    repo.deactivate_client_users = AsyncMock()

    await svc.edit_client(_actor(), client_id, {"is_active": False})

    repo.deactivate_client_users.assert_awaited_once_with(client_id)


@pytest.mark.asyncio
async def test_edit_client_not_found() -> None:
    svc, repo, _ = _svc()
    repo.get_by_id = AsyncMock(return_value=None)

    with pytest.raises(NotFoundError):
        await svc.edit_client(_actor(), uuid.uuid4(), {"name": "X"})


@pytest.mark.asyncio
async def test_soft_delete_client_no_projects_succeeds() -> None:
    actor = _actor(UserRole.admin)
    svc, repo, _ = _svc()
    client_id = uuid.uuid4()
    repo.get_by_id = AsyncMock(return_value=_client())
    repo.count_non_deleted_projects = AsyncMock(return_value=0)

    await svc.soft_delete(actor, client_id)

    repo.deactivate_client_users.assert_awaited_once_with(client_id)
    repo.soft_delete.assert_awaited_once_with(client_id, actor.organization_id)


@pytest.mark.asyncio
async def test_soft_delete_client_with_projects_raises_conflict() -> None:
    svc, repo, _ = _svc()
    repo.get_by_id = AsyncMock(return_value=_client())
    repo.count_non_deleted_projects = AsyncMock(return_value=3)

    with pytest.raises(ConflictError) as exc_info:
        await svc.soft_delete(_actor(UserRole.admin), uuid.uuid4())

    assert exc_info.value.code == "clients.has_projects"
    repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_soft_delete_client_by_manager_is_forbidden() -> None:
    svc, repo, _ = _svc()

    with pytest.raises(ForbiddenError):
        await svc.soft_delete(_actor(UserRole.manager), uuid.uuid4())

    repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_client_user_duplicate_email_raises_conflict() -> None:
    svc, _, user_repo = _svc()
    user_repo.get_by_email = AsyncMock(return_value=MagicMock())

    with pytest.raises(ConflictError) as exc_info:
        await svc.create_client_user(
            _actor(),
            uuid.uuid4(),
            {"email": "dup@hds.test", "full_name": "Dup", "initial_password": "x" * 12},
        )
    assert exc_info.value.code == "users.duplicate_email"
    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_client_user_hashes_password_and_sets_client_role() -> None:
    svc, _, user_repo = _svc()
    actor = _actor()
    client_id = uuid.uuid4()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(MagicMock(), True))

    await svc.create_client_user(
        actor,
        client_id,
        {"email": "c@hds.test", "full_name": "C", "initial_password": "Correct-Horse-12"},
    )

    org_id, payload = user_repo.create_if_absent.await_args.args
    assert org_id == actor.organization_id
    assert payload["role"] == UserRole.client
    assert payload["client_id"] == client_id
    assert "initial_password" not in payload
    assert payload["password_hash"].startswith("$argon2id$")


@pytest.mark.asyncio
async def test_a_client_user_that_loses_the_address_during_the_hash_is_a_duplicate() -> None:
    svc, _, user_repo = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(MagicMock(), False))

    with pytest.raises(ConflictError) as exc_info:
        await svc.create_client_user(
            _actor(),
            uuid.uuid4(),
            {"email": "c@hds.test", "full_name": "C", "initial_password": "Correct-Horse-12"},
        )
    assert exc_info.value.code == "users.duplicate_email"


@pytest.mark.asyncio
async def test_create_client_user_short_password_rejected() -> None:
    svc, _, user_repo = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)

    with pytest.raises(DomainValidationError) as exc_info:
        await svc.create_client_user(
            _actor(),
            uuid.uuid4(),
            {"email": "c@hds.test", "full_name": "C", "initial_password": "short"},
        )
    assert exc_info.value.code == "users.password_too_short"
    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_remove_client_user_of_other_client_is_not_found() -> None:
    svc, _, user_repo = _svc()
    other = MagicMock()
    other.role = UserRole.client
    other.client_id = uuid.uuid4()
    user_repo.get_by_id = AsyncMock(return_value=other)

    with pytest.raises(NotFoundError):
        await svc.remove_client_user(_actor(), uuid.uuid4(), uuid.uuid4())

    user_repo.soft_delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_remove_client_user_success() -> None:
    actor = _actor()
    svc, _, user_repo = _svc()
    client_id = uuid.uuid4()
    user_id = uuid.uuid4()
    target = MagicMock()
    target.role = UserRole.client
    target.client_id = client_id
    user_repo.get_by_id = AsyncMock(return_value=target)

    await svc.remove_client_user(actor, client_id, user_id)

    user_repo.soft_delete.assert_awaited_once_with(user_id, actor.organization_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_non_manager_is_forbidden(role: UserRole) -> None:
    svc, repo, user_repo = _svc()
    actor = _actor(role)

    with pytest.raises(ForbiddenError):
        await svc.create_client(actor, {"name": "X"})
    with pytest.raises(ForbiddenError):
        await svc.edit_client(actor, uuid.uuid4(), {"name": "X"})
    with pytest.raises(ForbiddenError):
        await svc.create_client_user(actor, uuid.uuid4(), {"email": "a@b.c"})
    with pytest.raises(ForbiddenError):
        await svc.remove_client_user(actor, uuid.uuid4(), uuid.uuid4())

    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()
    user_repo.create.assert_not_awaited()


# ── Cancelling outstanding links of portal users (T234, spec FR-207) ───────────────────────────


def _portal_user() -> MagicMock:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    return user


@pytest.mark.asyncio
async def test_deactivating_a_client_cancels_the_links_of_its_portal_users() -> None:
    svc, repo, _ = _svc()
    actor = MagicMock(spec=User, role=UserRole.manager, organization_id=uuid.uuid4())
    client = MagicMock(is_active=True)
    portal = [_portal_user(), _portal_user()]
    repo.get_by_id = AsyncMock(return_value=client)
    repo.list_users = AsyncMock(return_value=portal)
    repo.update = AsyncMock(return_value=client)

    await svc.edit_client(actor, uuid.uuid4(), {"is_active": False})

    repo.deactivate_client_users.assert_awaited_once()
    cast(Any, svc._link_repo).cancel_for_users.assert_awaited_once_with([u.id for u in portal])


@pytest.mark.asyncio
async def test_editing_a_client_without_deactivating_it_cancels_nothing() -> None:
    svc, repo, _ = _svc()
    actor = MagicMock(spec=User, role=UserRole.manager, organization_id=uuid.uuid4())
    repo.get_by_id = AsyncMock(return_value=MagicMock(is_active=True))
    repo.update = AsyncMock()

    await svc.edit_client(actor, uuid.uuid4(), {"contact_name": "New Name"})

    cast(Any, svc._link_repo).cancel_for_users.assert_not_awaited()
    repo.deactivate_client_users.assert_not_awaited()


@pytest.mark.asyncio
async def test_deleting_a_client_cancels_the_links_of_its_portal_users() -> None:
    svc, repo, _ = _svc()
    actor = MagicMock(spec=User, role=UserRole.admin, organization_id=uuid.uuid4())
    portal = [_portal_user()]
    repo.get_by_id = AsyncMock(return_value=MagicMock())
    repo.count_non_deleted_projects = AsyncMock(return_value=0)
    repo.list_users = AsyncMock(return_value=portal)

    await svc.soft_delete(actor, uuid.uuid4())

    cast(Any, svc._link_repo).cancel_for_users.assert_awaited_once_with([portal[0].id])


@pytest.mark.asyncio
async def test_a_refused_client_deletion_cancels_nothing() -> None:
    svc, repo, _ = _svc()
    actor = MagicMock(spec=User, role=UserRole.admin, organization_id=uuid.uuid4())
    repo.get_by_id = AsyncMock(return_value=MagicMock())
    repo.count_non_deleted_projects = AsyncMock(return_value=2)

    with pytest.raises(ConflictError):
        await svc.soft_delete(actor, uuid.uuid4())

    cast(Any, svc._link_repo).cancel_for_users.assert_not_awaited()


def _portal(user_id: uuid.UUID) -> MagicMock:
    user = MagicMock()
    user.id = user_id
    return user


@pytest.mark.asyncio
async def test_deactivating_a_client_ends_its_portal_sessions_in_id_order() -> None:
    svc, repo, _ = _svc()
    actor = MagicMock(spec=User, role=UserRole.manager, organization_id=uuid.uuid4())
    first, second = sorted([uuid.uuid4(), uuid.uuid4()])
    repo.get_by_id = AsyncMock(return_value=_client(is_active=True))
    repo.update = AsyncMock(return_value=_client(is_active=False))
    repo.list_users = AsyncMock(return_value=[_portal(first), _portal(second)])

    await svc.edit_client(actor, uuid.uuid4(), {"is_active": False})

    calls = cast(Any, svc._token_repo).revoke_all_for_user.await_args_list
    assert [c.args for c in calls] == [(first, "account"), (second, "account")]


@pytest.mark.asyncio
async def test_removing_a_portal_user_ends_their_sessions_and_links() -> None:
    svc, _, user_repo = _svc()
    actor = MagicMock(spec=User, role=UserRole.manager, organization_id=uuid.uuid4())
    client_id = uuid.uuid4()
    user = MagicMock()
    user.id = uuid.uuid4()
    user.role = UserRole.client
    user.client_id = client_id
    user_repo.get_by_id = AsyncMock(return_value=user)

    await svc.remove_client_user(actor, client_id, user.id)

    cast(Any, svc._token_repo).revoke_all_for_user.assert_awaited_once_with(user.id, "account")
    cast(Any, svc._link_repo).cancel_for_user.assert_awaited_once_with(user.id)


@pytest.mark.asyncio
async def test_portal_sessions_end_before_their_links_are_touched() -> None:
    """The user's row lock comes before the links, or a reset can deadlock with this path."""
    svc, repo, _ = _svc()
    actor = MagicMock(spec=User, role=UserRole.manager, organization_id=uuid.uuid4())
    order: list[str] = []
    repo.get_by_id = AsyncMock(return_value=_client(is_active=True))
    repo.update = AsyncMock(return_value=_client(is_active=False))
    repo.list_users = AsyncMock(return_value=[_portal(uuid.uuid4())])
    cast(Any, svc._token_repo).revoke_all_for_user.side_effect = lambda *_: order.append("user")
    cast(Any, svc._link_repo).cancel_for_users.side_effect = lambda *_: order.append("links")

    await svc.edit_client(actor, uuid.uuid4(), {"is_active": False})

    assert order == ["user", "links"]
